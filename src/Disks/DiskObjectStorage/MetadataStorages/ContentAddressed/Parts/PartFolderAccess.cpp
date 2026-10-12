#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Common/Exception.h>
#include <base/defines.h>
#include <base/scope_guard.h>
#include <algorithm>
#include <chrono>
#include <unordered_set>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
}
}

namespace DB::Cas
{

PartFolderView::PartFolderView(PartRefKey key_, std::shared_ptr<const Cas::PartManifest> manifest_)
    : key(std::move(key_))
    , manifest_body(std::move(manifest_))
{
    chassert(manifest_body);
    /// The binary-search contract: entries must be strictly ascending by `path` (sorted and unique) —
    /// `decodePartFiles` enforces exactly this for every decoded record, and `findEntry`'s binary
    /// search assumes uniqueness. `adjacent_find` with `!(a.path < b.path)` flags any adjacent pair
    /// that is out-of-order OR duplicate (stronger than `is_sorted`, which permits duplicates); a
    /// hand-constructed manifest (tests) must honor it too.
    chassert(std::adjacent_find(manifest_body->entries.begin(), manifest_body->entries.end(),
        [](const Cas::ManifestEntry & a, const Cas::ManifestEntry & b) { return !(a.path < b.path); })
        == manifest_body->entries.end());
}

std::optional<std::string> PartFolderView::projectionDirPrefix(const std::string & file)
{
    if (file.empty())
        return std::nullopt;
    const auto last_slash = file.find_last_of('/');
    const std::string_view last_component
        = last_slash == std::string::npos ? std::string_view(file) : std::string_view(file).substr(last_slash + 1);
    if (last_component.ends_with(".proj") || last_component.ends_with(".tmp_proj"))
        return file + "/";
    return std::nullopt;
}

const Cas::ManifestEntry * PartFolderView::findFile(const String & path) const
{
    return Cas::findEntry(manifest_body->entries, path);
}

bool PartFolderView::hasFile(const String & path) const
{
    return findFile(path) != nullptr;
}

std::optional<uint64_t> PartFolderView::fileSize(const String & path) const
{
    if (const auto * e = findFile(path))
        return e->size();
    return std::nullopt;
}

std::optional<String> PartFolderView::inlineBytes(const String & path) const
{
    const auto * e = findFile(path);
    if (e && e->placement == Cas::EntryPlacement::Inline)
        return e->inline_bytes;
    return std::nullopt;
}

std::vector<String> PartFolderView::listChildren(const String & dir_prefix) const
{
    /// Collapse each entry to its first child component. Projection folders are structurally flat in
    /// `MergeTree`, so this produces the same names as the old projection-specific path handling while
    /// keeping one directory-listing rule for all manifest folders.
    std::unordered_set<String> names;
    auto add = [&](const String & full)
    {
        if (!full.starts_with(dir_prefix) || full.size() <= dir_prefix.size())
            return;
        const std::string_view rest = std::string_view(full).substr(dir_prefix.size());
        const auto slash = rest.find('/');
        names.emplace(slash == std::string_view::npos ? rest : rest.substr(0, slash));
    };
    const auto [first, last] = Cas::entryRange(manifest_body->entries, dir_prefix);
    for (const auto * e = first; e != last; ++e)
        add(e->path);
    return {std::make_move_iterator(names.begin()), std::make_move_iterator(names.end())};
}

bool PartFolderView::hasDirectory(const String & dir_prefix) const
{
    const auto [first, last] = Cas::entryRange(manifest_body->entries, dir_prefix);
    return first != last;
}

HeldParts::HeldParts(std::shared_ptr<BlobRefCount> refs_)
    : blob_refs(std::move(refs_))
{
}

std::vector<BlobRef> HeldParts::blobsOf(const PartManifest & manifest)
{
    std::vector<BlobRef> out;
    for (const auto & e : manifest.entries)
        if (e.placement == EntryPlacement::Blob)
            out.push_back(e.ref);
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::shared_ptr<const PartFolderView> HeldParts::getView(const PartRefKey & key) const
{
    std::lock_guard lock(mutex);
    auto ns_it = parts.find(key.ns.string());
    if (ns_it == parts.end())
        return nullptr;
    auto it = ns_it->second.find(key.ref);
    return it == ns_it->second.end() ? nullptr : it->second.view;
}

bool HeldParts::existsRef(const PartRefKey & key) const
{
    return getView(key) != nullptr;
}

std::optional<uint64_t> HeldParts::heldSinceMs(const PartRefKey & key) const
{
    std::lock_guard lock(mutex);
    auto ns_it = parts.find(key.ns.string());
    if (ns_it == parts.end())
        return std::nullopt;
    auto it = ns_it->second.find(key.ref);
    if (it == ns_it->second.end())
        return std::nullopt;
    return it->second.held_since_ms;
}

void HeldParts::install(const PartRefKey & key, std::vector<ManifestEntry> entries)
{
    auto manifest = std::make_shared<PartManifest>();
    manifest->entries.reserve(entries.size());
    for (const auto * e : sortedUniqueEntries(entries, "content-addressed part"))
        manifest->entries.push_back(*e);
    const uint64_t now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());

    /// The part's references are taken BEFORE it becomes visible and before the caller's build releases its
    /// own, so the count never drops to zero in between.
    blob_refs->add(blobsOf(*manifest));
    std::shared_ptr<const PartFolderView> replaced;
    {
        std::lock_guard lock(mutex);
        auto & held = parts[key.ns.string()][key.ref];
        replaced = std::exchange(held.view, std::make_shared<const PartFolderView>(key, std::move(manifest)));
        held.held_since_ms = now_ms;
    }
    if (replaced)
        blob_refs->release(blobsOf(*replaced->manifest()));
}

bool HeldParts::republishRef(const PartRefKey & src, const PartRefKey & dst)
{
    std::shared_ptr<const PartFolderView> forgotten;
    {
        std::lock_guard lock(mutex);
        auto ns_it = parts.find(src.ns.string());
        if (ns_it == parts.end())
            return false;
        auto it = ns_it->second.find(src.ref);
        if (it == ns_it->second.end())
            return false;
        Held moved = it->second;
        auto & dst_refs = parts[dst.ns.string()];
        if (auto existing = dst_refs.find(dst.ref); existing != dst_refs.end())
        {
            if (existing->second.view->manifest()->entries != moved.view->manifest()->entries)
                throw Exception(ErrorCodes::ABORTED,
                    "republishRef: destination '{}' is already held with different content -- refusing (rename/attach conflict)",
                    dst.ns.string() + "/" + dst.ref);
            forgotten = moved.view;   /// the destination keeps its own references
        }
        else
        {
            moved.view = std::make_shared<const PartFolderView>(dst, moved.view->manifest());
            dst_refs.emplace(dst.ref, std::move(moved));
        }
        ns_it = parts.find(src.ns.string());
        ns_it->second.erase(src.ref);
        if (ns_it->second.empty())
            parts.erase(ns_it);
    }
    if (forgotten)
        blob_refs->release(blobsOf(*forgotten->manifest()));
    return true;
}

void HeldParts::dropRefIfPresent(const PartRefKey & key)
{
    std::shared_ptr<const PartFolderView> dropped;
    {
        std::lock_guard lock(mutex);
        auto ns_it = parts.find(key.ns.string());
        if (ns_it == parts.end())
            return;
        auto it = ns_it->second.find(key.ref);
        if (it == ns_it->second.end())
            return;
        dropped = std::move(it->second.view);
        ns_it->second.erase(it);
        if (ns_it->second.empty())
            parts.erase(ns_it);
    }
    blob_refs->release(blobsOf(*dropped->manifest()));
}

void HeldParts::dropNamespace(const RootNamespace & ns)
{
    std::map<String, Held> dropped;
    {
        std::lock_guard lock(mutex);
        auto ns_it = parts.find(ns.string());
        if (ns_it == parts.end())
            return;
        dropped = std::move(ns_it->second);
        parts.erase(ns_it);
    }
    for (const auto & [_, held] : dropped)
        blob_refs->release(blobsOf(*held.view->manifest()));
}

std::vector<String> HeldParts::refs(const RootNamespace & ns, std::string_view prefix) const
{
    std::vector<String> out;
    std::lock_guard lock(mutex);
    auto ns_it = parts.find(ns.string());
    if (ns_it == parts.end())
        return out;
    for (auto it = ns_it->second.lower_bound(String(prefix)); it != ns_it->second.end() && it->first.starts_with(prefix); ++it)
        out.push_back(it->first);
    return out;
}

std::vector<String> HeldParts::namespaces() const
{
    std::vector<String> out;
    std::lock_guard lock(mutex);
    for (const auto & [ns, _] : parts)
        out.push_back(ns);
    return out;
}

}
