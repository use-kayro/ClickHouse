#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobRefCount.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::Cas
{

/// Stable identity of a committed part or projection folder: its owning root namespace and
/// committed-ref name (for example, a part name or `detached/<part>`). The key is used for both
/// storage operations and retained-view indexing, so its equality and cache-key representations
/// must describe exactly the same ref.
struct PartRefKey
{
    Cas::RootNamespace ns{""};
    String ref;

    bool operator==(const PartRefKey & o) const { return ns.string() == o.ns.string() && ref == o.ref; }

    /// Canonical map key. '\0' cannot occur in namespace strings or ref names (both derive from
    /// disk paths), so the join is unambiguous even though refs may contain '/'.
    String cacheKey() const { return ns.string() + '\0' + ref; }
};


/// Immutable snapshot of one held part folder. Index-free: entries are in strictly ascending canonical
/// path order, so file lookup is a binary search and directory listing is a contiguous range scan. No I/O;
/// never mutated after construction.
class PartFolderView
{
public:
    /// The manifest must be non-null and its entries strictly ascending by canonical path.
    PartFolderView(PartRefKey key_, std::shared_ptr<const Cas::PartManifest> manifest_);

    /// Recognizes a projection directory by its last path component, `.proj` or `.tmp_proj`, and
    /// returns the corresponding in-tree prefix. The input is the routed file path; unrelated paths
    /// return nullopt.
    static std::optional<std::string> projectionDirPrefix(const std::string & file);

    const PartRefKey & refKey() const { return key; }
    const std::shared_ptr<const Cas::PartManifest> & manifest() const { return manifest_body; }

    /// Finds an entry by canonical path using the manifest's sorted-entry invariant.
    const Cas::ManifestEntry * findFile(const String & path) const;
    /// Returns whether the manifest contains an entry at `path`.
    bool hasFile(const String & path) const;
    /// Returns the logical size of an inline or blob entry, or nullopt when absent.
    std::optional<uint64_t> fileSize(const String & path) const;
    /// Returns inline bytes for an inline entry, or nullopt for absent and blob entries.
    std::optional<String> inlineBytes(const String & path) const;
    /// Lists immediate child names below `dir_prefix`; the result is not required to be sorted.
    std::vector<String> listChildren(const String & dir_prefix) const;
    /// Returns whether any manifest entry lies below `dir_prefix`.
    bool hasDirectory(const String & dir_prefix) const;

private:
    PartRefKey key;
    std::shared_ptr<const Cas::PartManifest> manifest_body;
};

}

namespace DB::Cas
{

/// The parts this content-addressed disk holds, in memory and nowhere else. A part enters it when its
/// write transaction commits (the in-progress `tmp_*` rename) or when ReplicatedMergeTree attaches it from
/// its Keeper record (boot, fetch); it leaves it when the part directory is removed. Nothing is read from or
/// written to the object store: the persistent copy of every part is its Keeper record
/// (`<zk>/cas_parts/<part>`, `CasKeeperPartRecord.h`), so after a restart the disk holds exactly what the
/// tables re-attach from their records.
///
/// Every held part holds one reference to each of its distinct blobs in the pool's `BlobRefCount`: a blob a
/// held part references is never deleted by GC and is adopted, not re-uploaded, by a build.
class HeldParts
{
public:
    explicit HeldParts(std::shared_ptr<BlobRefCount> refs_);

    /// The held part, or nullptr.
    std::shared_ptr<const PartFolderView> getView(const PartRefKey & key) const;
    bool existsRef(const PartRefKey & key) const;
    /// When the part entered this disk (its directory's modification time).
    std::optional<uint64_t> heldSinceMs(const PartRefKey & key) const;

    /// Holds `entries` (any order; duplicates are a logical error) as the part `key`, replacing whatever
    /// was held there.
    void install(const PartRefKey & key, std::vector<ManifestEntry> entries);
    /// Moves the held part `src` to `dst`; false when `src` is not held. A `dst` already held with other
    /// content is a conflict (`ABORTED`); with the same content the move completes by forgetting `src`.
    bool republishRef(const PartRefKey & src, const PartRefKey & dst);
    /// Forgets the part; a no-op when it is not held.
    void dropRefIfPresent(const PartRefKey & key);
    /// Forgets every part of `ns`.
    void dropNamespace(const RootNamespace & ns);

    /// The held ref names of `ns` that start with `prefix`.
    std::vector<String> refs(const RootNamespace & ns, std::string_view prefix) const;
    /// The namespaces that hold at least one part.
    std::vector<String> namespaces() const;

private:
    struct Held
    {
        std::shared_ptr<const PartFolderView> view;
        uint64_t held_since_ms = 0;
    };

    /// The distinct blobs of a part: what it holds one reference to each of.
    static std::vector<BlobRef> blobsOf(const PartManifest & manifest);

    std::shared_ptr<BlobRefCount> blob_refs;
    mutable std::mutex mutex;
    std::map<String, std::map<String, Held>> parts;   /// namespace -> ref -> part
};

}
