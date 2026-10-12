#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <limits>

namespace ProfileEvents
{
    extern const Event CASGCEnumerationPages;
}

namespace DB::Cas
{

namespace
{

/// Hook used by this file's GC-owned enumeration calls to `forEachListedKey` and `recoverRefTable`.
/// It increments once per physical LIST page, never once per listed key.
void onGcEnumerationPage()
{
    ProfileEvents::increment(ProfileEvents::CASGCEnumerationPages);
}

/// The durable build floor is stored in the per-server mount lease together with the writer epoch. A
/// namespace is rooted by `server_root_id`, but that id is a clean relative path and can contain slashes.
/// Try namespace prefixes from longest to shortest and accept the first durable mount body. Without a
/// mount there is no deletion authority, so the caller must leave the prefix untouched. The mount's
/// `writer_epoch` and `min_active_build_sequence` are the single durable epoch/floor pair used for eligibility, including
/// across process replacement and the retired sentinel.
///
/// `listed_server_roots` is the caller's own LIST of `gc/server-roots/`: a prefix it does not name has no
/// mount body to read, so only the prefixes it names are tried. A mount younger than that LIST is then
/// not found, which retains -- the same answer an absent mount gives -- until the caller's next LIST.
std::optional<MountLease> floorForNamespace(CasOperation & op, const Layout & layout, const RootNamespace & ns,
                                            const std::set<String> & listed_server_roots)
{
    const String & value = ns.string();
    size_t pos = value.size();
    while (true)
    {
        pos = value.rfind('/', pos == 0 ? 0 : pos - 1);
        if (pos == String::npos)
            break;

        const String server_root_id = value.substr(0, pos);
        if (!server_root_id.empty() && listed_server_roots.contains(server_root_id))
            if (const auto got = op.read(layout.mountKey(server_root_id), Retry::standard()))
                return decodeMountLease(got->bytes);
        if (pos == 0)
            break;
    }
    return std::nullopt;
}

/// Return this operation's ONE catalog row for `ns`. Callers hold the enclosing `Snapshot` for their
/// entire decision; a later name resolution would splice its life into an earlier deletion decision.
const CatalogEntry * catalogEntryOf(const CasRefCatalog::Snapshot & catalog_cut, const RootNamespace & ns)
{
    const auto it = std::find_if(
        catalog_cut.catalog.entries.begin(), catalog_cut.catalog.entries.end(),
        [&](const CatalogEntry & entry) { return entry.ns == ns; });
    return it == catalog_cut.catalog.entries.end() ? nullptr : &*it;
}

/// The active manifest-object-key set for one namespace, built as the same complete view writer
/// recovery uses: owners in the newest snapshot + owner changes in every later log (== `recoverRefTable`'s
/// committed rows and live precommits). Keys (not ManifestIds) so a listed object key can be tested
/// directly. Throws on a corrupt snapshot / invalid transaction (via the authority-grounded recovery);
/// the caller SKIPS the namespace's deletions on such a throw rather than substituting an empty owner set.
std::set<String> activeManifestKeys(
    CasOperation & op, const Layout & layout, const CatalogEntry & catalog_entry, const RefCkpt & ckpt)
{
    /// The exact row and `_ckpt` come from the caller's frozen catalog cut. Do not resolve the name here:
    /// a later catalog cut can name a reborn life and turn this old life into an apparent orphan.
    const RefTableState state = recoverRefTableDetailedFromAuthority(op, layout, catalog_entry, ckpt).state;
    std::set<String> active;
    for (const auto [ref_name, row] : state.getCommitted())
        active.insert(layout.manifestKey(ManifestId{catalog_entry.ns, row.manifest_ref}));
    for (const auto & [ref_name, manifest_ref] : state.getPrecommits())
        active.insert(layout.manifestKey(ManifestId{catalog_entry.ns, manifest_ref}));
    return active;
}

/// Eligibility comes only from the durable mount-lease floor. A missing floor means NOT eligible;
/// do not replace that authority check with a frozen-sequence or judged-dead guess. Compare
/// `writer_epoch` first, then `build_sequence`, so old-epoch
/// debris drains after a process restart even when its build_sequence is above the current min_active_build_sequence.
bool prefixEligibleOn(CasOperation & op, const Layout & layout, const RootNamespace & ns, const BuildPrefix & prefix,
                      const std::set<String> & listed_server_roots)
{
    return prefixEligibleUnder(floorForNamespace(op, layout, ns, listed_server_roots), prefix);
}

}

bool prefixEligibleUnder(const std::optional<MountLease> & floor, const BuildPrefix & prefix)
{
    if (!floor)
        return false;

    const MountLease & w = *floor;
    if (prefix.writer_epoch < w.writer_epoch)
        return true;
    if (prefix.writer_epoch > w.writer_epoch)
        return false;
    if (w.min_active_build_sequence == std::numeric_limits<uint64_t>::max())
        return true;   /// farewell/retired sentinel: every seq is retired
    return w.min_active_build_sequence > prefix.build_sequence;
}

std::set<String> listServerRoots(Pool & store)
{
    CasOperation op = store.openRequests().admit();
    std::set<String> server_roots;
    for (const MountInfo & mount : listMounts(op, store.layout(), /*now_ms=*/0, /*skew_margin_ms=*/0))
        server_roots.insert(mount.srid);
    return server_roots;
}

bool prefixEligible(Pool & store, const RootNamespace & ns, const BuildPrefix & prefix,
                    const std::set<String> & listed_server_roots)
{
    CasOperation op = store.openRequests().admit();
    return prefixEligibleOn(op, store.layout(), ns, prefix, listed_server_roots);
}

uint64_t sweepNamespace(Pool & store, const RootNamespace & ns, const BuildPrefix & prefix,
                        const std::set<String> & listed_server_roots, std::vector<String> * warnings)
{
    const Layout & layout = store.layout();
    CasOperation op = store.openRequests().admit();
    if (!prefixEligibleOn(op, layout, ns, prefix, listed_server_roots))
        return 0;   /// not eligible by the durable watermark fact — delete nothing (controls #8/#9)

    /// Build the protection view. A missing snapshot body, an invalid transaction, or an incomplete
    /// ordered view throws, causing the sweep to skip deletion and surface the error; it never substitutes
    /// an empty owner set. Skip this namespace's deletions on such a throw rather than deleting against a
    /// wrong (empty) view. When a
    /// caller opted in (`warnings != nullptr`), this "cannot confirm emptiness" also lands in `*warnings`
    /// -- not just the log -- so a decommission run does not silently report a clean drain that never
    /// happened; a caller without `warnings` gets the log line only.
    const CasRefCatalog::Snapshot catalog_cut = CasRefCatalog::read(op, layout);
    catalog_cut.life_index.throwIfAmbiguous("CAS orphan manifest sweep");
    const CatalogEntry * catalog_entry = catalogEntryOf(catalog_cut, ns);
    if (!catalog_entry || catalog_entry->state == NsState::Creating)
        return 0;   /// absent/Creating names have no recovery authority and therefore no deletion authority

    std::set<String> active;
    try
    {
        const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(catalog_entry->ns, catalog_entry->incarnation);
        const std::optional<CkptSample> ckpt = readCkpt(op, layout, life);
        if (!ckpt)
        {
            const String warning = "CAS orphan sweep: namespace " + ns.string()
                + " is catalog-named but its required _ckpt is absent; skipped, emptiness not confirmed";
            LOG_WARNING(getLogger("CasOrphanManifestSweep"), "{}", warning);
            if (warnings)
                warnings->push_back(warning);
            return 0;
        }
        active = activeManifestKeys(op, layout, *catalog_entry, ckpt->ckpt);
    }
    catch (const Exception & e)
    {
        LOG_WARNING(getLogger("CasOrphanManifestSweep"),
                    "CAS orphan sweep: namespace {} protection view unavailable ({}); skipping its deletions",
                    ns.string(), e.message());
        if (warnings)
            warnings->push_back("CAS orphan sweep: namespace " + ns.string() + " protection view unavailable ("
                                 + e.message() + "); skipped, emptiness not confirmed");
        return 0;
    }

    /// Enumerate the ONE build prefix: cas/manifests/<ns>/<epoch-hex>-<seq-hex>/ in the canonical
    /// hexadecimal form -- the same rendering `Layout::manifestKey` uses.
    const String prefix_key = layout.manifestNamespacePrefix(ns)
        + renderRefTxnId(RefTxnId{prefix.writer_epoch, prefix.build_sequence}) + "/";

    uint64_t deleted = 0;
    op.forEachListedKey(prefix_key, [&](const ListedKey & listed)
    {
        if (active.contains(listed.key))
            return true;   /// owned by a committed or precommit owner — never sweep

        /// Exact-token delete: HEAD for the current incarnation, then remove exactly it -- never
        /// `removeCurrent`, whose re-head would delete whatever a fresh owner put there instead. A
        /// `Gone` (a 404 between the two) or a `Mismatch` (a fresh owner reclaimed the key) is
        /// tolerated, same as always, regardless of `warnings` -- that is the normal "someone else
        /// already reclaimed it" race, not a failure. A THROWN exception (a transient backend hiccup)
        /// is the one thing `warnings` changes: opted-in (non-null), it is recorded and the sweep moves
        /// to the next key; opted-out (nullptr, every pre-existing caller), it propagates exactly as
        /// before (fail-close).
        try
        {
            const std::optional<Meta> head = op.head(listed.key, Retry::standard());
            if (!head)
                return true;
            if (op.remove(listed.key, head->etag, Retry::standard()) == Removal::Removed)
                ++deleted;
        }
        catch (...)
        {
            if (!warnings)
                throw;
            warnings->push_back("CAS orphan sweep: " + listed.key + " delete failed: "
                                 + getCurrentExceptionMessage(/*with_stacktrace=*/false));
        }
        return true;
    }, Retry::standard(), 1000, onGcEnumerationPage);
    return deleted;
}

}
