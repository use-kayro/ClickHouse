#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <Disks/IO/WriteBufferWithFinalizeCallback.h>
#include <Disks/IDiskTransaction.h>
#include <Common/thread_local_rng.h>
#include <Common/config_version.h>
#include <algorithm>
#include <filesystem>
#include <unordered_set>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <base/hex.h>
#include <base/scope_guard.h>
#include <ctime>
#include <map>
#include <span>
#include <vector>

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

namespace
{

bool hasSuffix(std::string_view s, std::string_view suffix)
{
    return s.size() >= suffix.size() && s.substr(s.size() - suffix.size()) == suffix;
}

}

bool partFileMustStayBlob(std::string_view file_name)
{
    if (file_name == "primary.idx")
        return true;
    /// Column statistics (`statistics.packed`, and the older per-column `statistics_<col>.stats`) are the
    /// one small-file class that grows large: inline they would dominate the part record a replica reads
    /// from Keeper (`cas_part_files`), so they are content-addressed like column data.
    const size_t slash = file_name.rfind('/');
    if (file_name.substr(slash == std::string_view::npos ? 0 : slash + 1).starts_with("statistics"))
        return true;
    for (std::string_view suffix : {".bin", ".mrk", ".mrk2", ".mrk3", ".cmrk", ".cmrk2", ".cmrk3"})
        if (hasSuffix(file_name, suffix))
            return true;
    return false;
}

}

namespace DB
{

namespace
{

[[noreturn]] void notYet(const char * op)
{
    /// These operations are part of the generic disk-transaction interface but have no
    /// content-addressed equivalent or are not wired for this storage yet. Keep the message
    /// self-explanatory because it is visible to operators.
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "The operation '{}' is not implemented for a content-addressed disk: it belongs to the "
        "generic disk-transaction surface that the content-addressed write path does not use. "
        "Hitting it usually means the disk is wrapped by a layer that bypasses the "
        "content-addressed write path.", op);
}

/// Every write target that is not a top-level part directory of a table: a table-level file, a loose disk
/// file, `detached/`, `moving/`, a FREEZE `shadow/` path.
[[noreturn]] void throwNotAPartTarget(const std::string & path)
{
    throwNotPersistedOnContentAddressedDisk(fmt::format("Writing '{}' (not a part of a table)", path));
}

/// Inline candidates above this size spill to a blob instead of riding the tree object — a tuning
/// knob (could become a disk setting later). Keeps the tree object bounded against an unexpectedly
/// large eager file.
constexpr size_t INLINE_CAP = 1024 * 1024;   /// 1 MiB

}

ContentAddressedTransaction::ContentAddressedTransaction(ContentAddressedMetadataStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
{
}

ContentAddressedTransaction::~ContentAddressedTransaction()
{
    /// Always clean up pending staging (whether committed or not). On the success path
    /// cleanupPendingTempFiles was already called at the end of commit(); this call is the defensive
    /// backstop for aborted/exception-unwound transactions whose publishStaging never ran.
    cleanupPendingTempFiles();

    /// An uncommitted transaction published nothing; destroying its builds releases their blob claims.
}

ContentAddressedTransaction::PartStaging &
ContentAddressedTransaction::stagingFor(const ContentAddressedMetadataStorage::Route & r)
{
    return parts[{r.ns.string(), r.ref}];
}

Cas::PartBlobs & ContentAddressedTransaction::buildFor(
    const ContentAddressedMetadataStorage::Route & r, PartStaging & st)
{
    if (!st.build)
    {
        /// Only the pool's writer builds a part: its GC sees its own builds and the records of its own tables,
        /// nothing a reader holds. Refused before any blob is consulted, so a reader holding every blob of the
        /// new part cannot commit it without an upload either (INSERT, a local clone, a local merge or
        /// mutation). A reader commits only a fetched part whose record exists (`holdKeeperPart`).
        if (!metadata_storage.writesBlobs())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot build part '{}' on a content-addressed disk: only the pool's writer builds parts, and this "
                "server's replicas cannot become merge leader (replicated_can_become_leader = 0). Run the write on "
                "the writer; this replica fetches the result from its Keeper record",
                r.ns.string() + "/" + r.ref);
        st.build = std::make_unique<Cas::PartBlobs>(metadata_storage.store(), r.ns.string() + "/" + r.ref);
    }
    return *st.build;
}

ContentAddressedTransaction::PartStaging * ContentAddressedTransaction::findStaging(
    const ContentAddressedMetadataStorage::Route & r)
{
    auto it = parts.find({r.ns.string(), r.ref});
    return it == parts.end() ? nullptr : &it->second;
}

void ContentAddressedTransaction::cleanupPendingTempFiles() noexcept
{
    /// The staging files are private scratch files, removed on both commit and abort.
    for (auto & [key, st] : parts)
    {
        for (const auto & pb : st.pending_blobs)
        {
            std::error_code ec;
            std::filesystem::remove(pb.staging_key, ec);
        }
        st.pending_blobs.clear();
    }
}

const ContentAddressedTransaction::PartStaging::PendingBlob *
ContentAddressedTransaction::findPendingBlob(const PartStaging & st, const Cas::BlobRef & ref) const
{
    /// Locate a pending blob by ref. Returns nullptr when the blob has already been uploaded (pending_blobs
    /// is cleared at publish) or was never staged as pending.
    for (const auto & pb : st.pending_blobs)
        if (pb.ref == ref)
            return &pb;
    return nullptr;
}

void ContentAddressedTransaction::adoptStagedBlob(
    const PartStaging::PendingBlob * pb, const Cas::ManifestEntry & entry,
    PartStaging & dst_st, Cas::PartBlobs & dst_build, bool copy_pending)
{
    if (pb)
    {
        /// Pending blob (not yet uploaded): if copy_pending, push a copy of the record into dst_st so
        /// publishStaging uploads it for the dst part too (hardlink = copy semantics); otherwise the
        /// caller already moved it there.
        if (copy_pending)
            dst_st.pending_blobs.push_back(*pb);
    }
    else
    {
        /// Uploaded, or held by a part: the destination build claims it (no request; the holder keeps it alive).
        if (entry.placement == Cas::EntryPlacement::Blob)
            dst_build.adopt(entry.ref);
    }
}

std::optional<ContentAddressedMetadataStorage::Route>
ContentAddressedTransaction::routeOf(const std::string & path) const
{
    auto p = Cas::parsePartFilePath(path);
    if (!p)
        return std::nullopt;
    return metadata_storage.route(*p);
}

void ContentAddressedTransaction::uploadPendingBlobs(PartStaging & st)
{
    /// Build the set of blob hashes actually referenced by the staged manifest. Only Blob
    /// entries represent pending content uploads — Inline are not pending blobs. A pending_blob whose
    /// hash is NOT in this set had its entry removed by unlinkFile/replaceFile and must not be uploaded
    /// (it is an orphan). Its temp file is still cleaned by cleanupPendingTempFiles at commit end.
    std::unordered_set<Cas::BlobRef, Cas::BlobRefHash> referenced_hashes;
    for (const auto & entry : st.entries)
        if (entry.placement == Cas::EntryPlacement::Blob)
            referenced_hashes.insert(entry.ref);

    /// One upload request per referenced pending blob; duplicate refs (staged-hardlink copies push a copy of
    /// the record) are collapsed by `PartBlobs::upload`, one task per distinct blob. Each publication re-opens
    /// the local staging file and streams it under a fresh envelope.
    std::vector<Cas::BlobUploadRequest> requests;
    requests.reserve(st.pending_blobs.size());
    for (const auto & pb : st.pending_blobs)
    {
        if (!referenced_hashes.contains(pb.ref))
            continue;   /// The entry was removed by unlinkFile/replaceFile; skip this orphan.
        Cas::BlobSource source;
        source.size = pb.size;
        const std::string staging_key = pb.staging_key;
        source.open = [staging_key]() -> std::unique_ptr<ReadBuffer>
        {
            return std::make_unique<ReadBufferFromFile>(staging_key);
        };
        /// `declared_size` mirrors `source.size` (both are `pb.size`); the fan-out fail-closes if they
        /// ever diverge, so build them together from the one authority.
        requests.push_back(Cas::BlobUploadRequest{pb.ref, std::move(source), pb.size});
    }

    if (requests.empty())
        return;

    /// Fan out the uploads on the server-wide pool (spec §1). The fan-out enforces
    /// one-task-per-unique-ref grouping, the merge-nothing failure contract, and merges every result
    /// into `st.build`'s dep set on THIS (the owning writer) thread after the join.
    st.build->upload(requests);
}

void ContentAddressedTransaction::publishStaging(const Cas::RootNamespace & ns, const std::string & ref, PartStaging & st,
                                                 std::optional<Cas::PartRefKey> & out_slot)
{
    if (st.published)
        return;

    if (!st.build && st.entries.empty() && st.content_removed.empty())
    {
        /// Nothing staged for this ref (the residue of a removeDirectory that superseded its marks).
        st.published = true;
        return;
    }

    /// A change to a part already held (a clone gets its `metadata_version.txt` after the copy, the quick removal
    /// unlinks files before the directory goes) applies to the held part; ReplicatedMergeTree makes the record
    /// from what is held when it commits the part.
    if (auto view = metadata_storage.partAccess()->getView({ns, ref}))
    {
        uploadPendingBlobs(st);
        std::vector<Cas::ManifestEntry> merged;
        for (const auto & e : view->manifest()->entries)
            if (!st.content_removed.contains(e.path)
                && std::none_of(st.entries.begin(), st.entries.end(), [&](const Cas::ManifestEntry & s_e) { return s_e.path == e.path; }))
                merged.push_back(e);
        for (auto & e : st.entries)
            merged.push_back(std::move(e));
        metadata_storage.partAccess()->install({ns, ref}, std::move(merged));
        st.build.reset();
        st.published = true;
        return;
    }

    uploadPendingBlobs(st);

    /// The part is held from here on (its blobs owned by it before this build's claims go).
    metadata_storage.partAccess()->install({ns, ref}, std::move(st.entries));
    out_slot = Cas::PartRefKey{ns, ref};
    st.build.reset();
    st.published = true;
}

void ContentAddressedTransaction::commit(const TransactionCommitOptionsVariant &)
{
    if (failed)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "retrying a failed content-addressed transaction is not supported");

    /// Publish each staged part: upload its pending blobs and hold it in memory (`HeldParts`). There is no
    /// multi-part atomic publish, so a publish that throws after earlier parts were held would leave a partial
    /// commit; the parts THIS commit created (`part_outcomes`, preallocated so `publishStaging` writes its slot
    /// without throwing) are forgotten again before the exception propagates. A part that was already held is
    /// never forgotten here. Parts are published serially; only the blob uploads within a part fan out.
    struct IndexedPart { Cas::RootNamespace ns; std::string ref; PartStaging * st; };
    std::vector<IndexedPart> ordered;
    ordered.reserve(parts.size());
    for (auto & [key, st] : parts)
        ordered.push_back({Cas::RootNamespace{key.first}, key.second, &st});

    std::vector<std::optional<Cas::PartRefKey>> part_outcomes;
    part_outcomes.assign(ordered.size(), std::nullopt);

    try
    {
        for (size_t i = 0; i < ordered.size(); ++i)
            publishStaging(ordered[i].ns, ordered[i].ref, *ordered[i].st, part_outcomes[i]);
    }
    catch (...)
    {
        failed = true;
        /// Forget the parts this commit created; their blobs go through the record GC.
        for (const auto & key : part_outcomes)
            if (key)
                metadata_storage.partAccess()->dropRefIfPresent(*key);
        throw;
    }
    /// All pending blobs have been uploaded in publishStaging; remove their staging resources now.
    cleanupPendingTempFiles();
}

TransactionCommitOutcomeVariant ContentAddressedTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ContentAddressed transaction supports only tryCommit without options");
    commit(options);
    return true;
}

ObjectStorageKey ContentAddressedTransaction::generateObjectKeyForPath(const std::string &)
{
    notYet("generateObjectKeyForPath");
}

StoredObjects ContentAddressedTransaction::getSubmittedForRemovalBlobs()
{
    /// CA never hands shared backing objects to the disk layer for removal — reclamation is the
    /// GC's. Empty unconditionally because shared content-addressed objects are never owned by the
    /// disk-layer removal list.
    return {};
}

const Cas::ManifestEntry * ContentAddressedTransaction::findStagedEntry(
    const ContentAddressedMetadataStorage::Route & r) const
{
    auto it = parts.find({r.ns.string(), r.ref});
    if (it == parts.end())
        return nullptr;
    auto eit = std::find_if(it->second.entries.begin(), it->second.entries.end(),
        [&](const Cas::ManifestEntry & e) { return e.path == r.file; });
    return eit == it->second.entries.end() ? nullptr : &*eit;
}

std::optional<StoredObjects> ContentAddressedTransaction::tryGetInFlightStorageObjects(const std::string & path) const
{
    /// Read-your-writes: a projection spill-and-merge reads back its own staged blocks before
    /// the parent part's single commit. Staged content blobs may still be pending (not yet uploaded).
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    if (!r || r->file.empty())
        return {};
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return {};
    if (const auto * entry = findStagedEntry(*r))
    {
        if (entry->placement == Cas::EntryPlacement::Blob)
        {
            /// A pending blob has not been uploaded yet — its storage object does not exist in
            /// the pool. Return empty so the caller falls back to tryReadFileInFlight (local temp read).
            if (findPendingBlob(it->second, entry->ref))
                return {};
            const auto location = metadata_storage.store()->locate(*entry);
            return StoredObjects{StoredObject(location.key, path, location.length)};
        }
        /// An Inline entry carries its bytes in `inline_bytes`; `size()` (not `blob_size`, which is 0
        /// for an inline entry carried forward from a decoded source manifest — createHardLink) reports
        /// the real inline byte count, so an in-flight read of a carried-forward inline sidecar (e.g. a
        /// MATERIALIZE-PROJECTION projection marks file) resolves to its real size, matching the
        /// committed getStorageObjects path.
        return StoredObjects{StoredObject("", path, entry->size())};
    }
    return {};
}

std::unique_ptr<ReadBufferFromFileBase> ContentAddressedTransaction::tryReadFileInFlight(
    const std::string & path, const ReadSettings & settings, std::optional<size_t> /*read_hint*/) const
{
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    if (!r || r->file.empty())
        return nullptr;
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return nullptr;
    if (const auto * entry = findStagedEntry(*r))
    {
        if (entry->placement == Cas::EntryPlacement::Inline)
            return std::make_unique<ReadBufferFromOwnMemoryFile>(path, entry->inline_bytes);
        if (entry->placement == Cas::EntryPlacement::Blob)
        {
            /// A pending blob has not been uploaded yet -- serve reads from its local staging file (the same
            /// bytes `publishStaging` will upload).
            if (const auto * pb = findPendingBlob(it->second, entry->ref))
                return std::make_unique<ReadBufferFromFile>(pb->staging_key);
            return metadata_storage.readBlobPayload(metadata_storage.store()->locate(*entry), path, settings);
        }
    }
    return nullptr;
}

std::optional<uint64_t> ContentAddressedTransaction::tryGetInFlightFileSize(const std::string & path) const
{
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    if (!r || r->file.empty())
        return {};
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return {};
    if (const auto * entry = findStagedEntry(*r))
        /// `size()` (not `blob_size` directly, which is 0 for an inline entry carried forward via
        /// createHardLink from a decoded source manifest). Without this, an in-flight size query for a
        /// carried-forward inline sidecar returns 0 — the 02941 MATERIALIZE-PROJECTION "Empty marks
        /// file: 0, must be: 144" corruption on a same-session read.
        return entry->size();
    return {};
}

bool ContentAddressedTransaction::hasInFlightDirectory(const std::string & path) const
{
    /// The directory overlay is true iff at least one staged file lives under `path` for `path`'s
    /// part - what makes a carried-forward projection dir visible to loadProjections.
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    /// INNER directories only: the overlay exists for staged projection dirs
    /// The overlay is used by `loadProjections` during finalize. The PART DIR ITSELF answers FALSE - a
    /// dedup-rejected temporary part still holds its uncommitted transaction at destruction, and
    /// an overlay "exists" for the bare part dir sends removeIfNeeded into remove(), whose
    /// bare-disk check then logs the "part to remove doesn't exist" warning.
    if (!r || r->ref.empty() || r->file.empty())
        return false;
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return false;
    const std::string prefix = r->file + "/";
    for (const auto & entry : it->second.entries)
        if (entry.path.starts_with(prefix))
            return true;
    return false;
}

std::vector<std::string> ContentAddressedTransaction::listInFlightDirectory(const std::string & path) const
{
    /// Immediate-child names staged directly under `path` (one level) - loadProjections'
    /// withPartFormatFromDisk iterates a staged projection dir to find its mark file.
    auto r = const_cast<ContentAddressedTransaction *>(this)->routeOf(path);
    std::vector<std::string> result;
    if (!r || r->ref.empty())
        return result;
    auto it = parts.find({r->ns.string(), r->ref});
    if (it == parts.end())
        return result;
    const std::string prefix = r->file.empty() ? "" : r->file + "/";
    std::set<std::string> names;
    auto add = [&](const std::string & name)
    {
        if (!name.starts_with(prefix) || name.size() <= prefix.size())
            return;
        const auto rest = name.substr(prefix.size());
        const auto slash = rest.find('/');
        names.insert(slash == std::string::npos ? rest : rest.substr(0, slash));
    };
    for (const auto & entry : it->second.entries)
        add(entry.path);
    return {names.begin(), names.end()};
}

void ContentAddressedTransaction::createMetadataFile(const std::string &, const StoredObjects &)
{
    notYet("createMetadataFile");
}

void ContentAddressedTransaction::stageBlobPartFile(
    const ContentAddressedMetadataStorage::Route & route,
    const Cas::BlobRef & ref, size_t size, const std::string & staging_key)
{
    /// Do not upload here: record the pending blob; `publishStaging` uploads it. The transaction owns the
    /// local staging file until then.
    auto & st = stagingFor(route);
    st.pending_blobs.push_back({ref, staging_key, size});
    (void)buildFor(route, st);

    Cas::ManifestEntry entry;
    entry.path = route.file;
    entry.placement = Cas::EntryPlacement::Blob;
    entry.ref = ref;
    entry.blob_size = size;
    std::erase_if(st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
    st.entries.push_back(std::move(entry));
}

std::unique_ptr<WriteBufferFromFileBase> ContentAddressedTransaction::tryCreateWriteBuffer(
    const std::shared_ptr<IDiskTransaction> & owner,
    const std::string & path, size_t buf_size, WriteMode mode,
    const WriteSettings & settings, bool autocommit)
{
    /// This transaction owns the write because the blob key is known only after hashing the payload.
    /// Append is serviceable (read-modify-rewrite) only for a non-part / table-level verbatim file
    /// (handled inside writeFile). A part file is a content blob or a whole-rewritten inline entry, so
    /// append on a part-file path is unsupported.
    if (mode == WriteMode::Append && Cas::isPartFilePath(path))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support WriteMode::Append for content part files");

    /// Autocommit cannot work for a CONTENT BLOB part file (column data/marks, primary.idx): a part's blobs
    /// are always written together as one build, held only when commit() runs. A small INLINE-eligible part
    /// file IS autocommittable (a standalone one-shot write): it applies to the held part in memory
    /// (`publishStaging`), the transactional-INSERT creation-CSN fill-in / removal-TID rewrite path.
    if (autocommit && Cas::isPartFilePath(path))
    {
        auto p = Cas::parsePartFilePath(path);
        if (!p || p->file.empty() || Cas::partFileMustStayBlob(p->file))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Autocommit writes are not supported for content part files on a content-addressed disk");

        auto inner = writeFile(path, buf_size, mode, settings);
        auto commit_callback = [owner](size_t) mutable { owner->commit(); };
        return std::make_unique<WriteBufferWithFinalizeCallback>(
            std::move(inner), std::move(commit_callback), path, /*create_blob_if_empty=*/true);
    }

    /// Non-autocommit: pin the owning disk transaction for the returned buffer's
    /// lifetime. The CA write buffers capture a bare `this` in their deferred finalize / pin-blob callbacks;
    /// MergedBlockOutputStream may finalize them LATER (another thread, or after the part storage /
    /// transaction would otherwise be torn down on async-insert / cancel / exception-unwind). Holding
    /// `owner` (which owns this ContentAddressedTransaction by shared_ptr) keeps that `this` valid until the
    /// buffer — and so this callback — is destroyed after finalize (the lifetime guarantee now
    /// expressed generically via `owner`). No cycle: the transaction does not hold the buffer.
    auto inner = writeFile(path, buf_size, mode, settings);
    auto keep_alive_callback = [owner](size_t) mutable {};
    return std::make_unique<WriteBufferWithFinalizeCallback>(
        std::move(inner), std::move(keep_alive_callback), path, /*create_blob_if_empty=*/true);
}

std::unique_ptr<WriteBufferFromFileBase> ContentAddressedTransaction::writeFile(
    const std::string & path, size_t buf_size, WriteMode /*mode*/, const WriteSettings & settings)
{
    auto r = routeOf(path);
    if (!r || r->file.empty())
        throwNotAPartTarget(path);

    /// A CONTENT part file that must stay a blob (per-column data/marks, primary.idx): spill + hash, then
    /// stage the blob as PENDING. `publishStaging` uploads it; staging performs no pool operation.
    if (Cas::partFileMustStayBlob(r->file))
    {
        /// Spill to a local staging file while hashing. Hash with this pool's node-local write algorithm rather than a hardcoded city hash;
        /// `PoolMeta` no longer records a single pool-wide algorithm --
        /// mixed-algo pools track `algos_used`; `writeAlgo()` is the write-mint accessor now).
        const auto hash_algo = metadata_storage.store()->writeAlgo();
        /// `hash_hex` is rendered by the streaming write buffer at `hash_algo`'s own width —
        /// parse it back at that SAME width via `Cas::codecFor(hash_algo)` (never a pool-wide
        /// `DigestCodec`, which no longer exists) into a full `BlobRef` pair.

        return std::make_unique<Cas::CaContentWriteBuffer>(
            metadata_storage.scratchPath(),
            hash_algo,
            buf_size,
            settings.use_adaptive_write_buffer,
            settings.adaptive_write_buffer_initial_size,
            [this, route = *r, hash_algo](const std::string & hash_hex, size_t size, const std::string & temp_path)
            {
                const Cas::BlobRef ref{hash_algo, Cas::codecFor(hash_algo).fromHex(hash_hex)};
                stageBlobPartFile(route, ref, size, temp_path);
            });
    }

    /// Inline candidate (small eager metadata): buffer in memory, decide at finalize. <= INLINE_CAP
    /// rides the single tree object as an Inline entry (one-GET part open); an oversized
    /// candidate spills to a blob (the safety net).
    return std::make_unique<Cas::CaInlineWriteBuffer>(
        [this, route = *r](std::string bytes)
        {
            /// Mint via the one write hash function, `Cas::poolContentHash` (algorithm, payload) -> BlobRef
            /// (`CasPartBlobs.h`) -- the SAME mint the streaming blob path's callers use, so an inline file
            /// and a standalone blob of identical content get the same ref (same content hash identity)
            /// under EVERY algo, including sha256.
            const auto hash_algo = metadata_storage.store()->writeAlgo();
            const Cas::BlobRef ref = Cas::poolContentHash(hash_algo, bytes);
            if (bytes.size() <= INLINE_CAP)
            {
                auto & st = stagingFor(route);
                /// A part whose files are ALL inline (a tiny/empty merge output) still has a build, so
                /// `publishStaging` publishes it like any other.
                buildFor(route, st);
                Cas::ManifestEntry entry;
                entry.path = route.file;
                entry.placement = Cas::EntryPlacement::Inline;
                entry.ref = ref;   /// content hash identity (same for inline and blob of same content)
                /// `blob_size` stays 0 (its default) for an Inline entry — matching decode, which never
                /// fills it for Inline. `entry.size()` is the logical size, derived from `inline_bytes`.
                entry.inline_bytes = std::move(bytes);
                std::erase_if(st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
                st.entries.push_back(std::move(entry));
            }
            else
            {
                /// Safety fallback: an unexpectedly large candidate spills to a blob (preserves the
                /// invariant that big files are not held inline). Write the buffered bytes to a unique
                /// local temp file (same scratchPath + random-name scheme as CaContentWriteBuffer), then
                /// stage exactly like a streaming blob.
                std::filesystem::create_directories(metadata_storage.scratchPath());
                const std::string temp_path =
                    metadata_storage.scratchPath() + "/inline_overflow_" + getRandomASCIIString(32) + ".tmp";
                {
                    WriteBufferFromFile tmp(temp_path);
                    tmp.write(bytes.data(), bytes.size());
                    tmp.finalize();
                }
                /// Until stageBlobPartFile takes ownership, WE own the temp file — mirror the blob path,
                /// where CaContentWriteBuffer's dtor removes it unless the callback succeeded. If
                /// stageBlobPartFile throws, drop the orphan instead of leaking it into scratch.
                bool staged = false;
                SCOPE_EXIT({ if (!staged) { std::error_code ec; std::filesystem::remove(temp_path, ec); } });
                stageBlobPartFile(route, ref, bytes.size(), temp_path);
                staged = true;
            }
        });
}

void ContentAddressedTransaction::createDirectory(const std::string &)
{
    /// Directories are virtual (a part exists while it is held).
}

void ContentAddressedTransaction::createDirectoryRecursive(const std::string &)
{
}

void ContentAddressedTransaction::removeDirectory(const std::string & path)
{
    /// CONTRACT: `removeDirectory`/`moveDirectory` mutate durable refs at CALL TIME, not at commit —
    /// this is the everything-immediate model, not a missed "defer to commit" opportunity. `renameParts`
    /// is the actual commit point; anything that goes wrong after one of these calls is undone by a
    /// COMPENSATING operation over already-committed state, the same way upstream MergeTree's own
    /// `rollbackPartsToTemporaryState` and outdated-part cleanup run over committed disk state rather
    /// than an in-memory intent log. Recording these as staged intents and applying them at commit would
    /// duplicate that compensation machinery for no correctness gain.
    ///

    /// The MergeTree fast-removal path unlinks a part's files one by one and then calls
    /// removeDirectory(<part>): the point at which the held part is forgotten. Anything else is a no-op
    /// (directories are virtual; a table is removed via removeRecursive).
    if (auto r = routeOf(path); r && !r->ref.empty() && r->file.empty())
    {
        metadata_storage.partAccess()->dropRefIfPresent(r->refKey());
        /// This transaction's staged removal marks for the same ref (content_removed, populated by
        /// unlinkFile's per-file unlinks that the MergeTree fast-removal path issues right before this
        /// call) are superseded by forgetting the whole part above -- discard them so `publishStaging`
        /// never edits a part that is gone.
        if (auto * st = findStaging(*r))
        {
            st->content_removed.clear();
            st->entries.clear();
            st->build.reset();
        }
        return;
    }
}

void ContentAddressedTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    /// Removal forgets held parts; their blobs go through the GC once no record references them. The predicate
    /// gates backing-object deletion, which CA always defers, so it is intentionally ignored here.

    /// A shadow table dir also satisfies `parseTableUuid`: FREEZE copies are never held, nothing to remove.
    if (Cas::isShadowPath(path))
        return;

    /// Table dir: forget every part of the table.
    if (auto uuid = Cas::parseTableUuid(path))
    {
        metadata_storage.partAccess()->dropNamespace(metadata_storage.liveNamespace(*uuid));
        return;
    }

    /// A part dir: forget it. A path inside a part (a projection subdir) is virtual: its blobs go with the
    /// part. Anything else (`detached/`, `moving/`, a table-level subdirectory) holds nothing.
    if (auto r = routeOf(path); r && r->file.empty())
        metadata_storage.partAccess()->dropRefIfPresent(r->refKey());
}

void ContentAddressedTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    auto src = routeOf(path_from);
    auto dst = routeOf(path_to);
    if (!dst || dst->file.empty())
        throwNotAPartTarget(path_to);
    if (!src || src->file.empty())
        throwNotAPartTarget(path_from);

    auto & dst_st = stagingFor(*dst);

    /// Content file. Prefer an entry staged earlier in THIS transaction; else carry it forward from the held
    /// source part (the destination build claims its blob; the source part keeps it alive meanwhile).
    Cas::ManifestEntry entry;
    if (auto * src_st = findStaging(*src))
    {
        auto it = std::find_if(src_st->entries.begin(), src_st->entries.end(),
            [&](const Cas::ManifestEntry & e) { return e.path == src->file; });
        if (it != src_st->entries.end())
        {
            entry = *it;
            if (entry.placement == Cas::EntryPlacement::Blob)
            {
                /// Unified adopt dispatch. copy_pending=(&dst_st != src_st) so the pending
                /// blob record is copied into dst_st only when the destination is a different part
                /// (hardlink = copy semantics; same-part is a self-ref that shouldn't duplicate the record).
                const auto * pb = findPendingBlob(*src_st, entry.ref);
                adoptStagedBlob(pb, entry, dst_st, buildFor(*dst, dst_st), /*copy_pending=*/(&dst_st != src_st));
            }
            else if (entry.placement != Cas::EntryPlacement::Inline)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "ContentAddressed: staged hardlink of unsupported placement for {}", path_from);
            entry.path = dst->file;
            std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
            dst_st.entries.push_back(std::move(entry));
            return;
        }
    }

    /// Carry forward from the held source part: find the named entry and claim its blob (no request).
    auto view = metadata_storage.partAccess()->getView(src->refKey());
    if (!view)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "ContentAddressed: createHardLink source part missing: {}", path_from);
    const auto * src_entry = view->findFile(src->file);
    if (!src_entry)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST,
            "ContentAddressed: createHardLink source file missing in manifest: {}", path_from);
    if (src_entry->placement == Cas::EntryPlacement::Blob)
        buildFor(*dst, dst_st).adopt(src_entry->ref);
    else
        buildFor(*dst, dst_st);
    entry = *src_entry;
    entry.path = dst->file;
    std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
    dst_st.entries.push_back(std::move(entry));
}

void ContentAddressedTransaction::setLastModified(const std::string &, const Poco::Timestamp &)
{
    /// Timestamps are derived (when the part entered this disk): accept and ignore them.
}

void ContentAddressedTransaction::chmod(const String &, mode_t)
{
    notYet("chmod");
}

void ContentAddressedTransaction::setReadOnly(const std::string &)
{
    /// Read-only flags have no content-addressed representation: accept and ignore them.
}

void ContentAddressedTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    /// Same call-time-durability-plus-compensation contract as `removeDirectory` above: this mutates
    /// durable refs immediately rather than staging an intent for commit; see the contract note there.
    auto src_p = Cas::parsePartFilePath(path_from);
    auto dst_p = Cas::parsePartFilePath(path_to);
    auto src = src_p ? metadata_storage.route(*src_p) : std::nullopt;
    auto dst = dst_p ? metadata_storage.route(*dst_p) : std::nullopt;

    /// RENAME TABLE / cross-engine move: both endpoints are TABLE dirs. Republish every ref (live
    /// and folded-in `detached/`-prefixed refs) plus every verbatim file under the new table
    /// identity, then drop the old namespace (the blobs/trees are content-addressed and untouched).
    ///
    /// There is no native cross-namespace atomicity (object storage has no directory rename, unlike a
    /// non-CAS disk where RENAME TABLE is a single atomic directory rename). This is a best-effort
    /// multi-op move, but it is RE-DRIVABLE/IDEMPOTENT: `republishRef` no-ops when the source ref is
    /// already gone (resolveRef miss after a prior drive moved it), `putNamespaceFile` is
    /// last-writer-wins (re-putting identical bytes is a no-op), and `dropNamespace` of an
    /// already-empty/absent namespace is a no-op. So a mid-loop throw leaves the table SPLIT across the
    /// two namespaces, but re-driving the SAME rename completes it. There is no in-call compensation;
    /// true atomicity would need a durable move-journal (deliberately out of scope — it would touch the
    /// tested GC/journal layer). On partial failure we log loudly so the split state is diagnosable.
    if (auto src_table = Cas::parseTableUuid(path_from))
    {
        if (auto dst_table = Cas::parseTableUuid(path_to))
        {
            const auto from_ns = metadata_storage.liveNamespace(*src_table);
            const auto to_ns = metadata_storage.liveNamespace(*dst_table);
            try
            {
                for (const auto & ref : metadata_storage.partAccess()->refs(from_ns, ""))
                    metadata_storage.partAccess()->republishRef({from_ns, ref}, {to_ns, ref});
                metadata_storage.partAccess()->dropNamespace(from_ns);
            }
            catch (...)
            {
                LOG_ERROR(getLogger("ContentAddressedTransaction"),
                    "RENAME TABLE move was only partially applied: the table is SPLIT across namespaces "
                    "'{}' and '{}'. The move is idempotent — retrying the same RENAME re-drives it to "
                    "completion (already-moved refs/files are no-ops). Underlying error: {}",
                    from_ns.string(), to_ns.string(), getCurrentExceptionMessage(/*with_stacktrace=*/false));
                throw;
            }
            return;
        }
    }

    if (!dst)
        throwNotAPartTarget(path_to);
    if (!src)
        throwNotAPartTarget(path_from);

    /// Projection MATERIALIZE/merge renames the staged <proj>_<n>.tmp_proj subdir to <proj>.proj
    /// inside the SAME staged part: re-key the staged entry-name prefixes so the published tree
    /// carries the final keys.
    if (!src->file.empty() && !dst->file.empty()
        && src->ns.string() == dst->ns.string() && src->ref == dst->ref
        && src->file.ends_with(".tmp_proj") && dst->file.ends_with(".proj"))
    {
        if (auto * st = findStaging(*src))
        {
            const std::string old_prefix = src->file + "/";
            const std::string new_prefix = dst->file + "/";
            for (auto & entry : st->entries)
                if (entry.path.starts_with(old_prefix))
                    entry.path = new_prefix + entry.path.substr(old_prefix.size());
            return;
        }
    }

    /// Every remaining shape is a PART-DIR move: (ns, ref) -> (ns', ref') with empty files. This
    /// uniformly covers tmp->final (staged), committed renames (delete_tmp_, merge results),
    /// DETACH (live -> detached ns), detached renames (attaching_/deleting_), and ATTACH
    /// (detached -> live ns) - in the new layout they are all the same two moves: re-key any
    /// staging, then move any committed ref.
    if (!src->ref.empty() && src->file.empty() && !dst->ref.empty() && dst->file.empty())
    {
        const std::pair<std::string, std::string> src_key{src->ns.string(), src->ref};
        const std::pair<std::string, std::string> dst_key{dst->ns.string(), dst->ref};
        if (src_key == dst_key)
            return;

        /// Re-key a STAGED source into the destination. A move carries the
        /// SOURCE's content to the destination — the POSIX `rename` semantic the rest of MergeTree
        /// assumes, and exactly what `moveFile` does (`dst[file] = src_bytes`). On the happy path the
        /// destination staging is freshly-created/empty so there is no collision at all; this only
        /// matters if some future op-order stages the same mutable file under BOTH keys.
        bool had_staged_source = false;
        if (auto src_it = parts.find(src_key); src_it != parts.end())
        {
            had_staged_source = true;
            PartStaging & dst_st = parts[dst_key];
            PartStaging & src_st = src_it->second;
            for (auto & entry : src_st.entries)
            {
                /// A genuine collision (both src and dst independently
                /// staged DIFFERING bytes for the SAME path) is a fail-loud LOGICAL_ERROR rather than a
                /// silent lost-update — the same defensive rule this loop used to apply only to the
                /// three legacy mutable names now applies uniformly to every entry (that scoping was
                /// itself a leftover of the mutable-file/entry split; there is only one kind of staged
                /// file left). Identical bytes are a benign idempotent re-key; distinct paths are the
                /// ordinary source-wins merge (a genuine collision is not expected in normal operation
                /// — only some future op-order re-keying the same file under both stagings).
                if (const auto existing = std::find_if(dst_st.entries.begin(), dst_st.entries.end(),
                        [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
                    existing != dst_st.entries.end() && !(*existing == entry))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ContentAddressed: moveDirectory file collision on '{}' ({} -> {}): "
                        "source and destination staged different bytes for the same file",
                        entry.path, src->ref, dst->ref);
                std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
                dst_st.entries.push_back(std::move(entry));
            }
            /// Carry any staged removal marks forward too — a re-key of the
            /// staging key must not silently drop them.
            for (const auto & file : src_st.content_removed)
                dst_st.content_removed.insert(file);
            /// Move pending blobs from src to dst — they will be uploaded in dst's publishStaging.
            for (auto & pb : src_st.pending_blobs)
                dst_st.pending_blobs.push_back(std::move(pb));
            src_st.pending_blobs.clear();
            if (!dst_st.build)
            {
                dst_st.build = std::move(src_st.build);
            }
            else if (src_st.build)
            {
                /// Two Builds for one destination part: keep the destination's; the source build's
                /// deps ride the staged entries (re-observed by the destination build at adopt
                /// time is unnecessary - entries staged via putBlob/adopt carry deps in the SOURCE
                /// build... merge conservatively by abandoning nothing and re-observing):
                for (const auto & entry : dst_st.entries)
                    if (entry.placement == Cas::EntryPlacement::Blob)
                    {
                        /// Unified adopt dispatch. Pending blob records were already moved
                        /// to dst_st.pending_blobs above (MOVE semantics), so copy_pending=false.
                        adoptStagedBlob(findPendingBlob(dst_st, entry.ref), entry, dst_st, *dst_st.build, /*copy_pending=*/false);
                    }
                src_st.build.reset();
            }
            parts.erase(src_it);

            /// A freshly-written part finalized tmp->final is re-keyed in the
            /// overlay above (entries/marks/pending blobs/build moved src->dst). The durable publish
            /// happens only in this transaction's commit (the existing publishStaging loop), not in
            /// this method. `MergeTree` calls that commit from `Transaction::renameParts` while off
            /// the `data_parts` lock and before the
            /// Keeper multi. No early-published ref to compensate on abort within this method
            /// (see ~ContentAddressedTransaction).
        }

        if (had_staged_source)
        {
            /// A nested text-index sub-storage (MergeTask/MutateTask createTemporaryTextIndexStorage)
            /// may have DURABLY published a committed scratch ref at THIS part's own path holding only
            /// `<part>/text_index_tmp/` files. That ref is not ours and is not staged; drop it now so the
            /// overlay we publish in commit() is the authoritative manifest. Independent of our publish
            /// timing (it targets an already-committed foreign ref), so it stays a call-time drop.
            metadata_storage.partAccess()->dropRefIfPresent(src->refKey());
            return;
        }

        /// Move any COMMITTED source ref (a merge/mutation result rename, DETACH, ATTACH, a
        /// delete_tmp_ rename, an early-committed child ref being renamed away). Absent = a pure
        /// staged/tmp move - nothing durable to touch.
        metadata_storage.partAccess()->republishRef(src->refKey(), dst->refKey());
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR,
        "ContentAddressed: moveDirectory from {} to {} has an unsupported shape", path_from, path_to);
}

void ContentAddressedTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    auto src = routeOf(path_from);
    auto dst = routeOf(path_to);
    /// A part-DIRECTORY rename reaching moveFile (PartsTemporaryRename::rollBackAll undoes an
    /// attach via moveFile): delegate to moveDirectory, which owns directory shapes.
    if (src && dst && !src->ref.empty() && src->file.empty() && !dst->ref.empty() && dst->file.empty())
    {
        moveDirectory(path_from, path_to);
        return;
    }
    if (!dst || dst->file.empty())
        throwNotAPartTarget(path_to);
    if (!src || src->file.empty())
        throwNotAPartTarget(path_from);

    auto & src_st = stagingFor(*src);
    auto & dst_st = stagingFor(*dst);

    /// Staged content entry re-keys in place (cross-part included; dependencies follow the entries).
    /// canonical policy: SOURCE-wins — a move/rename carries the source's content to the destination,
    /// overwriting any prior dest bytes (the POSIX `rename` semantic, and what the atomic-write
    /// `.tmp -> final` rename requires). moveDirectory's staged merge is aligned to this same policy.
    auto it = std::find_if(src_st.entries.begin(), src_st.entries.end(),
        [&](const Cas::ManifestEntry & e) { return e.path == src->file; });
    if (it != src_st.entries.end())
    {
        auto entry = std::move(*it);
        src_st.entries.erase(it);
        entry.path = dst->file;
        if (&src_st != &dst_st && entry.placement == Cas::EntryPlacement::Blob)
        {
            /// Unified adopt dispatch. MOVE semantics — physically move the pending blob
            /// record from src_st to dst_st FIRST (so dst_st owns the upload), then call adoptStagedBlob
            /// with copy_pending=false (the record is already in dst_st; no additional copy needed).
            auto pb_it = std::find_if(src_st.pending_blobs.begin(), src_st.pending_blobs.end(),
                [&](const PartStaging::PendingBlob & pb) { return pb.ref == entry.ref; });
            if (pb_it != src_st.pending_blobs.end())
            {
                dst_st.pending_blobs.push_back(std::move(*pb_it));
                src_st.pending_blobs.erase(pb_it);
            }
            adoptStagedBlob(findPendingBlob(dst_st, entry.ref), entry, dst_st, buildFor(*dst, dst_st), /*copy_pending=*/false);
        }
        std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == entry.path; });
        dst_st.entries.push_back(std::move(entry));
        return;
    }
    /// Source not staged in this transaction: this would cover a standalone one-shot rename of a
    /// committed `txn_version.txt` file. Atomic-write storages (including CA) bypass that rename:
    /// `VersionMetadataOnDisk::storeInfoToDataPartStorage` writes `txn_version.txt` directly, with no
    /// `.tmp` + `replaceFile` dance. This branch therefore has no live caller and is retained only as
    /// a fail-loud guard for an unsupported mutation shape.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "ContentAddressed: moveFile source not staged: {}", path_from);
}

void ContentAddressedTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    /// replaceFile = moveFile that overwrites the destination. Drop any staged destination state
    /// first, then delegate (the verbatim branch's putNamespaceFile already overwrites).
    if (auto dst = routeOf(path_to); dst && !dst->file.empty())
    {
        auto & dst_st = stagingFor(*dst);
        /// A matching pending_blobs record (if any) is left in place — its temp file is cleaned by
        /// cleanupPendingTempFiles at commit end, and the orphaned record is filtered out of the
        /// publish upload by the staged-tree-hash check in publishStaging. We do NOT purge it
        /// eagerly because the same hash may still be referenced by another staged entry.
        std::erase_if(dst_st.entries, [&](const Cas::ManifestEntry & e) { return e.path == dst->file; });
    }
    moveFile(path_from, path_to);
}

void ContentAddressedTransaction::unlinkFile(const std::string & path, bool if_exists, bool /*should_remove_objects*/)
{
    /// Part file. Two sub-cases:
    ///   1. A file STAGED in this transaction: drop the staged state so it never reaches the held part.
    ///   2. A file of a held part (not staged here): stage a REMOVAL MARK (`content_removed`), applied to the
    ///      held part at publish (`publishStaging`) -- unless this same transaction also removes the whole
    ///      part directory (`removeDirectory`), which supersedes the marks. The MergeTree fast removal
    ///      unlinks every file of a part one by one and THEN removes its directory, so that storm costs one
    ///      forget; a lone unlink (ATTACH's `removeVersionMetadata`) edits the held part.
    if (auto r = routeOf(path); r && !r->file.empty())
    {
        auto & st = stagingFor(*r);
        const bool staged_here = std::any_of(st.entries.begin(), st.entries.end(),
                           [&](const Cas::ManifestEntry & e) { return e.path == r->file; });
        /// A matching pending_blobs record (if any) is left in place — its temp file is cleaned by
        /// cleanupPendingTempFiles at commit end, and the orphaned record is filtered out of the
        /// publish upload by the staged-tree-hash check in publishStaging. We do NOT purge it
        /// eagerly because the same hash may still be referenced by another staged entry.
        std::erase_if(st.entries, [&](const Cas::ManifestEntry & e) { return e.path == r->file; });
        if (!staged_here)
        {
            const auto view = metadata_storage.partAccess()->getView(r->refKey());
            if (!view || !view->hasFile(r->file))
            {
                if (if_exists)
                    return;
                throw Exception(
                    ErrorCodes::FILE_DOESNT_EXIST,
                    "ContentAddressed: unlinkFile target does not exist: {}",
                    path);
            }
            st.content_removed.insert(r->file);
        }
        return;
    }

    if (if_exists)
        return;
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: unlinkFile target does not exist: {}", path);
}

void ContentAddressedTransaction::truncateFile(const std::string &, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "truncateFile is not supported on a content-addressed disk (blobs are immutable; "
        "whole-file rewrites replace the staged entry instead)");
}

}

namespace DB::Cas
{

namespace
{
/// Ceiling for a CAS write-buffer allocation. An extreme `max_compress_block_size` (or any other
/// out-of-range buffer-size setting -- fuzzed or misconfigured) flows into `writeFile`'s `buf_size` /
/// `adaptive_write_buffer_initial_size` and, unclamped, reaches `Memory::alloc` where the allocator's
/// `checkSize` (>= 0x8000000000000000) fires a `LOGICAL_ERROR` and aborts the server in
/// debug/sanitizer builds. The ordinary MergeTree writers clamp compress-block sizes to 256 MiB
/// (`MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE`) for exactly this reason; the CAS write path
/// received the value unclamped. Mirror that ceiling here, at the allocation site, so no caller can
/// pass an absurd size to the allocator. 256 MiB is duplicated (not #included) to keep the Disks layer
/// free of a Storages/MergeTree dependency; the regression guard is
/// `04070_no_crash_extreme_compress_block_size` run on a content-addressed storage policy.
constexpr size_t kMaxCasWriteBufferBytes = 256ULL * 1024 * 1024;

size_t clampCasWriteBufferSize(size_t size)
{
    return std::min(size, kMaxCasWriteBufferBytes);
}
}


CaContentWriteBuffer::CaContentWriteBuffer(
    std::string temp_dir,
    Cas::BlobHashAlgo hash_algo,
    size_t buf_size,
    bool use_adaptive_buffer_size,
    size_t adaptive_buffer_initial_size,
    OnFinalized on_finalized_)
    : WriteBufferFromFileBase(clampCasWriteBufferSize(use_adaptive_buffer_size ? adaptive_buffer_initial_size : buf_size), nullptr, 0)
    , on_finalized(std::move(on_finalized_))
{
    fs::create_directories(temp_dir);
    temp_path = temp_dir + "/" + getRandomASCIIString(32) + ".tmp";

    /// The spill buffer is a SECOND per-stream buffer; thread the adaptive flag into it too so a
    /// wide part keeps its footprint small. Its IO is a local temp file, not the remote stream.
    sink = std::make_unique<WriteBufferFromFile>(
        temp_path,
        clampCasWriteBufferSize(buf_size),
        /*flags=*/-1,
        /*throttler=*/nullptr,
        /*mode=*/0666,
        /*existing_memory=*/nullptr,
        /*alignment=*/0,
        use_adaptive_buffer_size,
        clampCasWriteBufferSize(adaptive_buffer_initial_size));
    hashing = Cas::makeBlobHashingWriteBuffer(hash_algo, *sink);
}

CaContentWriteBuffer::~CaContentWriteBuffer()
{
    /// Best-effort cleanup if finalize was never reached (exception unwind / cancel).
    cancel();
    /// If on_finalized ran successfully the transaction owns the staged bytes and cleans them up.
    if (!temp_ownership_transferred)
        removeTempFile();
}

void CaContentWriteBuffer::nextImpl()
{
    if (!offset())
        return;
    hashing->write(working_buffer.begin(), offset());
}

void CaContentWriteBuffer::finalizeImpl()
{
    next();
    const size_t size = count();

    /// getHashHex flushes the chain and returns the streaming digest (the pool's selected algo) of
    /// everything written, as 32 lowercase hex chars.
    const std::string hash_hex = hashing->getHashHex();

    hashing->finalize();
    sink->finalize();

    /// On successful finalize, ownership of temp_path transfers to the caller (the transaction uploads it and cleans up). cancel() still
    /// removes/cancels it.
    if (on_finalized)
    {
        on_finalized(hash_hex, size, temp_path);
        temp_ownership_transferred = true;
    }
}

void CaContentWriteBuffer::cancelImpl() noexcept
{
    if (hashing)
        hashing->cancel();
    if (sink)
        sink->cancel();
    removeTempFile();
}

void CaContentWriteBuffer::removeTempFile() noexcept
{
    std::error_code ec;
    fs::remove(temp_path, ec);
}

void CaContentWriteBuffer::sync()
{
    next();
    hashing->next();
    sink->sync();
}

std::string CaContentWriteBuffer::getFileName() const
{
    return temp_path;
}

CaInlineWriteBuffer::CaInlineWriteBuffer(OnInlined on_inlined_)
    : WriteBufferFromFileBase(DBMS_DEFAULT_BUFFER_SIZE, nullptr, 0)
    , on_inlined(std::move(on_inlined_))
{
}

CaInlineWriteBuffer::~CaInlineWriteBuffer()
{
    cancel();
}

void CaInlineWriteBuffer::nextImpl()
{
    if (!offset())
        return;
    accumulated.append(working_buffer.begin(), offset());
}

void CaInlineWriteBuffer::finalizeImpl()
{
    next();
    if (on_inlined)
        on_inlined(std::move(accumulated));
}

void CaInlineWriteBuffer::sync()
{
    next();
}

std::string CaInlineWriteBuffer::getFileName() const
{
    return "ca_inline";
}

}
