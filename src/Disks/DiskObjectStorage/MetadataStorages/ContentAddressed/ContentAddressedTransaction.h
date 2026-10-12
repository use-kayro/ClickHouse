#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartBlobs.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>
#include <Disks/WriteMode.h>
#include <Common/ThreadPool_fwd.h>
#include <span>
#include <Core/Defines.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

/// Owns the metadata-side overlay for one object-storage transaction. Part files are accumulated by routed
/// namespace/ref, then held by `commit` (`HeldParts`). A part uses one `Cas::PartBlobs` build, created
/// lazily at its first staged entry, which claims every blob the part will reference.
///
/// Content blobs are first held in local staging files and are uploaded only during publication. Inline
/// entries stay in the part's file list when they fit `INLINE_CAP`. The destructor removes the staging
/// files and releases the open builds' claims; it never holds an uncommitted part.
class ContentAddressedTransaction : public IMetadataTransaction
{
public:
    /// Borrows the metadata storage for the transaction lifetime; staged builds and resources are
    /// owned by this transaction and finalized or abandoned by `commit`/destruction.
    explicit ContentAddressedTransaction(ContentAddressedMetadataStorage & metadata_storage_);

    bool supportsChmod() const override { return false; }

    /// Publishes every staged part (uploads its blobs, holds it); if a later part fails, only the parts
    /// created by this call are forgotten again.
    void commit(const TransactionCommitOptionsVariant & options) override;

    /// Accepts only `NoCommitOptions`, delegates to `commit`, and returns its successful outcome.
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;

    /// Unsupported because content-addressed keys are generated from payload hashes, not paths.
    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    /// Returns no removal list: shared CAS objects are reclaimed only by garbage collection.
    StoredObjects getSubmittedForRemovalBlobs() override;

    /// Resolves a staged entry to its committed-style storage description. Pending blobs return no
    /// object because their bytes still reside in staging and must be read through the overlay path.
    std::optional<StoredObjects> tryGetInFlightStorageObjects(const std::string & path) const override;
    /// Opens an inline entry, pending local staging file, or already uploaded blob for a
    /// read-your-writes operation; returns null when the path is outside this transaction's overlay.
    std::unique_ptr<ReadBufferFromFileBase> tryReadFileInFlight(
        const std::string & path, const ReadSettings & settings, std::optional<size_t> read_hint) const override;
    /// Returns the logical size of a staged entry, including inline bytes and pending blob payloads.
    std::optional<uint64_t> tryGetInFlightFileSize(const std::string & path) const override;
    /// Reports only inner staged directories. The bare part directory intentionally remains absent
    /// so cleanup of a temporary, dedup-rejected part does not treat it as a real directory.
    bool hasInFlightDirectory(const std::string & path) const override;
    /// Lists immediate child names visible in the staged directory overlay.
    std::vector<std::string> listInFlightDirectory(const std::string & path) const override;

    /// This legacy object-list operation has no content-addressed equivalent and throws.
    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;

    /// Creates the buffer used by the disk transaction for this path. The returned wrapper keeps
    /// `owner` alive until deferred finalization, because its callback captures this transaction;
    /// it also applies the content-addressed append and autocommit rules before selecting a blob or inline
    /// buffer. Part blobs cannot be published independently of their part, while an inline-eligible
    /// standalone part file may autocommit into the held part.
    std::unique_ptr<WriteBufferFromFileBase> tryCreateWriteBuffer(
        const std::shared_ptr<IDiskTransaction> & owner,
        const std::string & path, size_t buf_size, WriteMode mode,
        const WriteSettings & settings, bool autocommit) override;

    /// Creates the inner buffer for a part file. Part blobs are hashed while being staged, small files are
    /// accumulated in memory and classified at finalize. Any other target is refused (`NOT_IMPLEMENTED`).
    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const std::string & path,
        size_t buf_size,
        WriteMode mode,
        const WriteSettings & settings);

    /// Directory creation is a metadata no-op because object storage has no directory objects.
    void createDirectory(const std::string & path) override;
    /// Recursive directory creation is likewise a no-op; entries create their own prefixes.
    void createDirectoryRecursive(const std::string & path) override;
    /// Drops a part ref, or does nothing for a non-part directory. A whole-part drop supersedes any
    /// per-file removal marks staged earlier in this transaction.
    void removeDirectory(const std::string & path) override;
    /// Removes refs, namespace files, and shadow objects while leaving shared CAS objects to GC.
    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;
    /// Copies an entry between parts, preserving pending staging ownership and trusted-manifest evidence.
    void createHardLink(const std::string & path_from, const std::string & path_to) override;
    /// These filesystem metadata operations are unsupported because CAS metadata is immutable and
    /// object storage exposes neither POSIX timestamps nor mode bits.
    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;
    void chmod(const String & path, mode_t mode) override;
    void setReadOnly(const std::string & path) override;
    /// Re-keys or merges staged part entries without publishing early; non-part paths use the
    /// corresponding object-storage copy/remove semantics.
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;
    /// Moves a staged entry and its pending ownership, or copies/removes a verbatim object as needed.
    void moveFile(const std::string & path_from, const std::string & path_to) override;
    /// Replaces the destination while preserving the source's content-addressed ownership rules.
    void replaceFile(const std::string & path_from, const std::string & path_to) override;
    /// Unlinks a staged entry or records a removal mark for an already committed manifest; shared
    /// blobs are never deleted directly.
    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;
    /// Truncation is not representable without rewriting the content and is unsupported.
    void truncateFile(const std::string & path, size_t size) override;

    /// Abandons open builds and cleans owned staging without publishing an uncommitted ref.
    ~ContentAddressedTransaction() override;

protected:
    ContentAddressedMetadataStorage & metadata_storage;

private:
    /// State for one routed part. The build is opened lazily at the first staged write; pending blobs remain
    /// in staging until publication.
    struct PartStaging
    {
        /// Created lazily at the first staged entry: the claims on every blob the part will reference.
        std::unique_ptr<Cas::PartBlobs> build;
        std::vector<Cas::ManifestEntry> entries;   /// staged manifest entries (uploads + adoptions)
        /// Paths removed from a held part by this transaction. Publication carries forward all its other
        /// entries. If `removeDirectory` forgets the whole part in the same transaction, that supersedes
        /// these marks and clears them.
        std::set<std::string> content_removed;
        bool published = false;                    /// set by publishStaging during commit(); the commit
                                                   /// loop is idempotent (never re-publishes a staging).

        /// `staging_key` is the private local temp path `CaContentWriteBuffer` returned at finalize; this
        /// transaction removes it on commit and abort.
        struct PendingBlob { Cas::BlobRef ref; std::string staging_key; uint64_t size = 0; };
        std::vector<PendingBlob> pending_blobs;    /// Staged blobs, uploaded at publication.
    };

    /// Keyed by (namespace string, ref name) -- the routed identity.
    std::map<std::pair<std::string, std::string>, PartStaging> parts;
    bool failed = false;

    /// Stage a content part file as a pending blob and add/replace its entry. Shared by the streaming-blob
    /// path and the inline-cap fallback.
    void stageBlobPartFile(const ContentAddressedMetadataStorage::Route & route,
                           const Cas::BlobRef & ref, size_t size, const std::string & staging_key);

    /// Returns (and, when necessary, creates) the staging state for a routed namespace/ref.
    PartStaging & stagingFor(const ContentAddressedMetadataStorage::Route & r);
    /// Finds existing staging state without creating an entry; returns nullptr when untouched.
    PartStaging * findStaging(const ContentAddressedMetadataStorage::Route & r);
    /// Finds the staged manifest entry for a routed file without consulting committed storage.
    const Cas::ManifestEntry * findStagedEntry(const ContentAddressedMetadataStorage::Route & r) const;
    /// Returns the pending (staged but not yet uploaded) blob for `ref`, or nullptr when it has
    /// already been uploaded or was never staged.
    const PartStaging::PendingBlob * findPendingBlob(const PartStaging & st, const Cas::BlobRef & ref) const;
    /// Returns the part build, creating it lazily with the routed ref as its intended destination.
    Cas::PartBlobs & buildFor(const ContentAddressedMetadataStorage::Route & r, PartStaging & st);
    /// Parses a disk path and maps a part-file path to its namespace/ref/file route.
    std::optional<ContentAddressedMetadataStorage::Route> routeOf(const std::string & path) const;

    /// Removes the local staging files after commit or abort.
    void cleanupPendingTempFiles() noexcept;

    /// Uploads through `st.build` only the pending blobs still referenced by `st.entries`. An entry
    /// removed by `unlinkFile` or `replaceFile` is skipped, but its staging resource is still cleaned
    /// by `cleanupPendingTempFiles`.
    void uploadPendingBlobs(PartStaging & st);

    /// Adopts an entry into another part. For a pending blob (`pb != nullptr`), `copy_pending` controls whether
    /// the staging record is copied into `dst_st` (hardlink semantics) or was already moved there by the caller.
    /// For an uploaded or held blob, the destination build claims it (no request).
    void adoptStagedBlob(const PartStaging::PendingBlob * pb, const Cas::ManifestEntry & entry,
                         PartStaging & dst_st, Cas::PartBlobs & dst_build, bool copy_pending);

    /// Publishes one staged part: uploads its pending blobs and holds it (or applies the staged changes to the
    /// part already held). Writes the part's key into `out_slot` when this call created it, so `commit` can
    /// forget it if a later part fails; `commit` preallocates one slot per part so this is a no-throw write.
    void publishStaging(const Cas::RootNamespace & ns, const std::string & ref, PartStaging & st,
                        std::optional<Cas::PartRefKey> & out_slot);
};

}

namespace DB::Cas
{

/// Part files that must NOT be inlined into the tree: per-column data (`.bin`) and marks (`.mrk*`/
/// `.cmrk*`) — inlining them would force a full-part fetch and destroy column-read selectivity — plus
/// `primary.idx`, which can be large (a size-threshold inlining of small primary.idx is a follow-up).
/// Everything else (the small eager metadata files) is an inline candidate, subject to INLINE_CAP.
bool partFileMustStayBlob(std::string_view file_name);

/// Writes a CONTENT part file while computing its content hash. The blob key is only known
/// once all bytes are written, so the buffer spills to a unique local temp file while hashing with
/// `hash_algo` (`Cas::makeBlobHashingWriteBuffer` — the pool's selectable blob-hash
/// function the wiring defines; the core never re-hashes payloads). `CityHash128` stays the thin
/// `HashingWriteBuffer` adapter (byte-for-byte unchanged); `XXH3_128` hashes with xxh3 instead. On
/// finalize it hands (hash_hex, size, temp_path)
/// to the owning transaction; the transaction owns the staging resource after finalize and uploads it at
/// publication, so finalizeImpl does not remove it. cancelImpl and the destructor (on error
/// paths) still remove it.
class CaContentWriteBuffer : public WriteBufferFromFileBase
{
public:
    using OnFinalized = std::function<void(const std::string & hash_hex, size_t size, const std::string & temp_path)>;

    /// Spills to a local staging file under `temp_dir` while hashing. Buffer sizing mirrors
    /// the plain object-storage backends: with adaptive sizing on, the working buffer STARTS small
    /// and grows (what min_columns_to_activate_adaptive_write_buffer toggles — a wide part keeps its
    /// per-INSERT footprint small).
    CaContentWriteBuffer(
        std::string temp_dir,
        Cas::BlobHashAlgo hash_algo,
        size_t buf_size,
        bool use_adaptive_buffer_size,
        size_t adaptive_buffer_initial_size,
        OnFinalized on_finalized_);

    ~CaContentWriteBuffer() override;

    void sync() override;
    std::string getFileName() const override;

private:
    /// Feeds bytes to the hashing/staging sink while preserving the base write-buffer contract.
    void nextImpl() override;
    /// Finalizes the sink, computes the content hash, and transfers the staging resource through
    /// `on_finalized`; after that callback succeeds, the transaction owns cleanup.
    void finalizeImpl() override;
    /// Cancels the sink and removes the local staging file.
    void cancelImpl() noexcept override;
    /// Removes the local staging path when ownership has not been transferred to the transaction.
    void removeTempFile() noexcept;

    OnFinalized on_finalized;
    /// The local temp file path (removed by removeTempFile).
    std::string temp_path;
    /// The spill sink, a local WriteBufferFromFile: a SECOND per-stream buffer wrapped by `hashing` below.
    std::unique_ptr<WriteBufferFromFileBase> sink;
    /// Built via `Cas::makeBlobHashingWriteBuffer(hash_algo, *sink)`:
    /// `CityHash128` is a thin adapter over the pre-existing `HashingWriteBuffer` convention (byte-for-byte
    /// unchanged); `XXH3_128` hashes with the pool's selected algo instead.
    std::unique_ptr<Cas::IBlobHashingWriteBuffer> hashing;
    bool temp_ownership_transferred = false;   /// Set after successful `on_finalized`; the destructor skips local cleanup.
};

/// Write buffer for bytes that live INSIDE pool metadata (a small inline part file staged into the
/// manifest tree, or a verbatim namespace file PUT on finalize). Accumulates in memory (the bytes are
/// tiny) and hands them to the callback at finalize; the callback decides where they go and whether
/// they are durable immediately (verbatim) or at commit (staged into the part's manifest entries).
class CaInlineWriteBuffer : public WriteBufferFromFileBase
{
public:
    using OnInlined = std::function<void(std::string bytes)>;

    explicit CaInlineWriteBuffer(OnInlined on_inlined_);
    ~CaInlineWriteBuffer() override;

    void sync() override;
    std::string getFileName() const override;

private:
    /// Appends bytes to the in-memory payload under the base write-buffer contract.
    void nextImpl() override;
    /// Hands the complete inline payload to the callback; the callback decides whether it is staged
    /// in a manifest or written immediately as a verbatim file.
    void finalizeImpl() override;

    OnInlined on_inlined;
    std::string accumulated;
};

}
