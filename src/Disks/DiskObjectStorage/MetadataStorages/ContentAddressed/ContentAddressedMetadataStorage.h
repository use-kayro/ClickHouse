#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartPathParser.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartFolderAccess.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Interpreters/Context_fwd.h>
#include <base/defines.h>
#include <atomic>
#include <condition_variable>
#include <map>
#include <thread>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
}

namespace DB::Cas
{

/// Selects where a content-addressed blob is staged before it is published.
///
}

namespace DB
{

/// Adapts ClickHouse's `IMetadataStorage` path-based interface to the content-addressed pool (TEC-851 step 3c).
///
/// The object store holds blob bodies and the pool's `_pool_meta`, nothing else. A part lives in memory
/// (`partAccess()`, a `Cas::HeldParts`), held from its Keeper record or written here; its persistent copy is
/// the record ReplicatedMergeTree commits with it (`CasKeeperPartRecord.h`). Only a top-level part directory of
/// a table is a write target: a table-level file, a loose disk file, `detached/`, `moving/` and FREEZE's
/// `shadow/` are refused (`NOT_IMPLEMENTED`), since nothing would persist them. Directories are virtual.
///
/// Namespace mapping (in memory only): a live part is `SERVER_ROOT_ID/TABLE_DIR@cas@` ref `PART_DIR`.
///
/// Small per-part files (`uuid.txt`, `metadata_version.txt`, `checksums.txt`, ...) are inline-placement
/// manifest entries. Their bytes are served through `DiskObjectStorage::prepareRead`'s CA branch via
/// `tryGetInManifestBytes`; `getStorageObjects` returns a sized placeholder with an EMPTY remote key for them
/// (any consumer bypassing the prepareRead branch fails loudly, never reads wrong bytes).
class ContentAddressedMetadataStorage final : public IMetadataStorage, public IContentAddressedExchange
{
public:
    /// Constructs an unopened storage adapter. `settings_` carries every tunable that used to be a
    /// positional parameter (see `ContentAddressedSettings`) -- it is the single source of defaults, so
    /// the constructor itself declares none. `server_root_id`/`scratch_path` (the local-scratch
    /// directory used when a write buffer must spill before hashing and upload, independent of the
    /// object-storage key prefix) are read from `settings_` rather than taken as their own parameters. A
    /// non-null `context_` enables the background GC scheduler on the disk-factory path and is retained
    /// only weakly; tests may pass null to disable system-log integration and scheduling. `disk_name_` falls back to
    /// `storage_path_prefix_` when empty, exactly as before this constructor collapsed.
    ContentAddressedMetadataStorage(
        ObjectStoragePtr object_storage_,
        String storage_path_prefix_,
        String server_id_,
        String disk_name_,
        ContextPtr context_,
        const ContentAddressedSettings & settings_);
    /// Runs the same bounded, fail-soft teardown as explicit `shutdown`, including for callers that
    /// destroy the storage without first invoking its lifecycle hook.
    ~ContentAddressedMetadataStorage() override;

    /// Returns the content-addressed metadata storage backing `disk`, or nullptr if `disk` is not
    /// content-addressed. Plain (non-object-storage) disks do not implement `getMetadataStorage` at
    /// all and throw `NOT_IMPLEMENTED`; that is treated as "not content-addressed" rather than
    /// propagated. Any other exception from `getMetadataStorage` is rethrown. Centralizes the
    /// detection lambda duplicated across `InterpreterSystemQuery` and
    /// `StorageSystemContentAddressedMounts`; callers there have not yet been migrated to it.
    static ContentAddressedMetadataStorage * tryFromDisk(const DiskPtr & disk);

    /// Runs one synchronous GC round for tests and diagnostics. If the scheduler is not running, this lazily
    /// creates one, so repeated calls keep the same in-memory `KeeperGc` state.
    void runOneGcRoundForTest();

    /// Test-only shared-ownership snapshot of the pool (null when not started).
    Cas::PoolPtr poolForTest() const;

    /// What `openPoolView` freezes into `pool_config.cas_request_budget.connect_timeout_cap_ms`: the
    /// connect cap every control request and every single-attempt clone will carry for the life of
    /// this mount. `nullopt` when `object_storage` has no S3 client (the envelope is then the attempt
    /// alone); otherwise `min(disk connect_timeout_ms, cas_attempt_timeout_ms)`, with a configured zero
    /// (Poco's "unbounded") normalized to `cas_attempt_timeout_ms` rather than treated as no limit.
    /// Exposed here (not test-only) because it is a pure read of already-public state -- freezing it in
    /// one place, unit-testable without opening a pool, is what keeps a later client reload from
    /// widening the envelope the lease arithmetic was validated against.
    static std::optional<uint64_t> freezeConnectTimeoutCapMs(
        const ObjectStoragePtr & object_storage, uint64_t cas_attempt_timeout_ms);

    /// Runs one synchronous GC round on the caller's thread and emits Start and Finish rows to
    /// `system.cas_gc_log`. Throws `BAD_ARGUMENTS` when GC is disabled
    /// by read-only mode or configuration.
    Cas::RoundReport runGarbageCollectionRoundNow();

    /// Coalesce an administrative liveness hint into the existing periodic GC worker. If this disk has
    /// no running scheduler, the normal operator-visible retry path remains unchanged.
    void requestGcRoundSoon();

    /// Returns per-disk GC health for `system.cas_mounts`. Returns nullopt when this
    /// disk has no scheduler because GC is disabled, the disk is read-only, or startup has not run.
    /// Holds `gc_scheduler_mutex` for the entire call so a concurrent system-table query cannot
    /// observe a scheduler while `shutdown` destroys it.
    std::optional<Cas::CasGcScheduler::GcHealth> gcHealth() const;

    MetadataStorageType getType() const override { return MetadataStorageType::CAS; }
    const std::string & getPath() const override { return storage_path_full; }
    bool supportsChmod() const override { return false; }
    bool supportsStat() const override { return false; }
    bool isReadOnly() const override { return read_only; }
    bool isContentAddressed() const override { return true; }


    /// Fail-close gate shared by every mutating entry point (transactions, GC round): an observe-only
    /// (`<readonly>`) disk must reject them all.
    void checkNotReadOnly(std::string_view what) const;

    /// Content-addressed transactions are eager staging overlays: each mutating disk-transaction
    /// method reaches the metadata transaction immediately rather than entering the FIFO queue.
    bool transactionIsStagingOverlay() const override { return true; }
    bool supportsAtomicFileWrites() const override { return true; }
    bool supportsTransactionalMutableFiles() const override { return true; }
    bool areBlobPathsRandom() const override { return false; }
    uint32_t getHardlinkCount(const std::string &) const override { return 0; }

    /// Creates a write transaction bound to this storage. Throws `READONLY` before allocating one
    /// when the disk was opened read-only.
    MetadataTransactionPtr createTransaction() override;

    /// Opens the pool (minting `_pool_meta` only on the server that writes blobs) and starts the GC scheduler
    /// there when enabled; failures in startup propagate. Runs exactly once, single-threaded,
    /// strictly before this object is exposed to any other thread (no other method can be called
    /// concurrently with it) -- TSA_NO_THREAD_SAFETY_ANALYSIS is deliberate here, not a bypass of a
    /// real risk: pointer_mutex/gc_scheduler_mutex exist to guard concurrent access AFTER startup,
    /// which is definitionally impossible during it.
    void startup() TSA_NO_THREAD_SAFETY_ANALYSIS override;
    /// Stops the GC scheduler before releasing the part-folder facade and pool. Their destruction is
    /// synchronized with the accessors and synchronous GC entry points.
    void shutdown() override;

    /// `SYSTEM CAS GC STOP` handler (Task 11, spec §6): stops ONLY the background GC scheduler.
    /// The disk stays fully usable -- reads/writes are unaffected; this is granular operator control of the
    /// GC pacer alone. This is
    /// STOP-IN-PLACE: the scheduler object is RETAINED in the member (not detached/destroyed), so `gcHealth`
    /// keeps reading its (now stopped) state truthfully and a later `gcStart` re-enters the SAME instance
    /// with its in-memory `KeeperGc` state preserved. `stop()`
    /// joins the worker thread and clears the in-process leadership hint. Idempotent (a second
    /// STOP is a no-op); a no-op success when no scheduler exists (GC disabled / read-only / not started).
    /// Serialized by `gc_scheduler_mutex`.
    void gcStop() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// `SYSTEM CAS GC START` handler (Task 11, spec §6): restarts the background GC scheduler
    /// stopped by `gcStop`, re-entering the SAME instance (its `start()` is re-enterable after a join).
    /// Idempotent (a no-op on a running scheduler); refused on a disk that does not run GC and on a
    /// not-started disk. Serialized by `gc_scheduler_mutex`.
    void gcStart() TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Test-only fault-injection hook. When set, `startup` invokes it right before it publishes
    /// `cas_store`/`part_access`/`gc_scheduler`/`pool_uuid` -- everything
    /// up to that point (opening the pool, building the part-folder facade, starting the GC scheduler) has already happened into locals, so throwing here proves a
    /// late startup failure publishes nothing and a retry can still succeed. Left empty (a no-op) in
    /// production.
    std::function<void()> startup_fault_injection_for_test;

    /// Tests whether a path is a file of a held part.
    bool existsFile(const std::string & path) const override;
    /// Tests whether a path names a held part (or a directory inside one), a table holding parts, or a
    /// directory above tables holding parts.
    bool existsDirectory(const std::string & path) const override;
    /// Tests both file and directory interpretations of a path.
    bool existsFileOrDirectory(const std::string & path) const override;
    /// Returns the logical payload size, excluding a blob envelope.
    uint64_t getFileSize(const std::string & path) const override;
    /// Returns when the part entered this disk.
    Poco::Timestamp getLastModified(const std::string & path) const override;
    /// Lists logical children of a virtual or mirrored directory.
    std::vector<std::string> listDirectory(const std::string & path) const override;
    /// Iterates over `listDirectory` results with each child joined to `path`.
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;
    /// Reports virtual part and projection directories as empty so removal unlinks their ref; table
    /// and container directories use their listing.
    bool isDirectoryEmpty(const std::string & path) const override;
    /// Maps a logical path to its storage object. Inline entries return a sized empty-key placeholder
    /// and must be served by the CA read branch.
    StoredObjects getStorageObjects(const std::string & path) const override;
    /// Performs one manifest lookup for part files instead of the inherited `existsFile` plus
    /// `getStorageObjects` sequence.
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    /// ==== `IContentAddressedExchange` ====
    const String & getPoolUUID() const override { return pool_uuid; }
    /// Keeper part record (`cas_part_files`): see `IContentAddressedExchange`.
    std::optional<String> encodeKeeperPartRecord(
        const String & part_path, const std::vector<CasKeeperBlobFile> & checksummed_files) const override;
    CasKeeperPartRecord decodeKeeperPartRecord(std::string_view record) const override;
    bool holdKeeperPart(const String & part_path,
                        const std::vector<CasKeeperInlineFile> & inline_files,
                        const std::vector<CasKeeperBlobFile> & blob_files) override;

    /// Whether this server writes blob bodies at all (its replicas can become merge leader).
    bool writesBlobs() const { return may_write_blobs; }

    /// ==== wiring-internal surface (the transaction + the disk's prepareRead CA branch) ====

    /// Returns a shared-ownership snapshot of the opened pool. Throws `INVALID_STATE` when no pool is
    /// published (before the first `startup`, or after `shutdown`). A thin wrapper over `poolAccess()`.
    Cas::PoolPtr store() const;
    /// Returns a shared-ownership snapshot of the held parts. Throws `INVALID_STATE` under the same
    /// not-started condition as `store()`; the snapshot keeps them alive across a concurrent `shutdown`.
    std::shared_ptr<Cas::HeldParts> partAccess() const;
    const std::string & serverRootId() const { return server_root_id; }
    const std::string & scratchPath() const { return local_scratch_path; }

    /// The bytes of an Inline-placement entry of a held part; nullopt for anything else.
    std::optional<String> tryGetInManifestBytes(const std::string & path) const;

    /// The CA read entry called by `DiskObjectStorage::prepareRead` before the generic
    /// storage-objects path: serves an inline entry's bytes from memory. Returns false for any other path.
    /// Declared on `IContentAddressedExchange` (the narrow seam `prepareRead` casts to); `BlobViewPlan`
    /// is likewise inherited from there.
    bool prepareInManifestRead(const std::string & path, const ReadSettings & settings, ReadPipeline & pipeline) const override;

    /// Resolves a blob-backed path to its physical object and payload window. Returns nullopt for any
    /// other path.
    std::optional<BlobViewPlan> getBlobViewPlan(const std::string & path) const override;

    /// Creates a seekable reader over one blob payload, excluding its envelope. Transactions use
    /// this for read-your-writes; committed reads use `getBlobViewPlan` and the normal pipeline.
    std::unique_ptr<ReadBufferFromFileBase> readBlobPayload(
        const Cas::BlobLocation & location, const std::string & path, const ReadSettings & settings) const;

    /// Maps a table UUID to its in-memory namespace.
    Cas::RootNamespace liveNamespace(const std::string & table_uuid) const;

    /// The prefix of every in-memory namespace: `server_root_id`.
    std::string serverPrefix() const;

    /// The children of a GENERIC intermediate directory (the disk root "", `store`, the `store/<u3>` shard
    /// dir): the first components, under it, of the namespaces holding parts, `@cas@`-suffixed table-dir
    /// segments surfaced under their logical name.
    std::vector<std::string> listLiveTreeChildren(const std::string & path) const;
    /// Whether `listLiveTreeChildren` is non-empty. The disk root is always considered present.
    bool liveTreeDirHasChildren(const std::string & path) const;

    /// Resolves one parsed path to its namespace, reference, and in-tree file.
    struct Route
    {
        Cas::RootNamespace ns{""};
        std::string ref;    /// the part directory
        std::string file;   /// empty => the path is the part dir itself

        /// The (ns, ref) identity subset — what the part-folder access layer keys on.
        Cas::PartRefKey refKey() const { return {ns, ref}; }
    };
    /// The route of a path inside a top-level part directory of a table: nullopt for anything else
    /// (`detached/`, `moving/`, a FREEZE `shadow/` path), which this disk neither holds nor writes.
    std::optional<Route> route(const Cas::PartFilePath & p) const;

    /// `existsDirectory` and `listDirectory` route a path through one fixed dispatch order (shadow ->
    /// atomic-shard -> table-uuid -> part -> other -> generic), computed once by `classifyDirectory`.
    enum class DirShape
    {
        Absent,        /// a FREEZE `shadow/` path, `detached/`, `moving/`, a table-level subdirectory
        AtomicShard,
        TableDir,
        PartDir,
        PartInner,     /// a directory inside a part (a projection, ...)
        GenericIntermediate,
    };

    struct DirRoute
    {
        /// Defaulted so a future classifyDirectory return path that forgets to set it fails safe
        /// (a defined shape) instead of switching on an indeterminate enum (UB). Matches the
        /// existing unreachable-fallthrough choice at the bottom of existsDirectory/listDirectory.
        DirShape shape = DirShape::GenericIntermediate;
        std::optional<Route> r;
        std::optional<std::string> uuid;
        std::optional<std::string> inner_prefix;   /// `PartInner`: `<dir>/` inside the part
    };

    /// Test-only: sees every round-log row the storage's own scheduler emits (Start, Phase, Finish),
    /// before and independently of the system-log path -- which a unit-test storage (null `Context`)
    /// does not have at all. Lets a test park a synchronous round on a phase row while it holds
    /// `gc_scheduler_mutex`, and read the round's outcome afterwards. Set before the first round.
    void setGcRoundRowHookForTest(std::function<void(const Cas::GcRoundLogRecord &)> fn)
    {
        gc_round_row_hook_for_test = std::move(fn);
    }

private:
    const ObjectStoragePtr object_storage;
    const std::string storage_path_prefix;
    const std::string storage_path_full;
    const std::string server_id;
    const std::string server_root_id;
    const std::string disk_name;
    const std::string local_scratch_path;
    /// `nullopt` means integration was deliberately disabled; an engaged reference may expire.
    const std::optional<ContextWeakPtr> context;

    const bool gc_enabled;
    const std::chrono::seconds gc_interval;
    /// The budget for one HTTP attempt of a pool request; feeds `Cas::PoolConfig::cas_request_budget` and the
    /// backend's own `attemptTimeoutMs()`.
    const uint64_t cas_attempt_timeout_ms;
    /// Configured staging backend; `Local` preserves the existing write path.
    /// Blob content-hash function passed to `Cas::PoolConfig`.
    const Cas::BlobHashAlgo blob_hash_algo;
    /// Whether `blob_hash_algo` may be admitted into the pool's persisted `algos_used` set.
    const bool blob_hash_allow_new;
    /// A single coherent snapshot of the pool and its cached part-folder facade, taken under ONE
    /// `pointer_mutex` acquisition (see `poolAccess()`) so no caller can observe `pool` from one mount
    /// generation and `part_access` from another -- the two used to be fetched by two separate calls
    /// to `store()`/`partAccess()` at some call sites, each taking its own `pointer_mutex` snapshot.
    struct PoolAccessSnapshot
    {
        Cas::PoolPtr pool;
        std::shared_ptr<Cas::HeldParts> part_access;
    };

    /// Set by startup (Pool::open is fail-closed; empty store == not started). shared_ptr so
    /// store()/partAccess() can return a by-value snapshot under `pointer_mutex` (see below) instead
    /// of a reference that could dangle across a concurrent `shutdown` reset.
    Cas::PoolPtr cas_store TSA_GUARDED_BY(pointer_mutex);
    /// The part-folder access facade: the normal path
    /// for committed part/projection reads and committed part-ref mutations. Constructed in
    /// startup right after Pool::open; reset in shutdown before cas_store. shared_ptr for the same
    /// snapshot-safety reason as cas_store.
    std::shared_ptr<Cas::HeldParts> part_access TSA_GUARDED_BY(pointer_mutex);
    String pool_uuid;
    /// The backend's incarnation-token dialect recorded at `startup` (see `openPoolView`'s `PoolView::
    /// native_token_type`) -- immutable afterwards. `startup` also hands it to the object storage as a
    /// pin, which is what refuses a reload that would flip the dialect under this live pool; the check
    /// belongs there because only the object storage knows the effective `http_client`.
    Cas::Dialect native_token_type = Cas::Dialect::ETag;
    /// shared_ptr so the synchronous GC verbs can take a snapshot under `pointer_mutex`, release it, and run
    /// the (long) round via the snapshot. Created only on a server that writes blobs.
    std::shared_ptr<Cas::CasGcScheduler> gc_scheduler TSA_GUARDED_BY(pointer_mutex);
    bool may_write_blobs = false;

    /// Serializes ONE synchronous GC round at a time. `shutdown` still takes it, but arms the pool
    /// first, so a round in flight is refused at its next request once the pool is armed and the wait
    /// is one request long -- held for the WHOLE round. Deliberately NOT the same mutex as `pointer_mutex` below: this one can be held for a
    /// long time, so nothing that only needs a brief pointer snapshot may share it.
    mutable std::mutex gc_scheduler_mutex;
    bool shutdown_called TSA_GUARDED_BY(gc_scheduler_mutex) = false;
    /// Guards ONLY reads/writes of `cas_store`/`part_access`/`gc_scheduler` themselves (snapshot,
    /// create-if-absent, reset) -- always held briefly. Lock order: `gc_scheduler_mutex`, then
    /// `pointer_mutex` nested inside, never the reverse.
    mutable std::mutex pointer_mutex;
    /// Derived from object_storage->isReadOnly() at startup (the disk's <readonly> config). When set: no GC
    /// scheduler, and the mutating surface fails closed.
    bool read_only = false;
    /// Joined in front of core keys for DIRECT object_storage reads. The Emulated (Local) backend
    /// maps bare pool keys under getCommonKeyPrefix; Native passes keys through - this member
    /// mirrors that rule so readBlobPayload reads exactly where the backend wrote ("" for Native).
    String physical_key_prefix;

    /// Adds the local-backend common prefix to a pool key when direct object-storage I/O requires
    /// the physical key; native backends use the key unchanged.
    String physicalKey(const String & key) const
    {
        if (physical_key_prefix.empty())
            return key;
        if (physical_key_prefix.back() == '/')
            return physical_key_prefix + key;
        return physical_key_prefix + "/" + key;
    }

    /// The one place that takes a `{pool, facade}` snapshot under a SINGLE `pointer_mutex`
    /// acquisition. Throws `INVALID_STATE` (via `throwStorageNotStarted`) when no pool is published
    /// (before the first `startup` or after `shutdown`). `store()`/`partAccess()`
    /// are thin wrappers over this; every other caller that needs BOTH the pool and the facade for one
    /// logical operation must call this ONCE and use both fields from the same snapshot, rather than
    /// calling `store()` and `partAccess()` separately -- otherwise it could straddle a startup/shutdown
    /// that changes `cas_store`/`part_access`.
    PoolAccessSnapshot poolAccess() const;

    /// Atomically unpublishes every pool-owning facade, then stops/releases/drains them in dependency
    /// order outside `pointer_mutex`. Each phase is independently fail-soft because this is also called
    /// by the destructor.
    void stopAndDrainForTeardown() noexcept;

    /// Builds and throws the `INVALID_STATE` "disk is not started" exception `poolAccess()`, the gate,
    /// and the synchronous GC round entry points throw when no pool is published -- the storage-level
    /// Constructing (before `startup`) / ShutDown (after `shutdown`) lifecycle. `pool_uuid` (written once
    /// at the end of a successful `startup`, never reset) distinguishes the two in the message. A normal,
    /// operator-facing refusal,
    /// never a `LOGICAL_ERROR` (which would abort under debug/ASan builds).
    [[noreturn]] void throwStorageNotStarted() const;

    /// A pool opened as a standalone, UNPUBLISHED view: never touches `cas_store`/`part_access`/
    /// `gc_scheduler` -- the caller owns it entirely and drops it when done.
    struct PoolView
    {
        Cas::PoolPtr pool;
        /// The direct-object-storage key prefix for this view's backend (see `physicalKey`'s own
        /// doc comment): populated for the Emulated (Local) backend, empty ("") for Native. Returned
        /// rather than written to the `physical_key_prefix` member so this helper stays side-effect-free.
        String physical_key_prefix;
        /// The resolved (bucket-relative, trailing-slash-trimmed) pool prefix passed into
        /// `Cas::PoolConfig::pool_prefix` -- `startup` needs the same resolved value to sweep this
        /// mount's S3-staging prefix, so it is returned here rather than recomputed a second time.
        String pool_prefix;
        /// The backend's native incarnation-token dialect (`Cas::ObjectStorageBackend::nativeTokenType`),
        /// captured while the concrete backend is still in scope. Reading it back through `pool`
        /// would mean unwrapping the instrumentation decorator `Pool::open` adds, so it is returned
        /// here instead.
        Cas::Dialect native_token_type = Cas::Dialect::ETag;
    };

    /// Builds the backend + `Cas::PoolConfig` and opens the pool for `startup()`. Never touches `cas_store`/`part_access`/`gc_scheduler`/`physical_key_prefix`/`pool_uuid`; `startup`
    /// applies its own result to those members itself, in its single publish step.
    PoolView openPoolView() const;

    /// Classifies `path`'s directory shape (see `DirShape`). Pure path classification, no state.
    DirRoute classifyDirectory(const std::string & path) const;

    /// Build the GC round sink: the std::function the scheduler calls per Start/Finish. Captures the
    /// weak `Context` reference, converts the POD GcRoundLogRecord into a
    /// ContentAddressedGarbageCollectionLogElement, and appends it to the SystemLog (best-effort).
    /// Returns an empty sink when context is null.
    Cas::GcRoundLogger makeGcRoundLogger() const;
    /// The GC scheduler of this disk; its rounds read the tables `getCasKeeperRecordSourcesProvider` finds on it.
    std::shared_ptr<Cas::CasGcScheduler> makeGcScheduler(Cas::PoolPtr pool) const;

    std::function<void(const Cas::GcRoundLogRecord &)> gc_round_row_hook_for_test;
};

}
