#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include "config.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasKeeperGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartBlobs.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <IO/S3/Client.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileView.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadPipeline.h>
#include <Interpreters/Context.h>
#include <Interpreters/ContentAddressedGarbageCollectionLog.h>
#include <Common/CurrentThread.h>
#include <base/getThreadId.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/LoggingHelpers.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/sleep.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <chrono>
#include <filesystem>
#include <ctime>
#include <unordered_map>
#include <unordered_set>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event CASEventDroppedContextExpired;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool replicated_can_become_leader;
}

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int READONLY;
    extern const int BAD_ARGUMENTS;
    extern const int ABORTED;
    extern const int NETWORK_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_STATE;
}

namespace ContentAddressedSetting
{
    extern const ContentAddressedSettingsString scratch_path;
    extern const ContentAddressedSettingsString server_root_id;
    extern const ContentAddressedSettingsBool gc_enabled;
    extern const ContentAddressedSettingsUInt64 gc_interval_sec;
    extern const ContentAddressedSettingsUInt64 attempt_timeout_ms;
    extern const ContentAddressedSettingsBool blob_hash_allow_new;
}

namespace
{

/// Canonical disk-relative path: components joined by single '/', no leading/trailing slashes.
/// Callers hand paths in both shapes (the Unfreezer walks shadow dirs WITH a trailing slash);
/// namespace strings and prefix matching need the canonical form.
std::string canonicalDiskPath(const std::string & path)
{
    std::string result;
    std::string component;
    auto flush = [&]
    {
        if (component.empty())
            return;
        if (!result.empty())
            result += '/';
        result += component;
        component.clear();
    };
    for (char c : path)
    {
        if (c == '/')
            flush();
        else
            component.push_back(c);
    }
    flush();
    return result;
}

void addFirstComponent(std::unordered_set<std::string> & out, const std::string & name)
{
    const auto slash = name.find('/');
    out.emplace(slash == std::string::npos ? name : name.substr(0, slash));
}

/// Drop a trailing `@cas@` content-addressing boundary marker from a mirrored path segment, so a
/// table-dir surfaces under its logical (unsuffixed) name in directory listings.
std::string stripCasArchiveSuffix(std::string s)
{
    const auto & suffix = Cas::kCasArchiveSuffix;
    if (s.size() >= suffix.size() && std::string_view(s).ends_with(suffix))
        s.resize(s.size() - suffix.size());
    return s;
}

std::vector<std::string> toVector(std::unordered_set<std::string> && set)
{
    return std::vector<std::string>(std::make_move_iterator(set.begin()), std::make_move_iterator(set.end()));
}

/// The server uuid string (with dashes) -> the core's UInt128 server id.
UInt128 serverIdToU128(const std::string & server_id)
{
    String hex;
    hex.reserve(32);
    for (char c : server_id)
        if (c != '-')
            hex += c;
    if (hex.size() == 32)
        return Cas::hexToU128(hex);
    /// Unit-test ids ("srv1") are not uuids — hash them stably.
    UInt128 r{};
    for (char c : server_id)
        r = r * 131 + static_cast<unsigned char>(c);
    return r == UInt128(0) ? UInt128(1) : r;
}

}

ContentAddressedMetadataStorage::ContentAddressedMetadataStorage(
    ObjectStoragePtr object_storage_,
    String storage_path_prefix_,
    String server_id_,
    String disk_name_,
    ContextPtr context_,
    const ContentAddressedSettings & settings_)
    : object_storage(std::move(object_storage_))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , server_id(std::move(server_id_))
    , server_root_id(settings_[ContentAddressedSetting::server_root_id].value)
    , disk_name(!disk_name_.empty() ? disk_name_ : storage_path_prefix)
    , local_scratch_path(settings_[ContentAddressedSetting::scratch_path].value)
    , context(context_ ? std::optional<ContextWeakPtr>(context_) : std::nullopt)
    , gc_enabled(settings_[ContentAddressedSetting::gc_enabled].value)
    , gc_interval(std::chrono::seconds(settings_[ContentAddressedSetting::gc_interval_sec].value))
    , cas_attempt_timeout_ms(settings_[ContentAddressedSetting::attempt_timeout_ms].value)
    , staging_backend(settings_.stagingBackend())
    , blob_hash_algo(settings_.blobHashAlgo())
    , blob_hash_allow_new(settings_[ContentAddressedSetting::blob_hash_allow_new].value)
{
}

ContentAddressedMetadataStorage::~ContentAddressedMetadataStorage()
{
    stopAndDrainForTeardown();
}

Cas::StagingBackend ContentAddressedMetadataStorage::parseStagingBackend(const std::string & value)
{
    if (value == "local")
        return Cas::StagingBackend::Local;
    if (value == "s3")
        return Cas::StagingBackend::S3;
    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown cas_staging_backend value '{}' (expected 'local' or 's3')", value);
}

Cas::StagingBackend ContentAddressedMetadataStorage::parseStagingBackend(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    return parseStagingBackend(config.getString(config_prefix + ".staging_backend", "local"));
}

ContentAddressedMetadataStorage * ContentAddressedMetadataStorage::tryFromDisk(const DiskPtr & disk)
{
    /// The cheap predicate FIRST, never an exception probe: for every non-object-storage disk
    /// (DiskLocal & co.) `getMetadataStorage` throws NOT_IMPLEMENTED, and merely CONSTRUCTING that
    /// exception increments `system.errors` even when the throw is caught. This function runs on
    /// every asynchronous-metrics tick for every configured disk, so the old exception-as-control-
    /// flow probe polluted `system.errors` with a steady stream of NOT_IMPLEMENTED on pure-local
    /// servers — caught as a stray-error failure by strict-error tests (`test_cancel_backup`'s
    /// NoTrashChecker, Altinity PR#2073). `isContentAddressed` is a throw-free virtual (IDisk
    /// defaults to false; wrappers forward it — see ReadOnlyDiskWrapper).
    if (!disk || !disk->isContentAddressed())
        return nullptr;
    /// A content-addressed disk always implements getMetadataStorage (it IS the CA metadata
    /// storage), so no NOT_IMPLEMENTED handling is needed past the predicate.
    return dynamic_cast<ContentAddressedMetadataStorage *>(disk->getMetadataStorage().get());
}

void ContentAddressedMetadataStorage::runOneGcRoundForTest()
{
    /// The pacing scheduler must be STABLE across calls: its `KeeperGc` keeps its marked records and
    /// pending candidates in memory, and a scheduler per call would start each round from nothing.
    /// Hold gc_scheduler_mutex for the whole round: a concurrent `shutdown` waits for the round to
    /// finish because clean GC completion takes priority over fast shutdown. pointer_mutex (a
    /// separate, briefly-held mutex) only guards the scheduler snapshot/creation below, so
    /// gcHealth/store/partAccess never block behind this round.
    std::lock_guard round_lock(gc_scheduler_mutex);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot run garbage collection after ContentAddressedMetadataStorage shutdown has begun");
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        if (!gc_scheduler)
        {
            if (!cas_store)
                throwStorageNotStarted();
            gc_scheduler = makeGcScheduler(cas_store);
        }
        snapshot = gc_scheduler;
    }
    snapshot->runOneRoundNow();
}

void ContentAddressedMetadataStorage::requestGcRoundSoon()
{
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard lock(pointer_mutex);
        snapshot = gc_scheduler;
    }
    if (snapshot)
        snapshot->requestRoundSoon();
}

std::optional<Cas::CasGcScheduler::GcHealth> ContentAddressedMetadataStorage::gcHealth() const
{
    /// A brief pointer_mutex snapshot only -- this must NEVER wait behind gc_scheduler_mutex (an
    /// in-flight round can hold that for a long time; an unprivileged SELECT on
    /// system.cas_mounts must not stall behind it). The snapshot keeps the scheduler
    /// alive via its own refcount even if `shutdown` concurrently resets the member, and
    /// CasGcScheduler::gcHealth() is itself lock-free (atomic reads), so calling it outside any lock
    /// here is safe.
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard lock(pointer_mutex);
        snapshot = gc_scheduler;
    }
    if (!snapshot)
        return std::nullopt;
    return snapshot->gcHealth();
}

Cas::GcRoundLogger ContentAddressedMetadataStorage::makeGcRoundLogger() const
{
    /// Unit tests pass a null context (no system logs); the scheduler then runs with the test hook
    /// as its only sink, or without a sink.
    const std::function<void(const Cas::GcRoundLogRecord &)> hook = gc_round_row_hook_for_test;
    if (!context)
        return hook ? Cas::GcRoundLogger(hook) : Cas::GcRoundLogger{};
    const ContextWeakPtr weak_context = *context;
    /// The configured disk name (threaded from the metadata-storage factory); falls back to
    /// storage_path_prefix for callers that don't supply one (e.g. unit tests).
    const String disk = disk_name;
    const String srid = server_root_id;
    return [weak_context, disk, srid, hook](const Cas::GcRoundLogRecord & r)
    {
        if (hook)
            hook(r);
        auto ctx = weak_context.lock();
        if (!ctx)
        {
            ProfileEvents::increment(ProfileEvents::CASEventDroppedContextExpired);
            return;
        }
        auto log = ctx->getContentAddressedGarbageCollectionLog();
        if (!log)
            return;
        ContentAddressedGarbageCollectionLogElement e;
        const auto now = std::chrono::system_clock::now();
        e.event_time = std::chrono::system_clock::to_time_t(now);
        e.event_time_microseconds = timeInMicroseconds(now);
        switch (r.event_type)
        {
            case Cas::GcRoundLogRecord::EventType::Start:
                e.event_type = ContentAddressedGarbageCollectionLogElement::START;
                break;
            case Cas::GcRoundLogRecord::EventType::Finish:
                e.event_type = ContentAddressedGarbageCollectionLogElement::FINISH;
                break;
            case Cas::GcRoundLogRecord::EventType::Phase:
                e.event_type = ContentAddressedGarbageCollectionLogElement::PHASE;
                break;
        }
        e.disk_name = r.disk_name.empty() ? disk : r.disk_name;
        e.srid = srid;
        e.trigger = r.trigger == Cas::GcRoundLogRecord::Trigger::Manual
            ? ContentAddressedGarbageCollectionLogElement::MANUAL
            : ContentAddressedGarbageCollectionLogElement::SCHEDULED;
        switch (r.outcome)
        {
            case Cas::GcRoundLogRecord::Outcome::Unknown:
                e.outcome = ContentAddressedGarbageCollectionLogElement::UNKNOWN;
                break;
            case Cas::GcRoundLogRecord::Outcome::Success:
                e.outcome = ContentAddressedGarbageCollectionLogElement::SUCCESS;
                break;
            case Cas::GcRoundLogRecord::Outcome::Failed:
                e.outcome = ContentAddressedGarbageCollectionLogElement::FAILED;
                break;
            case Cas::GcRoundLogRecord::Outcome::Aborted:
                e.outcome = ContentAddressedGarbageCollectionLogElement::ABORTED;
                break;
            case Cas::GcRoundLogRecord::Outcome::Stopped:
                e.outcome = ContentAddressedGarbageCollectionLogElement::STOPPED;
                break;
        }
        e.writer = r.writer;
        e.round = r.round;
        e.objects_deleted = r.objects_deleted;
        e.objects_absent = r.objects_absent;
        e.objects_replaced = r.objects_replaced;
        e.objects_spared = r.objects_spared;
        e.duration_ms = r.duration_ms;
        e.error = r.error;
        e.error_code = r.error_code;
        e.profile_events = r.profile_events;
        e.round_id = r.round_id;
        e.phase = r.phase;
        e.phase_duration_microseconds = r.phase_duration_microseconds;
        e.phase_metrics = r.phase_metrics;
        /// Best-effort: SystemLog::add never blocks GC; a full queue drops the row with a warning.
        log->add(std::move(e));
    };
}

Cas::RoundReport ContentAddressedMetadataStorage::runGarbageCollectionRoundNow()
{
    checkNotReadOnly("GC round");
    if (!gc_enabled || !may_write_blobs)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Garbage collection runs only on the pool's writer (cas_gc_enabled and replicated_can_become_leader)");
    /// Mirror runOneGcRoundForTest: a STABLE scheduler instance across calls.
    /// Hold gc_scheduler_mutex for the whole round: a concurrent `shutdown` waits for the round to
    /// finish because clean GC completion takes priority over fast shutdown. pointer_mutex only
    /// guards the scheduler snapshot/creation below, so gcHealth/store/partAccess never block behind
    /// this round.
    std::lock_guard round_lock(gc_scheduler_mutex);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot run garbage collection after ContentAddressedMetadataStorage shutdown has begun");
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        if (!gc_scheduler)
        {
            if (!cas_store)
                throwStorageNotStarted();
            gc_scheduler = makeGcScheduler(cas_store);
        }
        snapshot = gc_scheduler;
    }
    return snapshot->runOneRoundNow(Cas::GcRoundLogRecord::Trigger::Manual);
}

std::optional<uint64_t> ContentAddressedMetadataStorage::freezeConnectTimeoutCapMs(
    [[maybe_unused]] const ObjectStoragePtr & object_storage, [[maybe_unused]] uint64_t cas_attempt_timeout_ms)
{
#if USE_AWS_S3
    const auto s3_client = object_storage->tryGetS3StorageClient();
    if (!s3_client)
        return std::nullopt;
    /// A configured zero is "unbounded" to Poco: the cap is then the attempt timeout itself.
    const auto configured = static_cast<uint64_t>(std::max<long>(0, s3_client->getClientConfiguration().connectTimeoutMs));
    return configured == 0 ? cas_attempt_timeout_ms : std::min(configured, cas_attempt_timeout_ms);
#else
    /// Without the AWS S3 client compiled in there is no S3 storage to read a connect timeout from,
    /// so nothing is frozen: the same answer an S3-less object storage gets above.
    return std::nullopt;
#endif
}

ContentAddressedMetadataStorage::PoolView ContentAddressedMetadataStorage::openPoolView() const
{
    /// Native mode rides real conditional ops (probed fail-closed by Pool::open); Local object
    /// storage has none, so the backend emulates exact token semantics in-process (single server).
    const auto mode = object_storage->getType() == ObjectStorageType::Local
        ? Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess
        : Cas::ObjectStorageBackend::Mode::Native;

    /// EmulatedSingleProcess emulates the conditional-op / exact-token semantics in-process (local
    /// object storage has none). That emulation is per-process: two servers pointed at the SAME local
    /// pool (e.g. an NFS/shared mount) each keep independent token state and would silently violate
    /// the CAS invariants — the capability probe cannot detect this (each process passes it alone).
    /// Make a shared-pool misconfiguration visible at INFO, not WARNING.
    /// An inline `disk = disk(... object_storage_type=local ...)` opens the disk on the QUERY thread, so
    /// a WARNING is forwarded to the client at the functional-test default `send_logs_level=warning` and
    /// fails EVERY such query (clickhouse-test fails a test on ANY client stderr). At INFO the message
    /// still lands in the server log for operator visibility but is not forwarded to client queries, so
    /// the ~15 CA-over-local stateless tests stop failing on a benign single-server note. (A genuinely
    /// shared local pool is a niche risk that would also surface via CAS/GC corruption; a future
    /// `system.warnings` entry could restore a louder, test-safe signal.)
    if (mode == Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess)
        LOG_INFO(
            getLogger("ContentAddressedMetadataStorage"),
            "Content-addressed disk over LOCAL object storage uses emulated in-process conditional "
            "operations — safe ONLY for a single server. Do NOT share this pool path between multiple "
            "ClickHouse servers (e.g. a shared/NFS mount): the CAS/GC invariants would break silently. "
            "Use an S3-backed pool for multi-server / shared deployments.");

    /// Key spaces per mode: the Emulated (Local) backend maps bare pool keys under
    /// getCommonKeyPrefix (the disk root dir), so the POOL prefix must be bucket-relative - strip
    /// the common prefix when the configured prefix carries it (the local factory passes the root
    /// path). Native passes keys through, so the configured prefix is used as-is (for S3 it
    /// already embeds the endpoint sub-path).
    String pool_prefix = storage_path_prefix;
    /// The configured prefix is an endpoint sub-path and usually carries a TRAILING slash
    /// ("content_addressed_s3/"); Cas::Layout joins components with '/', and a doubled slash in
    /// keys is backend-hostile (RustFS rejects "p//_probe" LIST prefixes with InvalidArgument -
    /// Some backends reject such prefixes while others merely tolerate them).
    while (!pool_prefix.empty() && pool_prefix.back() == '/')
        pool_prefix.pop_back();
    String physical_key_prefix_local;
    if (mode == Cas::ObjectStorageBackend::Mode::EmulatedSingleProcess)
    {
        physical_key_prefix_local = object_storage->getCommonKeyPrefix();
        /// Slash-tolerant strip: the common prefix usually ends with '/', the pool prefix was
        /// just trimmed of trailing slashes - compare canonical forms.
        String common_trimmed = physical_key_prefix_local;
        while (!common_trimmed.empty() && common_trimmed.back() == '/')
            common_trimmed.pop_back();
        if (!common_trimmed.empty())
        {
            if (pool_prefix == common_trimmed)
                pool_prefix.clear();
            else if (pool_prefix.starts_with(common_trimmed + "/"))
                pool_prefix = pool_prefix.substr(common_trimmed.size() + 1);
        }
        if (pool_prefix.empty())
            pool_prefix = "ca";
    }

    Cas::PoolConfig pool_config;
    pool_config.pool_prefix = pool_prefix;
    pool_config.server_id = serverIdToU128(server_id);
    /// The node-local write algorithm: `PoolMeta::createOrValidate` accepts it with no write once it is in the
    /// pool's `algos_used`; a not-yet-admitted algo is admitted via `blob_hash_allow_new` or refused.
    pool_config.blob_hash_algo = blob_hash_algo;
    pool_config.blob_hash_allow_new = blob_hash_allow_new;
    /// Only the server that writes blobs mints a brand-new pool's `_pool_meta`.
    pool_config.mint_pool_meta = may_write_blobs;
    pool_config.cas_request_budget.attempt_timeout_ms = cas_attempt_timeout_ms;
    /// Frozen here, once: see `freezeConnectTimeoutCapMs`.
    pool_config.cas_request_budget.connect_timeout_cap_ms = freezeConnectTimeoutCapMs(object_storage, cas_attempt_timeout_ms);

    /// Pool requests are single-attempt at the transport on a Native mount that writes: the CAS retry
    /// controller owns their deadline and retry policy, and a transparently retried request would outlive it.
    auto backend = std::make_shared<Cas::ObjectStorageBackend>(
        object_storage, mode,
        /*single_attempt_control_plane_=*/may_write_blobs && mode == Cas::ObjectStorageBackend::Mode::Native,
        pool_config.cas_request_budget.attempt_timeout_ms,
        pool_config.cas_request_budget.connect_timeout_cap_ms.value_or(0));
    /// A programmer error, not an input one: the backend must carry exactly the budget the pool was configured
    /// with.
    Cas::ensureBackendMatchesBudget(*backend, pool_config.cas_request_budget);

    PoolView view;
    view.physical_key_prefix = physical_key_prefix_local;
    view.pool_prefix = pool_prefix;
    view.native_token_type = backend->nativeTokenType();
    view.pool = Cas::Pool::open(std::move(backend), std::move(pool_config));
    return view;
}

void ContentAddressedMetadataStorage::startup()
{
    if (cas_store)
        return;

    ContextPtr startup_context;
    if (context)
    {
        startup_context = context->lock();
        if (!startup_context)
            throw Exception(
                ErrorCodes::INVALID_STATE,
                "Cannot start content-addressed metadata storage {} because its `Context` has expired",
                storage_path_full);
    }

    read_only = object_storage->isReadOnly();

    if (staging_backend == Cas::StagingBackend::S3
        && !read_only
        && !object_storage->supportsCopyMode(ObjectStorageCopyMode::NativeOnly))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "cas_staging_backend=s3 requires native-only same-store copy, but object storage {} does not support it",
            object_storage->getName());

    /// Only the server whose replicas can become merge leader writes blob bodies and runs the GC
    /// (TEC-851 decision of 11/10): the record GC's safety covers this process's builds only. A CAS table
    /// whose own `replicated_can_become_leader` differs from this server's is refused at creation, so this
    /// is every table's setting.
    may_write_blobs = !read_only && startup_context
        && startup_context->getMergeTreeSettings()[MergeTreeSetting::replicated_can_become_leader];

    PoolView view = openPoolView();
    physical_key_prefix = view.physical_key_prefix;
    auto pool = std::move(view.pool);
    auto uuid = Cas::u128ToHex(pool->poolMeta().pool_id);
    auto facade = std::make_shared<Cas::HeldParts>(pool->blobRefs());

    if (staging_backend == Cas::StagingBackend::S3 && !read_only)
    {
        Cas::sweepOwnMountStaging(
            *object_storage,
            physicalKey(view.pool_prefix + "/staging/" + server_root_id) + "/");
    }

    /// The background GC runs only on the disk-factory path (context non-null), when enabled, and only on
    /// the server that writes blobs: its delete is safe against this process's builds only (`BlobRefCount`).
    /// `CasGcScheduler` holds its own `PoolPtr`, so starting it against the LOCAL `pool` before publish is
    /// safe; on an unwind below its destructor stops and joins it.
    std::shared_ptr<Cas::CasGcScheduler> scheduler;
    if (startup_context && gc_enabled && may_write_blobs)
    {
        scheduler = makeGcScheduler(pool);
        scheduler->start();
    }

    /// Test-only: lets a test prove that a failure here (after everything above has succeeded, but
    /// before publish) leaves nothing published and a retry can still succeed. A no-op in production.
    if (startup_fault_injection_for_test)
        startup_fault_injection_for_test();

    {
        std::lock_guard lock(pointer_mutex);
        cas_store = std::move(pool);
        part_access = std::move(facade);
        gc_scheduler = std::move(scheduler);
    }
    pool_uuid = std::move(uuid);
    native_token_type = view.native_token_type;

    object_storage->pinConditionalOpsGenerationDialect(native_token_type == Cas::Dialect::Generation);
}

void ContentAddressedMetadataStorage::shutdown()
{
    /// Arm the pool BEFORE waiting for `gc_scheduler_mutex`: a synchronous round holds that mutex
    /// for its whole duration and releases it only once its next request is refused. The arm frees,
    /// nulls and swaps nothing -- every pointer swap still happens below, under the same locks as
    /// before -- so taking `pointer_mutex` alone here, before the outer lock, inverts no order.
    Cas::PoolPtr pool;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        pool = cas_store;
    }
    if (pool)
        pool->beginTeardown();
    std::lock_guard round_lock(gc_scheduler_mutex);
    shutdown_called = true;
    stopAndDrainForTeardown();
}

void ContentAddressedMetadataStorage::stopAndDrainForTeardown() noexcept
{
    std::shared_ptr<Cas::CasGcScheduler> old_scheduler;
    std::shared_ptr<Cas::HeldParts> old_part_access;
    Cas::PoolPtr old_pool;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        old_scheduler = std::move(gc_scheduler);
        gc_scheduler.reset();
        old_part_access = std::move(part_access);
        part_access.reset();
        /// Terminal server-shutdown semantics: a one-way trip (no server-lifecycle "remount" after
        /// shutdown). Nulling `cas_store` puts the storage back into the null-pool (ShutDown) lifecycle,
        /// so `poolAccess()`/the gate report the same operational refusal post-shutdown as pre-startup.
        old_pool = std::move(cas_store);
        cas_store.reset();
    }
    /// Everything below runs OUTSIDE `pointer_mutex`. Required, not incidental: waiting under it would
    /// block snapshot readers for the whole wait, and it is also what keeps a `Pool` destructor triggered
    /// by the last local reference from running under that mutex.
    ///
    /// Each phase is guarded SEPARATELY. One `try` around all of them would let an exception from stopping
    /// the scheduler skip the part-access release.
    const auto guarded = [](auto && phase, const char * what)
    {
        try
        {
            phase();
        }
        catch (...)
        {
            try
            {
                tryLogCurrentException(getLogger("ContentAddressedMetadataStorage"), what);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
        }
    };

    /// The destructor reaches here without `shutdown`'s arm: arm now, before the join, so a
    /// background round is refused at its next request rather than joined at its end.
    if (old_pool)
        old_pool->beginTeardown();
    guarded([&] { if (old_scheduler) old_scheduler->stop(); }, "CAS storage teardown: stopping GC");
    guarded([&] { old_part_access.reset(); }, "CAS storage teardown: releasing part access");
    /// `old_pool` is released here, at the end of the function and outside every lock.
}

void ContentAddressedMetadataStorage::gcStop()
{
    /// SYSTEM CAS GC STOP (spec §6): stop ONLY the background GC scheduler. STOP-IN-PLACE --
    /// the scheduler object is RETAINED in the member (contrast `shutdown`, which `std::move`
    /// it out and destroys it): a later `gcStart` must re-enter the SAME instance so its in-memory
    /// `KeeperGc` state survives. Keeping it in the member also keeps `gcHealth` reading the (stopped)
    /// state truthfully, rather than "no GC". Serialized against a synchronous GC round by
    /// `gc_scheduler_mutex` (a round holds it, so this waits it out).
    std::lock_guard round_lock(gc_scheduler_mutex);

    /// Snapshot the scheduler under `pointer_mutex` by COPY (never `std::move`): leave it in the member.
    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard lock(pointer_mutex);
        snapshot = gc_scheduler;
    }
    if (!snapshot)
    {
        /// No scheduler at all (GC disabled / not the writer / not started). Stopping GC on a
        /// disk that runs none is a no-op success -- the operator's intent ("no GC background activity")
        /// already holds.
        LOG_INFO(getLogger("ContentAddressedMetadataStorage"),
            "SYSTEM CAS GC STOP on content-addressed disk '{}': no GC scheduler "
            "(disabled/read-only/not started) -- nothing to stop.", disk_name);
        return;
    }
    /// `stop()` joins the worker thread and clears the in-process leadership hint. Runs OUTSIDE
    /// `pointer_mutex` (it joins threads). Idempotent: a second STOP finds an already-stopped scheduler and
    /// `stop()` is a safe no-op.
    snapshot->stop();
}

void ContentAddressedMetadataStorage::gcStart()
{
    checkNotReadOnly("GC start");
    if (!gc_enabled || !may_write_blobs)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Garbage collection is not enabled on this content-addressed disk");
    std::lock_guard round_lock(gc_scheduler_mutex);
    if (shutdown_called)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot start garbage collection after ContentAddressedMetadataStorage shutdown has begun");

    std::shared_ptr<Cas::CasGcScheduler> snapshot;
    {
        std::lock_guard ptr_lock(pointer_mutex);
        if (!gc_scheduler)
        {
            /// The writer with GC enabled but no scheduler yet: create a STABLE instance now, mirroring the
            /// GC RUN entry points, so START is meaningful.
            if (!cas_store)
                throwStorageNotStarted();
            gc_scheduler = makeGcScheduler(cas_store);
        }
        snapshot = gc_scheduler;
    }
    /// `start()` is a no-op if already running (idempotent) and re-enters the SAME instance after a stop --
    /// its `KeeperGc` state is preserved. Runs outside `pointer_mutex` for symmetry
    /// with `stop()` (it spawns threads but joins nothing, so it does not block).
    snapshot->start();
}

ContentAddressedMetadataStorage::PoolAccessSnapshot ContentAddressedMetadataStorage::poolAccess() const
{
    PoolAccessSnapshot snap;
    {
        std::lock_guard lock(pointer_mutex);
        snap.pool = cas_store;
        snap.part_access = part_access;
    }
    /// A null pool covers before-first-startup and after-shutdown uniformly -- the storage-level
    /// Constructing/ShutDown lifecycle. Fail loud (spec §1's null-pool fail-loud); there is no benign
    /// answer for a storage that has not published a pool.
    if (!snap.pool)
        throwStorageNotStarted();
    return snap;
}

void ContentAddressedMetadataStorage::throwStorageNotStarted() const
{
    /// No pool is published: the storage-level lifecycle is Constructing (before `startup`) or ShutDown
    /// (after `shutdown`). `pool_uuid` is empty ONLY before the first successful startup (written once at
    /// its end, never reset by `shutdown`), so it distinguishes the two. Immutable-after-startup, so read
    /// without `pointer_mutex`.
    const char * phase = pool_uuid.empty() ? "constructing" : "shutdown";
    throw Exception(ErrorCodes::INVALID_STATE,
        "content-addressed disk '{}' is not started (storage lifecycle: {})", disk_name, phase);
}

Cas::PoolPtr ContentAddressedMetadataStorage::store() const
{
    return poolAccess().pool;
}

Cas::PoolPtr ContentAddressedMetadataStorage::poolForTest() const
{
    std::lock_guard lock(pointer_mutex);
    return cas_store;
}

std::shared_ptr<Cas::HeldParts> ContentAddressedMetadataStorage::partAccess() const
{
    return poolAccess().part_access;
}

void ContentAddressedMetadataStorage::checkNotReadOnly(std::string_view what) const
{
    if (read_only)
        throw Exception(ErrorCodes::READONLY,
            "Content-addressed disk is opened read-only: {} is rejected", what);
}

MetadataTransactionPtr ContentAddressedMetadataStorage::createTransaction()
{
    checkNotReadOnly("writes");
    return std::make_shared<ContentAddressedTransaction>(*this);
}

String ContentAddressedMetadataStorage::stagingKeyPrefix() const
{
    /// This is the writer-owned `staging/<server_root_id>/` subtree. `store` throws `INVALID_STATE`
    /// when no pool is published; every writer caller runs after `startup`.
    return physicalKey(store()->poolConfig().pool_prefix + "/staging/" + server_root_id);
}

/// ==== namespace mapping ====

std::string ContentAddressedMetadataStorage::serverPrefix() const
{
    return server_root_id;
}

std::vector<std::string> ContentAddressedMetadataStorage::listLiveTreeChildren(const std::string & path) const
{
    const std::string canonical = canonicalDiskPath(path);
    const std::string scope = serverPrefix() + "/" + (canonical.empty() ? "" : canonical + "/");
    std::unordered_set<std::string> result;
    for (const auto & ns : partAccess()->namespaces())
        if (ns.size() > scope.size() && ns.starts_with(scope))
        {
            std::unordered_set<std::string> first;
            addFirstComponent(first, ns.substr(scope.size()));
            for (const auto & child : first)
                result.emplace(stripCasArchiveSuffix(child));
        }
    return toVector(std::move(result));
}

bool ContentAddressedMetadataStorage::liveTreeDirHasChildren(const std::string & path) const
{
    return canonicalDiskPath(path).empty() || !listLiveTreeChildren(path).empty();
}

Cas::RootNamespace ContentAddressedMetadataStorage::liveNamespace(const std::string & table_uuid) const
{
    /// Path mirroring: the table's canonical disk path with the content-addressed boundary marked by `@cas@`
    /// on the table-dir segment, prefixed by `server_root_id`. e.g. `<server_root_id>/store/3f2/3f2a…@cas@`.
    return Cas::RootNamespace{serverPrefix() + "/" + Cas::mirroredArchiveNamespace(table_uuid)};
}

std::optional<ContentAddressedMetadataStorage::Route>
ContentAddressedMetadataStorage::route(const Cas::PartFilePath & p) const
{
    /// Only a top-level part directory of a table: this disk neither holds nor writes `detached/`, `moving/`
    /// or a FREEZE `shadow/` copy (none would have a Keeper record).
    if (!p.backup_name.empty() || p.part_name == Cas::kDetachedDirName || p.part_name == Cas::kMovingDirName)
        return std::nullopt;
    return Route{.ns = liveNamespace(p.table_uuid), .ref = p.part_name, .file = p.file};
}

/// ==== read surface ====

namespace
{

/// The route of a path that names a file inside a held part: the parsed path routes to a part and names a
/// file in it.
std::optional<ContentAddressedMetadataStorage::Route> routeToPartFile(
    const ContentAddressedMetadataStorage & storage, const std::string & path)
{
    if (!Cas::isPartFilePath(path))
        return std::nullopt;
    auto p = Cas::parsePartFilePath(path);
    auto r = p ? storage.route(*p) : std::nullopt;
    if (!r || r->file.empty())
        return std::nullopt;
    return r;
}

}

bool ContentAddressedMetadataStorage::existsFile(const std::string & path) const
{
    auto r = routeToPartFile(*this, path);
    if (!r)
        return false;
    auto view = partAccess()->getView(r->refKey());
    return view && view->findFile(r->file);
}

ContentAddressedMetadataStorage::DirRoute ContentAddressedMetadataStorage::classifyDirectory(const std::string & path) const
{
    DirRoute dr;
    /// A shadow table dir also satisfies `parseTableUuid`: FREEZE copies are never held here.
    if (Cas::isShadowPath(path))
    {
        dr.shape = DirShape::Absent;
        return dr;
    }
    /// The Atomic `store/<u3>` shard dir, before `parseTableUuid` misclaims it as a non-Atomic table.
    if (Cas::isAtomicShardDir(path))
    {
        dr.shape = DirShape::AtomicShard;
        return dr;
    }
    if (auto uuid = Cas::parseTableUuid(path))
    {
        dr.shape = DirShape::TableDir;
        dr.uuid = std::move(uuid);
        return dr;
    }
    if (auto p = Cas::parsePartFilePath(path))
    {
        auto r = route(*p);
        if (r && r->file.empty())
        {
            dr.shape = DirShape::PartDir;
            dr.r = std::move(r);
            return dr;
        }
        if (r)
        {
            /// A path inside a part: the part's manifest is its complete listing, so it is a directory iff
            /// some entry lies under `<file>/`.
            dr.shape = DirShape::PartInner;
            dr.inner_prefix = r->file + "/";
            dr.r = std::move(r);
            return dr;
        }
    }
    /// `detached/`, `moving/`, a table-level subdirectory: nothing is held there.
    if (Cas::parseTableFilePath(path))
    {
        dr.shape = DirShape::Absent;
        return dr;
    }
    dr.shape = DirShape::GenericIntermediate;
    return dr;
}

bool ContentAddressedMetadataStorage::existsDirectory(const std::string & path) const
{
    const DirRoute dr = classifyDirectory(path);
    switch (dr.shape)
    {
        case DirShape::Absent:
            return false;
        case DirShape::AtomicShard:
        case DirShape::GenericIntermediate:
            return liveTreeDirHasChildren(path);
        case DirShape::TableDir:
            return !partAccess()->refs(liveNamespace(*dr.uuid), "").empty();
        case DirShape::PartDir:
            return partAccess()->existsRef(dr.r->refKey());
        case DirShape::PartInner:
        {
            auto view = partAccess()->getView(dr.r->refKey());
            return view && view->hasDirectory(*dr.inner_prefix);
        }
    }
    return false;
}

bool ContentAddressedMetadataStorage::existsFileOrDirectory(const std::string & path) const
{
    if (auto r = routeToPartFile(*this, path))
    {
        auto view = partAccess()->getView(r->refKey());
        return view && (view->hasFile(r->file) || view->hasDirectory(r->file + "/"));
    }
    return existsDirectory(path);
}

uint64_t ContentAddressedMetadataStorage::getFileSize(const std::string & path) const
{
    auto r = routeToPartFile(*this, path);
    if (!r)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no object for {}", path);
    auto view = partAccess()->getView(r->refKey());
    if (!view)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no part for {}", path);
    if (auto size = view->fileSize(r->file))
        return *size;
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: file {} not in part of {}", r->file, path);
}

Poco::Timestamp ContentAddressedMetadataStorage::getLastModified(const std::string & path) const
{
    /// Every path inside a part (the part dir, a projection dir, a file) reports when the part entered this
    /// disk: stamps only feed cleanup TTLs and system tables.
    if (auto p = Cas::parsePartFilePath(path))
        if (auto r = route(*p))
            if (const auto held_since_ms = partAccess()->heldSinceMs(r->refKey()))
                return Poco::Timestamp::fromEpochTime(static_cast<time_t>(*held_since_ms / 1000));
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no object for {}", path);
}

std::vector<std::string> ContentAddressedMetadataStorage::listDirectory(const std::string & path) const
{
    const DirRoute dr = classifyDirectory(path);
    switch (dr.shape)
    {
        case DirShape::Absent:
            return {};
        case DirShape::AtomicShard:
        case DirShape::GenericIntermediate:
            return listLiveTreeChildren(path);
        case DirShape::TableDir:
            return partAccess()->refs(liveNamespace(*dr.uuid), "");
        case DirShape::PartDir:
        {
            auto view = partAccess()->getView(dr.r->refKey());
            return view ? view->listChildren("") : std::vector<std::string>{};
        }
        case DirShape::PartInner:
        {
            auto view = partAccess()->getView(dr.r->refKey());
            return view ? view->listChildren(*dr.inner_prefix) : std::vector<std::string>{};
        }
    }
    return {};
}

DirectoryIteratorPtr ContentAddressedMetadataStorage::iterateDirectory(const std::string & path) const
{
    /// Mirror MetadataStorageFromPlainObjectStorage: iterateDirectory includes the path.
    auto names = listDirectory(path);
    std::vector<fs::path> fs_paths;
    fs_paths.reserve(names.size());
    for (const auto & child : names)
        fs_paths.push_back(fs::path(path) / child);
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

bool ContentAddressedMetadataStorage::isDirectoryEmpty(const std::string & path) const
{
    /// A part directory's files are virtual: report it (and a projection subdirectory) EMPTY so
    /// `DiskObjectStorage::removeDirectory` proceeds straight to forgetting the part instead of throwing
    /// CANNOT_RMDIR. Table dirs keep the listing-based emptiness (DROP TABLE's non-empty guard).
    if (auto p = Cas::parsePartFilePath(path))
        if (auto r = route(*p); r && (r->file.empty() || Cas::PartFolderView::projectionDirPrefix(r->file)))
            return true;
    return !iterateDirectory(path)->isValid();
}

StoredObjects ContentAddressedMetadataStorage::getStorageObjects(const std::string & path) const
{
    if (auto objects = getStorageObjectsIfExist(path))
        return *std::move(objects);
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "ContentAddressed: no object for {}", path);
}

std::optional<StoredObjects> ContentAddressedMetadataStorage::getStorageObjectsIfExist(const std::string & path) const
{
    auto r = routeToPartFile(*this, path);
    if (!r)
        return std::nullopt;
    /// ONE snapshot for both the part lookup and the pool `locate` below (see `poolAccess()`).
    const auto snap = poolAccess();
    auto view = snap.part_access->getView(r->refKey());
    const auto * entry = view ? view->findFile(r->file) : nullptr;
    if (!entry)
        return std::nullopt;
    /// An inline entry has no object of its own: `DiskObjectStorage::prepareRead` serves it via
    /// `tryGetInManifestBytes` first. The sized empty-key placeholder keeps size-only consumers working and
    /// makes any bypassing reader fail LOUDLY (never silently wrong bytes).
    if (entry->placement == Cas::EntryPlacement::Inline)
        return StoredObjects{StoredObject("", path, entry->size())};
    /// StoredObject carries no range: the PAYLOAD length is the size; the header offset is applied by
    /// `getBlobViewPlan`'s view window, the only byte-reading path.
    const auto location = snap.pool->locate(*entry);
    return StoredObjects{StoredObject(location.key, path, location.length)};
}

std::optional<String> ContentAddressedMetadataStorage::tryGetInManifestBytes(const std::string & path) const
{
    auto r = routeToPartFile(*this, path);
    if (!r)
        return std::nullopt;
    auto view = partAccess()->getView(r->refKey());
    return view ? view->inlineBytes(r->file) : std::nullopt;
}

bool ContentAddressedMetadataStorage::prepareInManifestRead(
    const std::string & path, const ReadSettings & settings, ReadPipeline & pipeline) const
{
    auto bytes = tryGetInManifestBytes(path);
    if (!bytes)
        return false;

    const auto size = bytes->size();
    auto creator = [path, data = std::move(*bytes)](
        const StoredObject &, const ReadSettings &, bool, bool) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromOwnMemoryFile>(path, data);
    };
    pipeline.setSource(std::move(creator), {StoredObject("", path, size)}, settings);
    return true;
}

std::optional<ContentAddressedMetadataStorage::BlobViewPlan> ContentAddressedMetadataStorage::getBlobViewPlan(
    const std::string & path) const
{
    auto r = routeToPartFile(*this, path);
    if (!r)
        return std::nullopt;
    const auto snap = poolAccess();
    auto view = snap.part_access->getView(r->refKey());
    const auto * entry = view ? view->findFile(r->file) : nullptr;
    if (!entry || entry->placement != Cas::EntryPlacement::Blob)
        return std::nullopt;
    const auto location = snap.pool->locate(*entry);
    BlobViewPlan plan;
    /// bytes_size is the readable extent of THIS file's window, NOT the whole blob: a right-bounded read stops
    /// at payload_end, and a shared blob's bytes beyond it belong to other files. The caches key on the
    /// physical blob key, so payload ranges are shared between every part that references the same blob.
    plan.object = StoredObject(physicalKey(location.key), path, location.offset + location.length);
    plan.payload_offset = location.offset;
    plan.payload_end = location.offset + location.length;
    return plan;
}

std::unique_ptr<ReadBufferFromFileBase> ContentAddressedMetadataStorage::readBlobPayload(
    const Cas::BlobLocation & location, const std::string & path, const ReadSettings & settings) const
{
    auto impl = object_storage->readObject(
        StoredObject(physicalKey(location.key), path, location.offset + location.length), settings);
    return std::make_unique<ReadBufferFromFileView>(
        std::move(impl), path, location.offset, location.offset + location.length);
}

/// ==== Keeper part record (`cas_part_files`) ====

std::optional<String> ContentAddressedMetadataStorage::encodeKeeperPartRecord(
    const String & part_path, const std::vector<CasKeeperBlobFile> & checksummed_files) const
{
    auto p = Cas::parsePartFilePath(part_path);
    auto r = p ? route(*p) : std::nullopt;
    if (!r || r->ref.empty() || !r->file.empty())
        return std::nullopt;
    const auto view = partAccess()->getView(r->refKey());
    if (!view)
        return std::nullopt;

    std::unordered_map<std::string_view, const CasKeeperBlobFile *> checksummed;
    for (const auto & f : checksummed_files)
        checksummed.emplace(f.path, &f);

    /// The record is the manifest's Inline entries; every Blob must be what the reader derives.
    Cas::PartFiles part_files;
    part_files.pool_id = store()->poolMeta().pool_id;
    size_t covered = 0;
    for (const auto & e : view->manifest()->entries)
    {
        const auto it = checksummed.find(e.path);
        if (e.placement == Cas::EntryPlacement::Inline)
        {
            part_files.entries.push_back(e);
            covered += it != checksummed.end();
            continue;
        }
        if (it == checksummed.end() || e.blob_size != it->second->size
            || !(e.ref == Cas::keeperBlobRef(*it->second)))
            return std::nullopt;
        ++covered;
    }
    if (covered != checksummed.size())
        return std::nullopt;   /// a checksummed file the manifest does not have
    return Cas::encodePartFiles(part_files);
}

CasKeeperPartRecord ContentAddressedMetadataStorage::decodeKeeperPartRecord(std::string_view record) const
{
    Cas::PartFiles part_files = Cas::decodePartFiles(record);
    CasKeeperPartRecord out;
    out.pool_uuid = Cas::u128ToHex(part_files.pool_id);
    out.files.reserve(part_files.entries.size());
    for (auto & e : part_files.entries)
        out.files.push_back({std::move(e.path), std::move(e.inline_bytes)});
    return out;
}

bool ContentAddressedMetadataStorage::holdKeeperPart(
    const String & part_path,
    const std::vector<CasKeeperInlineFile> & inline_files,
    const std::vector<CasKeeperBlobFile> & blob_files)
{
    /// The same routing every read of the part uses; a non-part-directory target is a caller error.
    auto p = Cas::parsePartFilePath(part_path);
    auto r = p ? route(*p) : std::nullopt;
    if (!r || !r->file.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Keeper part target '{}' does not address a content-addressed part directory of a table", part_path);

    /// A part this disk already holds stays as it is.
    if (partAccess()->existsRef(r->refKey()))
        return false;

    std::vector<Cas::ManifestEntry> entries;
    entries.reserve(inline_files.size() + blob_files.size());
    for (const auto & f : inline_files)
    {
        Cas::ManifestEntry e;
        e.path = f.path;
        e.placement = Cas::EntryPlacement::Inline;
        e.ref = Cas::poolContentHash(store()->writeAlgo(), f.bytes);
        e.inline_bytes = f.bytes;
        entries.push_back(std::move(e));
    }
    for (const auto & f : blob_files)
    {
        Cas::ManifestEntry e;
        e.path = f.path;
        e.placement = Cas::EntryPlacement::Blob;
        e.ref = Cas::keeperBlobRef(f);
        e.blob_size = f.size;
        entries.push_back(std::move(e));
    }
    partAccess()->install(r->refKey(), std::move(entries));
    return true;
}

void Cas::sweepOwnMountStaging(IObjectStorage & object_storage, const String & mount_staging_prefix) noexcept
{
    try
    {
        RelativePathsWithMetadata children;
        object_storage.listObjects(mount_staging_prefix, children, /*max_keys=*/0);
        size_t removed = 0;
        for (const auto & child : children)
        {
            try
            {
                object_storage.removeObjectIfExists(StoredObject(child->relative_path));
                ++removed;
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Best-effort: retried by the next startup's sweep.
            }
        }
        if (removed)
            LOG_INFO(getLogger("CasStagingSweeper"), "Reclaimed {} leaked S3 staging object(s) under '{}' at startup",
                removed, mount_staging_prefix);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Best-effort: a LIST failure must never fail the startup; the debris waits for the next sweep.
    }
}

void throwNotPersistedOnContentAddressedDisk(const String & what)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "{} is not supported on a content-addressed disk: it persists only the parts a ReplicatedMergeTree table "
        "commits, as their Keeper records", what);
}

namespace
{
std::atomic<CasKeeperRecordSourcesProvider> keeper_record_sources_provider{nullptr};
}

void setCasKeeperRecordSourcesProvider(CasKeeperRecordSourcesProvider provider)
{
    keeper_record_sources_provider.store(provider);
}

CasKeeperRecordSourcesProvider getCasKeeperRecordSourcesProvider()
{
    return keeper_record_sources_provider.load();
}

std::shared_ptr<Cas::CasGcScheduler> ContentAddressedMetadataStorage::makeGcScheduler(Cas::PoolPtr pool) const
{
    return std::make_shared<Cas::CasGcScheduler>(
        std::move(pool), gc_interval, fmt::format("{}::ContentAddressedGC", storage_path_full), disk_name,
        [this]() -> std::optional<Cas::KeeperRecordTables>
        {
            const auto provider = getCasKeeperRecordSourcesProvider();
            const auto sources = provider ? provider(*this) : std::nullopt;
            if (!sources)
                return std::nullopt;
            Cas::KeeperRecordTables tables;
            for (const auto & source : *sources)
                tables.push_back({source, source->snapshotCasRecords(*this)});
            return tables;
        },
        makeGcRoundLogger());
}

}
