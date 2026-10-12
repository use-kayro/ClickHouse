#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasKeeperGc.h>
#include <Common/ThreadPool.h>
#include <base/types.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>

namespace DB::Cas
{

/// The decoupled, pure-data record the Disks-layer GC scheduler emits per round. It carries NO
/// Interpreters dependency: the metadata storage converts it into a
/// ContentAddressedGarbageCollectionLogElement and forwards it to the SystemLog. Keeping it a plain
/// POD lets the scheduler (and its unit tests) stay free of the system-log machinery.
struct GcRoundLogRecord
{
    /// `Phase`: one row per GC phase of the round, carrying that phase's own duration, semantic counts,
    /// and `ProfileEvents` delta. Emitted between the round's `Start` and `Finish`, correlated with them
    /// by `round_id`.
    enum class EventType { Start, Finish, Phase };
    /// `Success`: the round completed (`writer` says whether it ran as the writer or only read records).
    /// `Aborted`: the round threw an exception whose code names a transient condition (backend or Keeper
    /// unavailability) -- the next scheduled round retries it and nothing durable is wrong. `Stopped`: the same transient class, observed after the pool's
    /// teardown began -- a correlation, not a cause: the engine reports a refused teardown fence
    /// exactly like a lost mount fence, so the flag is the only witness, and the row says so honestly
    /// rather than reading a clean restart as a backend incident. `Failed` is reserved for everything
    /// else (a logic error, corrupted data, an unclassified code): fail-closed, an unrecognised
    /// failure reads as real -- during a teardown too.
    enum class Outcome { Unknown, Success, Failed, Aborted, Stopped };
    enum class Trigger { Scheduled, Manual };

    EventType event_type = EventType::Start;
    Outcome outcome = Outcome::Unknown;   /// Unknown until a round finishes
    Trigger trigger = Trigger::Scheduled;
    String disk_name;
    /// The `Cas::RoundReport` of a Finish row.
    bool writer = false;
    UInt64 round = 0;
    UInt64 objects_deleted = 0;
    UInt64 objects_absent = 0;
    UInt64 objects_replaced = 0;
    UInt64 objects_spared = 0;
    UInt64 duration_ms = 0;
    String error;
    /// `getCurrentExceptionCode()` of the failure on an `Aborted`/`Failed` Finish row; 0 otherwise.
    /// The structured twin of `error`: oracles and operators key on this, never on message wording.
    Int32 error_code = 0;
    /// On a `Start`/`Finish` row: the whole round's delta. On a `Phase` row: THAT PHASE's delta.
    std::map<String, UInt64> profile_events;

    /// Correlator for every row of ONE round attempt: a fresh random hex id minted per `runRoundLogged`
    /// invocation and stamped on the `Start` row, every `Phase` row, and the `Finish` row.
    ///
    /// DELIBERATELY NOT `round`: it is 0 on `Start` and on a round that does not run as the writer.
    String round_id;
    /// The GC phase this row describes; empty on `Start`/`Finish`. The literals are the ones passed to
    /// `Cas::GcPhaseTimer` at each phase's single instrumentation site.
    String phase;
    /// Wall time of this phase, in microseconds.
    UInt64 phase_duration_microseconds = 0;
    /// Phase-specific semantic counts (`Phase` rows only). The per-phase verb counts ride
    /// `profile_events` above instead, so no verb columns are invented.
    std::map<String, UInt64> phase_metrics;
};

using GcRoundLogger = std::function<void(const GcRoundLogRecord &)>;

/// True when an exception code names a condition that clears by itself -- the backend was unreachable
/// or slow, or another actor legitimately moved shared state -- so the next scheduled round is the
/// retry. False for everything else, deliberately including any code not on the list: an unrecognised
/// failure must read as a real one.
bool isTransientGcRoundError(int code);

/// Paces regular content-addressed garbage-collection rounds for one pool. The scheduler does not
/// implement the GC protocol: `Cas::KeeperGc` does, and only the writer's round deletes.
/// This class waits for a tick, runs one round, records its result, and retries after exceptions;
/// round operations are idempotent, while `LOGICAL_ERROR` exceptions remain visible in the log.
///
/// The background worker is a `ThreadFromGlobalPool` instance. It therefore has the attached `ThreadStatus` required by `ProfileEventsScope` for per-round
/// `ProfileEvents` deltas. A manual round runs on the caller's thread; if that thread has no
/// `ThreadStatus`, the per-round delta is omitted rather than making the GC round fail.
class CasGcScheduler
{
public:
    CasGcScheduler(
        Cas::PoolPtr store_,
        std::chrono::seconds interval_,
        const String & log_name,
        String disk_name_,
        Cas::KeeperRecordTablesFn tables_,
        GcRoundLogger logger_ = {});
    ~CasGcScheduler();

    /// Starts the periodic round worker. Calling `start` more than once while the scheduler is running
    /// is a no-op; after `stop`, it may be started again on the SAME instance, which keeps the round's
    /// condemnations (`SYSTEM CAS GC START`).
    void start();

    /// Stops the worker, wakes it if it is waiting, and joins it before returning. It is safe to call
    /// `stop` when the scheduler is not running and from the destructor. It also clears `i_am_leader`
    /// (after the join), so a stopped scheduler reports it no longer runs GC (`SYSTEM CAS GC STOP`).
    void stop();

    /// Wake the periodic worker so it starts the next ordinary round promptly. This does not create a
    /// second execution path: the same worker and `gc_round_mutex` remain the sole
    /// scheduled-round authority. Requests coalesce into one boolean while a round is pending.
    void requestRoundSoon();

    /// Test/diagnostics hook: run ONE round synchronously on the caller's thread. Returns the round
    /// report so the SYSTEM command / tests can inspect it. Emits a Start + Finish record.
    Cas::RoundReport runOneRoundNow(GcRoundLogRecord::Trigger trigger = GcRoundLogRecord::Trigger::Manual);

    /// Returns per-disk GC health for `system.cas_mounts`, a snapshot of this scheduler's state (kept here so
    /// process-global gauges do not collide across disks).
    struct GcHealth
    {
        bool is_leader = false;
        bool ever_succeeded = false;
        Int64 pending_reclaim = 0;             /// candidate blobs not deleted yet (`KeeperGc::pendingCount`)
        UInt64 last_success_age_seconds = 0;   /// seconds since the last round that ran as the writer (0 if never)
    };
    /// Takes a consistent-enough atomic snapshot for diagnostics: local health indicators rather than
    /// durable GC state.
    GcHealth gcHealth() const;

    /// Whether GC is quiescent right now: TRUE iff NO round is currently in flight on this scheduler
    /// (`round_in_flight` is held for the WHOLE round body via `SCOPE_EXIT` in `runRoundLogged`, on both the
    /// success and the exception path). Used by the `GC STOP` tests to
    /// prove the scheduler's worker threads were joined — no round can be mid-flight once `stop()` returned.
    bool isQuiescent() const { return !round_in_flight.load(std::memory_order_acquire); }

private:
    /// Waits for the configured interval, runs scheduled rounds while the scheduler is active, and
    /// logs exceptions before continuing with the next tick. The round lock serializes this worker
    /// with `runOneRoundNow` because the persistent `gc` object is not thread-safe.
    void loop();



    /// Run one round through the full logging path (Start record, ProfileEventsScope, Finish
    /// record). Used by BOTH loop() and runOneRoundNow. Logging is best-effort - the logger sink
    /// never throws into the round. Rethrows a round exception (after emitting an Aborted Finish).
    Cas::RoundReport runRoundLogged(GcRoundLogRecord::Trigger trigger);

    const Cas::PoolPtr store;
    const std::chrono::seconds interval;
    const LoggerPtr log;
    const String disk_name;
    const GcRoundLogger logger;

    /// One Gc for BOTH loop() and runOneRoundNow: it keeps the round's condemnations between rounds.
    Cas::KeeperGc gc;
    /// Serializes the manual round against the background round so the two never touch the single
    /// (not-thread-safe) `gc` concurrently. Distinct from `mutex`: the loop releases `mutex` before
    /// the round so stop() is not blocked, so the round cannot hold `mutex`.
    std::mutex gc_round_mutex;

    std::mutex mutex;
    std::condition_variable wake;
    bool stopping = false;
    bool round_requested = false;   /// guarded by `mutex`; coalesced external wake request
    ThreadFromGlobalPool thread;
    /// Whether the last round ran as the writer (this server writes every table on the disk).
    std::atomic<bool> i_am_leader{false};

    /// Set true for the whole body of one round (`runRoundLogged`, held across the `gc_round_mutex`
    /// critical section a scheduled or manual round runs under) and cleared when it returns, on the
    /// success AND exception paths. Read by `isQuiescent` (the GC-STOP join-completion signal).
    std::atomic<bool> round_in_flight{false};

    /// `KeeperGc::pendingCount` after the last round.
    std::atomic<Int64> pending_reclaim{0};
    /// Steady-clock timestamp of the last round that ran as the writer; zero means never.
    std::atomic<UInt64> last_success_ms{0};
};

}
