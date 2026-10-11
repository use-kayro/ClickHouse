#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsScope.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <base/scope_guard.h>
#include <algorithm>
#include <optional>

namespace DB::ErrorCodes
{
    extern const int S3_ERROR;
    extern const int NETWORK_ERROR;
    extern const int ABORTED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SOCKET_TIMEOUT;
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace DB::Cas
{

bool isTransientGcRoundError(int code)
{
    /// Codes that name a condition which clears without intervention: the backend refused or timed out
    /// (`S3_ERROR`, `NETWORK_ERROR`, `TIMEOUT_EXCEEDED`, `SOCKET_TIMEOUT`), another actor legitimately
    /// moved shared state (`ABORTED` -- the round CAS's own "another leader advanced it"), or memory
    /// pressure hit a manual round running on a budgeted query thread (`MEMORY_LIMIT_EXCEEDED`).
    /// Everything else -- notably `LOGICAL_ERROR`, `CORRUPTED_DATA`, `BAD_ARGUMENTS` -- stays
    /// non-transient BY OMISSION: an unrecognised code must read as a real failure, never as noise.
    return code == ErrorCodes::S3_ERROR
        || code == ErrorCodes::NETWORK_ERROR
        || code == ErrorCodes::ABORTED
        || code == ErrorCodes::TIMEOUT_EXCEEDED
        || code == ErrorCodes::SOCKET_TIMEOUT
        || code == ErrorCodes::MEMORY_LIMIT_EXCEEDED;
}

namespace
{
    /// Non-zero events of a per-round snapshot, keyed by event name. The snapshot is already a
    /// delta when produced by a ProfileEventsScope (its counters start at zero on this thread).
    std::map<String, UInt64> snapshotToMap(const ProfileEvents::Counters::Snapshot & snap)
    {
        std::map<String, UInt64> out;
        for (ProfileEvents::Event e = ProfileEvents::Event(0); e < ProfileEvents::Counters::num_counters; ++e)
        {
            const auto value = snap[e];
            if (value != 0)
                out.emplace(String(ProfileEvents::getName(e)), static_cast<UInt64>(value));
        }
        return out;
    }
}

CasGcScheduler::CasGcScheduler(
    Cas::PoolPtr store_,
    std::chrono::seconds interval_,
    const String & log_name,
    String disk_name_,
    Cas::KeeperRecordSourcesFn sources_,
    GcRoundLogger logger_)
    : store(std::move(store_))
    , interval(interval_)
    , log(getLogger(log_name))
    /// gc_id uniqueness across instances is the Gc caller obligation - a random u128 per scheduler.
    , gc_id((static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng())
    , disk_name(std::move(disk_name_))
    , logger(std::move(logger_))
    /// Thread this scheduler's own disk/srid-scoped logger (built from `log_name` above) into the
    /// round engine, so its log lines carry the same scope as the round-outcome log records.
    , gc(store, std::move(sources_), log)
{
}

CasGcScheduler::~CasGcScheduler()
{
    stop();
}

void CasGcScheduler::start()
{
    std::lock_guard lock(mutex);
    if (thread.joinable())
        return;
    stopping = false;
    thread = ThreadFromGlobalPool([this] { loop(); });
}

void CasGcScheduler::stop()
{
    {
        std::lock_guard lock(mutex);
        stopping = true;
    }
    wake.notify_all();
    if (thread.joinable())
        thread.join();
    /// Cleared AFTER the join, so it is final: a round in flight is joined above.
    i_am_leader.store(false, std::memory_order_relaxed);
}

void CasGcScheduler::requestRoundSoon()
{
    {
        std::lock_guard lock(mutex);
        if (stopping || !thread.joinable())
            return;
        round_requested = true;
    }
    wake.notify_all();
}


Cas::RoundReport CasGcScheduler::runRoundLogged(GcRoundLogRecord::Trigger trigger)
{
    using Rec = GcRoundLogRecord;

    /// Mark a round in flight for the whole body (success AND exception paths). `isQuiescent` reads this;
    /// the FORGET / `GC STOP` tests use it to prove the scheduler's workers were joined (no round can be
    /// mid-flight once `stop()` returned). See `isQuiescent`.
    round_in_flight.store(true, std::memory_order_release);
    SCOPE_EXIT({ round_in_flight.store(false, std::memory_order_release); });

    /// Best-effort: the table row must never break GC. A throwing sink is swallowed.
    auto emit = [&](const Rec & r)
    {
        if (logger)
        {
            try
            {
                logger(r);
            }
            catch (...) // NOLINT(bugprone-empty-catch)
            {
            }
        }
    };

    /// The correlator every row of this round attempt carries -- see `GcRoundLogRecord::round_id` for why
    /// it is a fresh id rather than the round number.
    const String round_id = Cas::u128ToHex(
        (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng());

    Rec start;
    start.event_type = Rec::EventType::Start;
    start.trigger = trigger;
    start.disk_name = disk_name;
    start.srid = store->poolConfig().server_root_id;
    start.gc_id = Cas::u128ToHex(gc_id);
    start.round_id = round_id;
    emit(start);

    /// The phase sink handed to the round engine for the duration of THIS round. Same best-effort
    /// discipline as `emit`: a throwing sink must never break GC. Cleared on the way out (on the
    /// exception path too), so it never outlives the locals it captures -- this guard is declared after
    /// them, so it is destroyed before them.
    gc.setPhaseSink([&](const Cas::GcPhaseRecord & p)
    {
        Rec row = start;
        row.event_type = Rec::EventType::Phase;
        row.phase = p.phase;
        row.phase_duration_microseconds = p.duration_us;
        row.phase_metrics = p.metrics;
        row.profile_events = p.profile_events;
        emit(row);
    });
    SCOPE_EXIT({ gc.setPhaseSink({}); });

    const auto t0 = std::chrono::steady_clock::now();
    /// ProfileEventsScope requires an attached ThreadStatus (CurrentThread::get throws otherwise).
    /// On the server it always is: the Scheduled path runs on a ThreadFromGlobalPool, the Manual
    /// path on the query thread. In unit tests calling runOneRoundNow on a bare gtest thread there
    /// is none — skip the per-round delta there rather than fail the round.
    std::optional<ProfileEventsScope> profile_scope;
    if (CurrentThread::isInitialized())
        profile_scope.emplace();

    auto collect_profile_events = [&]() -> std::map<String, UInt64>
    {
        if (!profile_scope)
            return {};
        return snapshotToMap(*profile_scope->getSnapshot());
    };

    Rec fin = start;
    fin.event_type = Rec::EventType::Finish;
    /// Lives OUTSIDE the try and is filled progressively by the round (the `progress` out-parameter of
    /// `runRegularRound`), so the Finish row of a THROWING round still carries everything the round
    /// durably did before it died -- `round != 0` on such a row proves the round's `gc/state` CAS
    /// committed and the failure hit the post-CAS tail.
    Cas::RoundReport rep;
    const auto fill_counters = [&fin](const Cas::RoundReport & r)
    {
        fin.round = r.round;
        fin.candidates_marked = r.candidates;
        fin.objects_deleted = r.deleted;
        fin.objects_absent = r.absent;
        fin.objects_replaced = r.replaced;
        fin.objects_spared = r.spared;
        fin.manifests_deleted = r.manifests_deleted;
        fin.entries_condemned = r.condemned;
        fin.entries_graduated = r.graduated;
        fin.entries_redeleted = r.redeleted;
        fin.fence_outs = r.fence_outs;
        fin.anomalies = r.anomalies.size();
    };
    try
    {
        gc.runRound(rep);
        if (rep.acquired_lease)
        {
            /// Keep health state per scheduler. Process-global gauges cannot distinguish multiple
            /// content-addressed disks in one server process.
            pending_reclaim.fetch_add(
                static_cast<Int64>(rep.condemned) - static_cast<Int64>(rep.redeleted),
                std::memory_order_relaxed);
            last_success_ms.store(
                static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count()),
                std::memory_order_relaxed);
        }
        fin.outcome = !rep.acquired_lease ? Rec::Outcome::NotALeader
                    : rep.deferred        ? Rec::Outcome::Deferred
                                           : Rec::Outcome::Success;
        fill_counters(rep);
        fin.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        fin.profile_events = collect_profile_events();
        emit(fin);
        return rep;
    }
    catch (...)
    {
        fin.error_code = getCurrentExceptionCode();
        /// Non-transient first, so a bug that coincides with a restart is never masked; then the
        /// teardown flag, the only witness of a refused teardown fence (see `Outcome::Stopped`).
        fin.outcome = !isTransientGcRoundError(fin.error_code) ? Rec::Outcome::Failed
                    : store->teardownBegun()                    ? Rec::Outcome::Stopped
                                                                : Rec::Outcome::Aborted;
        fin.error = getCurrentExceptionMessage(false);
        fill_counters(rep);
        fin.duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0).count();
        fin.profile_events = collect_profile_events();
        emit(fin);
        throw;
    }
}

Cas::RoundReport CasGcScheduler::runOneRoundNow(GcRoundLogRecord::Trigger trigger)
{
    std::lock_guard round_lock(gc_round_mutex);
    const Cas::RoundReport report = runRoundLogged(trigger);
    i_am_leader.store(report.acquired_lease, std::memory_order_relaxed);
    return report;
}

void CasGcScheduler::loop()
{
    setThreadName(ThreadName::CAS_GC_SCHEDULER);
    while (true)
    {
        {
            std::unique_lock lock(mutex);
            wake.wait_for(lock, interval, [this] { return stopping || round_requested; });
            if (stopping)
                return;
            round_requested = false;
        }
        /// rev.7 §3 [C1] + rev.8 §9 item 8: self-exit the pacing loop the moment the pool reaches — or is
        /// being driven toward — ANY terminal state. A NATURAL terminal transition (`VanishedReplaced` after
        /// a foreign pool took the prefix, or `IdentityLost` once the sentinels are gone) never calls
        /// `stop()` on this scheduler: only `~Pool`/FORGET join it. Without this check the loop would tick
        /// FOREVER against the vanished pool, and worse, after `VanishedReplaced` its rounds could
        /// condemn/delete the FOREIGN pool's objects. We also exit on a published FORGET intent
        /// (`vanishedIntentPublished`, still pre-terminal) — earliest-signal discipline — and on `IdentityLost`
        /// (rev.8: a fail-loud terminal state; the last G2-zombie case — eternal `CORRUPTED_DATA` retries
        /// against a half-erased pool — closes with it). Clearing `i_am_leader` before returning keeps
        /// `gcHealth` honest (a terminal, self-exited scheduler reports it no longer runs GC). The thread exits
        /// its OWN loop here — no join from this context (C6-safe); `stop()`/`~CasGcScheduler` still join the
        /// finished thread cleanly.
        if (store->isVanished() || store->vanishedIntentPublished()
            || store->lifecycle() == Cas::PoolLifecycle::IdentityLost)
        {
            i_am_leader.store(false, std::memory_order_relaxed);
            {
                std::lock_guard exit_lock(terminal_exit_mutex);
                loop_exited_on_terminal_for_test.store(true, std::memory_order_release);
            }
            terminal_exit_cv.notify_all();
            return;
        }
        try
        {
            /// LOW/benign: if stop() flips `stopping` while we're blocked here (a concurrent manual
            /// round holds gc_round_mutex), we still run one more Scheduled round once it unblocks,
            /// before the next wait_for() observes `stopping` - an accepted extra round, not a
            /// correctness issue.
            std::lock_guard round_lock(gc_round_mutex);

            /// A round that starts after the pool's teardown began would emit a Start row and be
            /// refused at its first request -- a row that says nothing. Checked here, under the
            /// round mutex, so it also covers the tick queued behind a manual round; the extra round
            /// on a plain `stop` (above) is a different race and stays as described.
            if (store->teardownBegun())
                return;

            /// runRoundLogged emits the Start + Finish table rows (incl. the per-round
            /// ProfileEvents delta) and rethrows on a round exception (after an Aborted Finish).
            const Cas::RoundReport report = runRoundLogged(GcRoundLogRecord::Trigger::Scheduled);
            i_am_leader.store(report.acquired_lease, std::memory_order_relaxed);
            if (report.acquired_lease)
                LOG_DEBUG(log, "CA GC round {}: condemned={} deleted={} absent={} replaced={} spared={}",
                    report.round, report.condemned, report.deleted, report.absent, report.replaced, report.spared);
        }
        catch (...)
        {
            if (store->teardownBegun() && isTransientGcRoundError(getCurrentExceptionCode()))
            {
                /// The disk is being torn down and the round was cut at its next request: expected,
                /// recorded as `Stopped` by `runRoundLogged`, not an error to raise.
                LOG_INFO(log, "CA GC round stopped by the disk's teardown: {}", getCurrentExceptionMessage(false));
                continue;
            }
            /// Idempotent round - the next tick retries; failures must never kill the pacing thread.
            /// runRoundLogged already emitted the classified (Aborted/Failed) Finish row before rethrowing.
            tryLogCurrentException(log, "CA GC round failed (will retry next tick)");
        }
    }
}


CasGcScheduler::GcHealth CasGcScheduler::gcHealth() const
{
    GcHealth h;
    h.is_leader = i_am_leader.load(std::memory_order_relaxed);
    h.pending_reclaim = pending_reclaim.load(std::memory_order_relaxed);
    const UInt64 last_ms = last_success_ms.load(std::memory_order_relaxed);
    h.ever_succeeded = last_ms != 0;
    if (last_ms != 0)
    {
        const UInt64 now_ms = static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        h.last_success_age_seconds = now_ms > last_ms ? (now_ms - last_ms) / 1000 : 0;
    }
    h.wedged_namespace_count = store->wedgedRefLaneCount();
    return h;
}

bool CasGcScheduler::waitForTerminalSelfExitForTest(std::chrono::milliseconds timeout)
{
    std::unique_lock lock(terminal_exit_mutex);
    return terminal_exit_cv.wait_for(lock, timeout, [this]
    {
        return loop_exited_on_terminal_for_test.load(std::memory_order_acquire);
    });
}

}
