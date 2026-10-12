#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>

#include <Common/Exception.h>

#include <functional>
#include <map>
#include <string>
#include <vector>

/// Unit coverage for the CA GC scheduler's logging sink (the source of
/// `system.cas_gc_log`). The scheduler emits a Start + Finish
/// `GcRoundLogRecord` per round through the injected `GcRoundLogger`; here we capture the records in
/// a vector and assert their shape over a real (in-memory) Pool. The rounds run with no Keeper record
/// source, so they never act as the writer (`writer = false`); a source function that throws drives the
/// failure-classification paths.
///
/// NOTE on ProfileEvents: `runOneRoundNow` runs on THIS (bare gtest) thread, which has no attached
/// `ThreadStatus`, so the scheduler's `CurrentThread::isInitialized()` guard skips per-round
/// ProfileEvents capture. The `profile_events` map is therefore EXPECTED to be empty here and this
/// test does NOT assert it non-empty (the on-server paths are attached; the functional/soak coverage
/// asserts non-empty there).

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using Rec = DB::Cas::GcRoundLogRecord;

namespace
{
/// One round emits a Start, then one Phase row per GC phase it reached, then a Finish. Tests that care
/// only about the round-outcome rows filter the phase rows out through this.
std::vector<Rec> roundRowsOnly(const std::vector<Rec> & rows)
{
    std::vector<Rec> out;
    for (const Rec & r : rows)
        if (r.event_type != Rec::EventType::Phase)
            out.push_back(r);
    return out;
}

/// The phase names of one round, in emission order.
std::vector<String> phaseNames(const std::vector<Rec> & rows, size_t from)
{
    std::vector<String> out;
    for (size_t i = from; i < rows.size(); ++i)
        if (rows[i].event_type == Rec::EventType::Phase)
            out.push_back(rows[i].phase);
    return out;
}

/// The `phase_metrics` of the named phase of one round. Fails the caller's expectation if absent.
std::map<String, UInt64> metricsOf(const std::vector<Rec> & rows, size_t from, const String & phase)
{
    for (size_t i = from; i < rows.size(); ++i)
        if (rows[i].event_type == Rec::EventType::Phase && rows[i].phase == phase)
            return rows[i].phase_metrics;
    return {};
}

/// No table on the disk: a round reads no record and does not act as the writer.
KeeperRecordTablesFn noSources()
{
    return [] { return std::optional<KeeperRecordTables>(KeeperRecordTables{}); };
}

/// A source function that fails the round with `code`, after `before_throw` ran.
KeeperRecordTablesFn throwingSources(int code, std::function<void()> before_throw = {})
{
    return [code, before_throw]() -> std::optional<KeeperRecordTables>
    {
        if (before_throw)
            before_throw();
        throw DB::Exception(code, "injected record-source failure");
    };
}
}

/// A round with no record source emits exactly a Start, the one phase it reached, and a Finish, with
/// the identity fields set on every row and `writer = false` on the Finish. `round` does not exist for
/// such a round, so `round_id` is the only thing tying its rows together -- which is why it is the
/// correlator.
TEST(CASGCLog, NonWriterRoundEmitsOnlyTheRecordsPhase)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca", noSources(),
        [&](const Rec & r) { rows.push_back(r); });
    EXPECT_FALSE(sched.runOneRoundNow(Rec::Trigger::Manual).writer);

    EXPECT_EQ(phaseNames(rows, 0), (std::vector<String>{"keeper_records"}));
    EXPECT_EQ(metricsOf(rows, 0, "keeper_records").at("tables"), 0u);
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows.front().event_type, Rec::EventType::Start);
    EXPECT_EQ(rows.back().event_type, Rec::EventType::Finish);
    EXPECT_EQ(rows.back().outcome, Rec::Outcome::Success);
    EXPECT_FALSE(rows.back().writer);
    for (const Rec & r : rows)
    {
        EXPECT_EQ(r.round_id, rows.front().round_id);
        EXPECT_EQ(r.disk_name, "ca");
        EXPECT_EQ(r.trigger, Rec::Trigger::Manual);
    }
}

/// A round whose record source throws must produce a Finish with a non-empty `error`, and
/// `runOneRoundNow` must rethrow the exception (the round failure is observable, not swallowed — the
/// logging sink itself is best-effort, but the round error propagates).
TEST(CASGCLog, AbortedFinishOnThrowingRound)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca", throwingSources(DB::ErrorCodes::BAD_ARGUMENTS),
        [&](const Rec & r) { rows.push_back(r); });

    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u) << "a throwing round still emits a Start and a (Aborted) Finish";
    EXPECT_EQ(round_rows[0].event_type, Rec::EventType::Start);
    EXPECT_EQ(round_rows[1].event_type, Rec::EventType::Finish);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Failed)
        << "BAD_ARGUMENTS is not on the transient list, so the row must read as a real failure";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::BAD_ARGUMENTS)
        << "the Finish row must carry the structured exception code, not only the message text";
    EXPECT_FALSE(round_rows[1].error.empty()) << "a failed Finish must carry the exception text";
    EXPECT_EQ(round_rows[1].disk_name, "ca");
    EXPECT_FALSE(round_rows[1].round_id.empty());
    for (const Rec & r : rows)
        EXPECT_EQ(r.round_id, round_rows[0].round_id)
            << "every row of a FAILED round must still correlate through round_id";
}

/// A round that dies with a TRANSIENT code -- the backend was unreachable, timed out, or another
/// actor moved shared state -- must be classified `Aborted`, not `Failed`: the next scheduled round
/// is the retry and nothing durable is wrong. The classifier keys on the exception CODE
/// (`isTransientGcRoundError`), never on message wording.
TEST(CASGCLog, TransientThrowIsClassifiedAborted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca", throwingSources(DB::ErrorCodes::NETWORK_ERROR),
        [&](const Rec & r) { rows.push_back(r); });

    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    EXPECT_EQ(round_rows[1].event_type, Rec::EventType::Finish);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Aborted)
        << "NETWORK_ERROR names a transient condition; the row must not read as a GC defect";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_FALSE(round_rows[1].error.empty());
}

/// The classifier itself, pinned direct: the transient list is exact and everything else fails closed.
TEST(CASGCLog, TransientErrorClassifierFailsClosed)
{
    EXPECT_TRUE(DB::Cas::isTransientGcRoundError(DB::ErrorCodes::NETWORK_ERROR));
    EXPECT_FALSE(DB::Cas::isTransientGcRoundError(DB::ErrorCodes::BAD_ARGUMENTS));
    EXPECT_FALSE(DB::Cas::isTransientGcRoundError(0));
    EXPECT_FALSE(DB::Cas::isTransientGcRoundError(-1));
}

/// Every row of one round -- its Start, each of its Phase rows, and its Finish -- carries the SAME
/// non-empty `round_id`, and two rounds carry DIFFERENT ones. That is the property the column exists
/// for: `round` is 0 on Start and on a round that did not run as the writer, so it cannot serve as the correlator.
TEST(CASGCLog, EveryRowOfARoundSharesOneRoundId)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});

    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca", noSources(),
        [&](const Rec & r) { rows.push_back(r); });

    sched.runOneRoundNow(Rec::Trigger::Manual);
    const size_t after_first = rows.size();
    ASSERT_GE(after_first, 2u);
    const String first_id = rows.front().round_id;
    EXPECT_FALSE(first_id.empty());
    for (size_t i = 0; i < after_first; ++i)
        EXPECT_EQ(rows[i].round_id, first_id) << "row " << i << " of the first round has a different round_id";

    sched.runOneRoundNow(Rec::Trigger::Manual);
    ASSERT_GT(rows.size(), after_first);
    const String second_id = rows[after_first].round_id;
    EXPECT_FALSE(second_id.empty());
    EXPECT_NE(second_id, first_id) << "two rounds must not share a round_id";
    for (size_t i = after_first; i < rows.size(); ++i)
        EXPECT_EQ(rows[i].round_id, second_id);
}

/// B3: the scheduler exposes per-disk GC health for system.cas_mounts (the process-global
/// CurrentMetrics gauges were clobbered with >= 2 CAS disks). A round that did not act as the writer
/// leaves the snapshot idle: not a leader, never succeeded, nothing pending.
TEST(CASGCHealth, NonWriterRoundLeavesHealthIdle)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});

    DB::Cas::CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca", noSources());

    const auto h0 = sched.gcHealth();
    EXPECT_FALSE(h0.is_leader);
    EXPECT_FALSE(h0.ever_succeeded);
    EXPECT_EQ(h0.pending_reclaim, 0);

    const RoundReport rep = sched.runOneRoundNow(Rec::Trigger::Manual);
    ASSERT_FALSE(rep.writer);

    const auto h1 = sched.gcHealth();
    EXPECT_FALSE(h1.is_leader);
    EXPECT_FALSE(h1.ever_succeeded);
    EXPECT_EQ(h1.pending_reclaim, 0);
}

/// `Stopped` is a transient failure observed after the pool's teardown began -- a correlation the row
/// records honestly.
TEST(CASGCLog, TransientFailureAfterTeardownBeganIsStopped)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});
    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        throwingSources(DB::ErrorCodes::NETWORK_ERROR, [&store] { store->beginTeardown(); }),
        [&](const Rec & r) { rows.push_back(r); });

    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    EXPECT_EQ(round_rows[1].event_type, Rec::EventType::Finish);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Stopped)
        << "a transient failure after the arm is the teardown cutting the round short, not an incident";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::NETWORK_ERROR);
    EXPECT_FALSE(round_rows[1].error.empty());
}

/// The rule is fail-closed: a non-transient failure that coincides with the arm stays `Failed`.
TEST(CASGCLog, NonTransientFailureCoincidingWithTeardownStaysFailed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});
    std::vector<Rec> rows;
    DB::Cas::CasGcScheduler sched(
        store, std::chrono::seconds(1), "test::gc", "ca",
        throwingSources(DB::ErrorCodes::CORRUPTED_DATA, [&store] { store->beginTeardown(); }),
        [&](const Rec & r) { rows.push_back(r); });

    EXPECT_THROW(sched.runOneRoundNow(Rec::Trigger::Manual), DB::Exception);

    const std::vector<Rec> round_rows = roundRowsOnly(rows);
    ASSERT_EQ(round_rows.size(), 2u);
    EXPECT_EQ(round_rows[1].outcome, Rec::Outcome::Failed)
        << "a bug that coincides with a restart is not masked as Stopped";
    EXPECT_EQ(round_rows[1].error_code, DB::ErrorCodes::CORRUPTED_DATA);
}
