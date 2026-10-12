#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <base/scope_guard.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <optional>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

/// A disk's teardown must not wait out a GC round. The pool's teardown flag is the open request
/// plane's fence, so a round in flight is refused at its next request, its next retry sleep or its
/// next streamed refill; the joins stay and the round becomes short. These tests pin the arm, the
/// plane wiring, the sleep wiring, and the scheduler's behaviour around a round that was cut.

namespace DB::ErrorCodes
{
extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::expectThrowsCode;

namespace DB::ContentAddressedSetting
{
extern const ContentAddressedSettingsBool gc_enabled;
}

namespace
{

PoolPtr openPlainPool(const std::shared_ptr<InMemoryBackend> & backend, PoolConfig config = {})
{
    config.pool_prefix = "p";
    config.writer = true;
    return Pool::open(backend, config);
}

/// A gate a test opens explicitly, so a thread can be held in flight without a sleep. Bounded, and it
/// names what it waited on: an unbounded wait on a premise that stopped holding hangs the binary.
struct Gate
{
    void wait(std::string_view name)
    {
        std::unique_lock lock(m);
        if (!cv.wait_for(lock, std::chrono::seconds(60), [this] { return open_; }))
            ADD_FAILURE() << "timed out waiting for '" << name << "'";
    }
    void open()
    {
        std::lock_guard lock(m);
        open_ = true;
        cv.notify_all();
    }
    std::mutex m;
    std::condition_variable cv;
    bool open_ = false;
};

/// Opens its gate on every exit from the scope, so a failing assertion cannot strand the thread
/// parked behind it.
struct GateOpenedOnExit
{
    explicit GateOpenedOnExit(Gate & gate_) : gate(gate_) {}
    GateOpenedOnExit(const GateOpenedOnExit &) = delete;
    GateOpenedOnExit & operator=(const GateOpenedOnExit &) = delete;
    ~GateOpenedOnExit() { gate.open(); }
    Gate & gate;
};

/// A real storage over a fresh local object storage, `context == nullptr`: no system log, no
/// scheduler until the first GC entry point creates one.
std::shared_ptr<DB::ContentAddressedMetadataStorage> openTestStorage()
{
    static std::atomic<uint64_t> counter{0};
    const auto scratch = std::filesystem::temp_directory_path()
        / ("cas_gc_teardown_stop_scratch_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1)));
    auto settings = DB::Cas::tests::makeSettingsForTest("test", scratch);
    /// No background scheduler: the test drives every round itself.
    settings[DB::ContentAddressedSetting::gc_enabled] = false;
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", getContext().context, settings);
    storage->startup();
    return storage;
}

/// The record source of one table this server writes, holding one record whose part no replica lists,
/// with no retention: the first round marks it, the second removes it and leaves its blob pending, so
/// that round goes on to issue a request (the pending blob's HEAD) after its `keeper_records` phase.
class WriterWithOneUnlistedRecord : public DB::ICasKeeperRecordSource
{
public:
    DB::CasKeeperRecordSnapshot snapshotCasRecords(const DB::IContentAddressedExchange &) override { return snapshot(); }
    DB::Strings markCasRecords(const std::vector<const DB::CasKeeperRecordState *> & records) override { return parts(records); }
    DB::Strings removeCasRecords(const DB::CasKeeperRecordSnapshot &, const std::vector<const DB::CasKeeperRecordState *> & records) override
    {
        return parts(records);
    }

    static DB::CasKeeperRecordSnapshot snapshot()
    {
        DB::CasKeeperRecordSnapshot snapshot;
        snapshot.writer = true;
        snapshot.records.push_back({.part = "all_1_1_0", .data = "record", .blobs = {{"data.bin", 1, UInt128(42)}},
                                    .listed = false, .version = 0, .mtime_ms = 0});
        return snapshot;
    }

private:
    static DB::Strings parts(const std::vector<const DB::CasKeeperRecordState *> & records)
    {
        DB::Strings out;
        for (const auto * record : records)
            out.push_back(record->part);
        return out;
    }
};

KeeperRecordTablesFn writerSources()
{
    return [source = std::make_shared<WriterWithOneUnlistedRecord>()]
    {
        return std::optional<KeeperRecordTables>(KeeperRecordTables{{source, WriterWithOneUnlistedRecord::snapshot()}});
    };
}

/// The disk's tables, as a provider: the one table above.
std::optional<std::vector<std::shared_ptr<DB::ICasKeeperRecordSource>>> oneWriterTable(const DB::IContentAddressedExchange &)
{
    static const auto source = std::make_shared<WriterWithOneUnlistedRecord>();
    return std::vector<std::shared_ptr<DB::ICasKeeperRecordSource>>{source};
}

uint64_t requestsTotal(const CountingBackend & backend)
{
    return backend.getTotal() + backend.headTotal() + backend.listTotal() + backend.writeTotal();
}

/// A thread-safe sink for the scheduler's rows, with a wait that never sleeps.
class RoundLogSink
{
public:
    GcRoundLogger logger()
    {
        return [this](const GcRoundLogRecord & r)
        {
            std::lock_guard lock(mutex);
            records.push_back(r);
            cv.notify_all();
        };
    }

    std::vector<GcRoundLogRecord> all()
    {
        std::lock_guard lock(mutex);
        return records;
    }

    /// The first Finish row at index >= `from`, waiting up to `timeout`; nullopt on timeout.
    std::optional<GcRoundLogRecord> waitForFinish(size_t from, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        const auto is_finish = [&]
        {
            for (size_t i = from; i < records.size(); ++i)
                if (records[i].event_type == GcRoundLogRecord::EventType::Finish)
                    return true;
            return false;
        };
        if (!cv.wait_for(lock, timeout, is_finish))
            return std::nullopt;
        for (size_t i = from; i < records.size(); ++i)
            if (records[i].event_type == GcRoundLogRecord::EventType::Finish)
                return records[i];
        return std::nullopt;
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<GcRoundLogRecord> records;
};

size_t countStarts(const std::vector<GcRoundLogRecord> & rows)
{
    size_t n = 0;
    for (const auto & r : rows)
        if (r.event_type == GcRoundLogRecord::EventType::Start)
            ++n;
    return n;
}

}

/// The exact delete GC issues runs on the same open plane as `read` above, so a delete issued after the
/// disk's shutdown (which arms teardown on this plane before the object storage's own `shutdown()` even
/// runs, see `DiskObjectStorage::shutdown()`) is refused at admission and never reaches the backend.
TEST(CASGCTeardownStop, RemoveIsRefusedAfterTeardownBeganAndNeverReachesTheBackend)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    {
        CasOperation op = store->openRequests().admit();
        orThrow(op.create("p/probe", "v", Retry::once()), "create");
    }
    backend->resetCounts();

    store->beginTeardown();

    CasOperation refused = store->openRequests().admit();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        refused.removeCurrent("p/probe", Retry::standard());
    });
    EXPECT_EQ(backend->deleteTotal(), 0u) << "a refused admission never reaches the backend";
}

/// The open plane's sleep is the interruptible one. Arming FIRST makes this a wiring test: a predicate `wait_for` whose predicate already
/// holds returns without waiting, so a plane still wired to the plain sleep cannot pass. The deadline
/// is the assertion; no sleep orders any thread.
TEST(CASGCTeardownStop, OpenPlaneSleepReturnsAtOnceOnceTeardownBegan)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);
    store->beginTeardown();

    auto paused = std::async(std::launch::async, [&store] { store->openRequests().pause(60'000); });
    EXPECT_EQ(paused.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "the open plane's sleep must observe the arm; a plain sleep holds for the full minute";
}

/// The defect itself: `shutdown` waits behind `gc_scheduler_mutex`, which a synchronous round holds
/// for its whole duration. After the fix it arms the pool first, the parked round is refused at its
/// next request, and `shutdown` returns. On the old code the arm never lands while the round is
/// parked, which is the assertion that goes red.
TEST(CASGCTeardownStop, ShutdownReturnsWhileASynchronousRoundIsParked)
{
    auto storage = openTestStorage();
    auto pool = storage->poolForTest();
    ASSERT_TRUE(pool);
    DB::setCasKeeperRecordSourcesProvider(&oneWriterTable);
    SCOPE_EXIT({ DB::setCasKeeperRecordSourcesProvider(nullptr); });

    auto parked = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    std::atomic<bool> park{false};
    std::mutex rows_mutex;
    std::vector<GcRoundLogRecord> rows;
    /// Installed before the first round: the scheduler takes its row sink when it is created.
    storage->setGcRoundRowHookForTest([&](const GcRoundLogRecord & r)
    {
        {
            std::lock_guard lock(rows_mutex);
            rows.push_back(r);
        }
        /// Park on the `keeper_records` phase row: the round holds `gc_scheduler_mutex` and, as the
        /// writer with a blob to delete, has a request ahead of it.
        if (park && r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "keeper_records")
        {
            parked->open();
            release->wait("release");
        }
    });
    storage->runOneGcRoundForTest();   /// marks the record: the next round has a pending blob to HEAD
    {
        std::lock_guard lock(rows_mutex);
        rows.clear();
    }
    park = true;

    auto round = std::async(std::launch::async, [&storage] { storage->runOneGcRoundForTest(); });
    parked->wait("parked");

    auto done = std::async(std::launch::async, [&storage] { storage->shutdown(); });

    /// The state handshake: the arm must land while the round is still parked. Bounded, and its
    /// expiry is the failure on the old code.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!pool->teardownBegun() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_TRUE(pool->teardownBegun()) << "shutdown waited for the round instead of arming the pool first";

    release->open();
    EXPECT_THROW(round.get(), DB::Exception) << "the released round must be refused at its next request";
    EXPECT_EQ(done.wait_for(std::chrono::seconds(30)), std::future_status::ready);

    std::optional<GcRoundLogRecord> finish;
    {
        std::lock_guard lock(rows_mutex);
        for (const auto & r : rows)
            if (r.event_type == GcRoundLogRecord::EventType::Finish)
                finish = r;
    }
    ASSERT_TRUE(finish.has_value());
    EXPECT_EQ(finish->outcome, GcRoundLogRecord::Outcome::Stopped);
}

/// A background round parked inside a request is refused at its NEXT request: nothing new reaches
/// the backend after the arm, the Finish row is `Stopped`, and `stop` returns with nothing in flight.
TEST(CASGCTeardownStop, BackgroundRoundIsCutAtItsNextRequest)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    RoundLogSink sink;
    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    /// Park on the SECOND round's `keeper_records`: the first marks the record, the second has its blob to HEAD.
    std::atomic<int> rounds_to_park{2};
    CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca", writerSources(),
        [log = sink.logger(), &rounds_to_park, entered, release](const GcRoundLogRecord & r)
        {
            log(r);
            if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "keeper_records" && --rounds_to_park == 0)
            {
                entered->open();
                release->wait("release");
            }
        });

    sched.start();
    entered->wait("entered");
    const size_t parked_round_rows_from = sink.all().size();   /// past the first round's Finish

    store->beginTeardown();
    const uint64_t requests_at_arm = requestsTotal(*backend);
    release->open();

    const auto finish = sink.waitForFinish(parked_round_rows_from, std::chrono::seconds(30));
    ASSERT_TRUE(finish.has_value()) << "the parked round never finished";
    EXPECT_EQ(finish->outcome, GcRoundLogRecord::Outcome::Stopped);
    sched.stop();
    EXPECT_TRUE(sched.isQuiescent());
    EXPECT_EQ(requestsTotal(*backend), requests_at_arm)
        << "after the arm no request may reach the backend: the round unwinds at the next gate";
    EXPECT_EQ(countStarts(sink.all()), 2u) << "no further round started after the arm";
}

/// The tick queued behind a manual round must not mint a Start row after the arm: it checks the
/// flag once it holds the round mutex, before it logs anything.
TEST(CASGCTeardownStop, AQueuedScheduledTickEmitsNoStartAfterTeardownBegan)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    RoundLogSink sink;
    auto entered = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    /// Park on the SECOND round's `keeper_records`: the first marks the record, the second has its blob to HEAD.
    std::atomic<int> rounds_to_park{2};
    /// An hour-long interval: the loop ticks only when asked.
    CasGcScheduler sched(store, std::chrono::seconds(3600), "test::gc", "ca", writerSources(),
        [log = sink.logger(), &rounds_to_park, entered, release](const GcRoundLogRecord & r)
        {
            log(r);
            if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "keeper_records" && --rounds_to_park == 0)
            {
                entered->open();
                release->wait("release");
            }
        });
    sched.start();
    (void)sched.runOneRoundNow(GcRoundLogRecord::Trigger::Manual);   /// marks the record

    auto manual = std::async(std::launch::async,
                             [&sched] { return sched.runOneRoundNow(GcRoundLogRecord::Trigger::Manual); });
    entered->wait("entered");

    /// The loop wakes and queues on the round mutex behind the parked manual round.
    sched.requestRoundSoon();
    store->beginTeardown();
    release->open();

    EXPECT_THROW((void)manual.get(), DB::Exception);
    sched.stop();
    const auto rows = sink.all();
    EXPECT_EQ(countStarts(rows), 2u) << "the queued tick minted a Start row after teardown began";
}
