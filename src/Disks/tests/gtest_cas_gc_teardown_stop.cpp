#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRetry.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasDetachedWork.h>
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

namespace
{

PoolPtr openPlainPool(const std::shared_ptr<InMemoryBackend> & backend, PoolConfig config = {})
{
    config.pool_prefix = "p";
    config.server_root_id = "test";
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
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

/// The record source of one table this server writes, with no part on the disk: a round reads it and
/// runs as the writer, so it goes on to issue requests after its `keeper_records` phase.
class WriterWithNoRecords : public DB::ICasKeeperRecordSource
{
public:
    DB::CasKeeperRecordSnapshot snapshotCasRecords(const String &) override
    {
        DB::CasKeeperRecordSnapshot snapshot;
        snapshot.writer = true;
        return snapshot;
    }
    void markCasRecords(const std::vector<const DB::CasKeeperRecordState *> &) override {}
    DB::Strings removeCasRecords(const DB::CasKeeperRecordSnapshot &, const std::vector<const DB::CasKeeperRecordState *> &) override
    {
        return {};
    }
};

KeeperRecordSourcesFn writerSources()
{
    return [source = std::make_shared<WriterWithNoRecords>()] { return KeeperRecordSources{source}; };
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

/// The arm is idempotent, observable, and closes the door to new detached work; a drain after an
/// early arm has nothing to wait for.
TEST(CASGCTeardownStop, BeginTeardownIsIdempotentAndRefusesNewDetachedWork)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPlainPool(backend);

    EXPECT_FALSE(store->teardownBegun());
    store->beginTeardown();
    EXPECT_TRUE(store->teardownBegun());
    store->beginTeardown();
    EXPECT_TRUE(store->teardownBegun()) << "a second arm changes nothing";
    EXPECT_TRUE(store->detachedWorkStoppingForTest());

    EXPECT_FALSE(store->tryDispatchDetached([](DetachedStopToken) {}))
        << "no detached task is accepted once teardown began";
    EXPECT_TRUE(store->stopAndDrainDetachedWork(/*deadline_ms=*/1000))
        << "the drain after an early arm finds nothing in flight and returns at once";
}

/// The open plane -- GC, FSCK, the probe -- refuses after the arm, before anything reaches the
/// backend; the mount plane, which the ref-lane drain and the farewell need alive, does not.
TEST(CASGCTeardownStop, OpenPlaneRefusesAfterTeardownBeganAndTheMountPlaneDoesNot)
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
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)refused.read("p/probe", Retry::standard()); });
    CasOperation resumed = store->openRequests().resume(/*admitted_generation=*/0);
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { (void)resumed.read("p/probe", Retry::standard()); });
    EXPECT_EQ(backend->getTotal(), 0u) << "a refused admission never reaches the backend";

    CasOperation mount = store->mountRequests().admit();
    ASSERT_TRUE(mount.read("p/probe", Retry::once()).has_value())
        << "the mount plane is not the open plane: teardown's own drain and farewell run on it";
    EXPECT_EQ(backend->getTotal(), 1u);
}

/// `removeManyWriteOnce` -- the verb the CAS GC bulk-delete phases call, and the one an
/// `S3ObjectStorage`-backed pool ultimately dispatches to `removeObjectsIfExistUnderProfile` -- runs on
/// the same open plane as `read` above, so a control-plane bulk delete issued after the disk's shutdown
/// (which arms teardown on this plane before the object storage's own `shutdown()` even runs, see
/// `DiskObjectStorage::shutdown()`) is refused at admission and never reaches the backend at all.
TEST(CASGCTeardownStop, RemoveManyWriteOnceIsRefusedAfterTeardownBeganAndNeverReachesTheBackend)
{
    auto backend = std::make_shared<CountingBackend>();
    auto store = openPlainPool(backend);
    {
        CasOperation op = store->openRequests().admit();
        orThrow(op.create("p/probe", "v", Retry::once()), "create");
    }
    backend->resetCounts();

    store->beginTeardown();

    const Layout layout{"p"};
    const ManifestId manifest_id{RootNamespace{"probe/ns@cas@"},
        ManifestRef{.writer_epoch = 1, .build_sequence = 1, .manifest_ordinal = 1}};

    CasOperation refused = store->openRequests().admit();
    expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        refused.removeManyWriteOnce({layout.writeOnceManifestKey(manifest_id)}, Retry::standard());
    });
    EXPECT_EQ(backend->deleteTotal(), 0u) << "a refused admission never reaches the backend, not even for one key";
}

/// The open plane's sleep is the interruptible one, in production wiring and after the test seam is
/// cleared. Arming FIRST makes this a wiring test: a predicate `wait_for` whose predicate already
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

    /// Clearing the seam must put the interruptible sleep back, not the engine's plain one.
    store->setCasRetrySleepForTest([](uint64_t) {});
    store->setCasRetrySleepForTest({});
    auto paused_again = std::async(std::launch::async, [&store] { store->openRequests().pause(60'000); });
    EXPECT_EQ(paused_again.wait_for(std::chrono::seconds(10)), std::future_status::ready)
        << "resetting the retry-sleep seam left the open plane on the plain sleep";
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
    const auto source = std::make_shared<WriterWithNoRecords>();
    storage->registerKeeperRecordSource(source);

    auto parked = std::make_shared<Gate>();
    auto release = std::make_shared<Gate>();
    GateOpenedOnExit opener(*release);
    std::mutex rows_mutex;
    std::vector<GcRoundLogRecord> rows;
    storage->setGcRoundRowHookForTest([&](const GcRoundLogRecord & r)
    {
        {
            std::lock_guard lock(rows_mutex);
            rows.push_back(r);
        }
        /// Park on the `keeper_records` phase row: the round holds `gc_scheduler_mutex` and, as the
        /// writer, has requests ahead of it.
        if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "keeper_records")
        {
            parked->open();
            release->wait("release");
        }
    });

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
    std::atomic<bool> park{true};
    CasGcScheduler sched(store, std::chrono::seconds(1), "test::gc", "ca", writerSources(),
        [log = sink.logger(), &park, entered, release](const GcRoundLogRecord & r)
        {
            log(r);
            if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "keeper_records" && park.exchange(false))
            {
                entered->open();
                release->wait("release");
            }
        });

    sched.start();
    entered->wait("entered");

    store->beginTeardown();
    const uint64_t requests_at_arm = requestsTotal(*backend);
    release->open();

    const auto finish = sink.waitForFinish(/*from=*/0, std::chrono::seconds(30));
    ASSERT_TRUE(finish.has_value()) << "the parked round never finished";
    EXPECT_EQ(finish->outcome, GcRoundLogRecord::Outcome::Stopped);
    sched.stop();
    EXPECT_TRUE(sched.isQuiescent());
    EXPECT_EQ(requestsTotal(*backend), requests_at_arm)
        << "after the arm no request may reach the backend: the round unwinds at the next gate";
    EXPECT_EQ(countStarts(sink.all()), 1u) << "no further round started after the arm";
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
    std::atomic<bool> park{true};
    /// An hour-long interval: the loop ticks only when asked.
    CasGcScheduler sched(store, std::chrono::seconds(3600), "test::gc", "ca", writerSources(),
        [log = sink.logger(), &park, entered, release](const GcRoundLogRecord & r)
        {
            log(r);
            if (r.event_type == GcRoundLogRecord::EventType::Phase && r.phase == "keeper_records" && park.exchange(false))
            {
                entered->open();
                release->wait("release");
            }
        });
    sched.start();

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
    EXPECT_EQ(countStarts(rows), 1u) << "the queued tick minted a Start row after teardown began";
}
