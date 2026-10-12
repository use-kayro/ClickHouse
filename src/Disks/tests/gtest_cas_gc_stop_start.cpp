#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcScheduler.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unistd.h>
#include <vector>

/// `SYSTEM CAS GC STOP` / `GC START` -- granular operator control of ONLY the background GC scheduler. STOP
/// is STOP-IN-PLACE: it joins the worker thread and clears the in-process leadership hint, but RETAINS the
/// scheduler object so a later START restarts the SAME instance (its in-memory state preserved). The disk
/// stays fully usable (reads/writes unaffected) while GC is stopped.
///
/// These tests exercise the scheduler-level behavior directly (`CasGcScheduler::stop`/`start`) and the
/// end-to-end verbs through a real `ContentAddressedMetadataStorage`.

namespace DB::ErrorCodes
{
extern const int INVALID_STATE;
}

using namespace DB;
using DB::Cas::CasGcScheduler;
using DB::Cas::GcRoundLogRecord;
using DB::Cas::InMemoryBackend;
using DB::Cas::RoundReport;
using DB::Cas::tests::openPoolForTest;

namespace
{

/// A live table dir + committed part reused by the "reads/writes unaffected while stopped" test.
const std::string kTableDir = "gg0/gg0gg0g0-0808-4808-8808-080808080808";
const std::string kPartDir = kTableDir + "/all_1_1_0";
const std::string kPartFile = kPartDir + "/data.bin";

/// A real `ContentAddressedMetadataStorage` over a fresh, unique local object storage, writing blobs (the
/// gtest context's `replicated_can_become_leader` is the default, 1). GC is enabled by default
/// (`gc_interval_sec == 60`), so no background round fires during the sub-second test window.
std::shared_ptr<ContentAddressedMetadataStorage> openGcStorage()
{
    static std::atomic<uint64_t> counter{0};
    const auto scratch = std::filesystem::temp_directory_path()
        / ("ca_gc_stopstart_scratch_" + std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1)));
    auto settings = Cas::tests::makeSettingsForTest("test", scratch);
    auto storage = std::make_shared<ContentAddressedMetadataStorage>(
        Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", getContext().context, settings);
    storage->startup();
    return storage;
}

void commitOnePart(ContentAddressedMetadataStorage & storage)
{
    auto tx = storage.createTransaction();
    auto & ca_tx = dynamic_cast<ContentAddressedTransaction &>(*tx);
    auto buf = ca_tx.writeFile(kTableDir + "/tmp_insert_all_1_1_0/data.bin", 65536, WriteMode::Rewrite, {});
    const std::string bytes = "content-of-the-part";
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
    tx->moveDirectory(kTableDir + "/tmp_insert_all_1_1_0", kPartDir);
    tx->commit(NoCommitOptions{});
}

/// A thread-safe sink for the scheduler's per-round log records, with a condition variable so a test can
/// WAIT (never sleep) for a background round to land. `waitForSuccessFinish` blocks until a Finish record
/// with `outcome == Success` and `writer` (the round ran as the writer) appears at index >= `from`, or the
/// timeout trips (only on a genuine hang/regression -- the round is sub-millisecond on an in-memory pool).
class RoundLogSink
{
public:
    Cas::GcRoundLogger logger()
    {
        return [this](const GcRoundLogRecord & r)
        {
            std::lock_guard lock(mutex);
            records.push_back(r);
            cv.notify_all();
        };
    }

    /// Index one past the current end of the record log -- the "from" watermark for a subsequent wait.
    size_t mark()
    {
        std::lock_guard lock(mutex);
        return records.size();
    }

    /// The first Success Finish record at index >= `from`, waiting up to `timeout`. Returns nullopt on
    /// timeout so the caller asserts with a clear message rather than hanging.
    std::optional<GcRoundLogRecord> waitForSuccessFinish(size_t from, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        const bool ok = cv.wait_for(lock, timeout, [&]
        {
            for (size_t i = from; i < records.size(); ++i)
                if (records[i].event_type == GcRoundLogRecord::EventType::Finish
                    && records[i].outcome == GcRoundLogRecord::Outcome::Success && records[i].writer)
                    return true;
            return false;
        });
        if (!ok)
            return std::nullopt;
        for (size_t i = from; i < records.size(); ++i)
            if (records[i].event_type == GcRoundLogRecord::EventType::Finish
                && records[i].outcome == GcRoundLogRecord::Outcome::Success && records[i].writer)
                return records[i];
        return std::nullopt;
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<GcRoundLogRecord> records;
};

/// The record source of one table this server writes, with no part on the disk: a round reads it and
/// runs as the writer (`writer`), with nothing to mark or remove.
class WriterWithNoRecords : public DB::ICasKeeperRecordSource
{
public:
    DB::CasKeeperRecordSnapshot snapshotCasRecords(const DB::IContentAddressedExchange &) override
    {
        DB::CasKeeperRecordSnapshot snapshot;
        snapshot.writer = true;
        return snapshot;
    }
    DB::Strings markCasRecords(const std::vector<const DB::CasKeeperRecordState *> &) override { return {}; }
    Strings removeCasRecords(const DB::CasKeeperRecordSnapshot &, const std::vector<const DB::CasKeeperRecordState *> &) override
    {
        return {};
    }
};

Cas::KeeperRecordTablesFn writerSources()
{
    return [source = std::make_shared<WriterWithNoRecords>()]
    {
        DB::CasKeeperRecordSnapshot snapshot;
        snapshot.writer = true;
        return std::optional<Cas::KeeperRecordTables>(Cas::KeeperRecordTables{{source, snapshot}});
    };
}

/// A generous wait bound for a background round to land -- trips only on a real deadlock/regression.
constexpr std::chrono::milliseconds kRoundWait{60000};

}

/// (a + e) STOP joins the worker + heartbeat threads and clears the in-process leadership hint. The T10
/// lesson: make the assertion REAL -- acquire leadership via a manual round FIRST, so `is_leader` is
/// genuinely true before STOP for the clear to prove anything (otherwise `EXPECT_FALSE` would be vacuous).
TEST(CASGCStopStart, StopJoinsWorkersAndClearsLeadershipHint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    /// A long interval keeps any BACKGROUND round from firing; the manual round below is what leads.
    CasGcScheduler sched(store, std::chrono::seconds(3600), "CasGcStopStartTest", "ca-disk", writerSources());
    sched.start();

    /// Acquire REAL leadership: a manual round on a free lease acquires it.
    const RoundReport rep = sched.runOneRoundNow();
    ASSERT_TRUE(rep.writer) << "a manual round on a fresh pool must acquire the free GC lease";
    ASSERT_TRUE(sched.gcHealth().is_leader) << "leadership must be true BEFORE stop for the clear to prove anything";
    ASSERT_TRUE(sched.isQuiescent()) << "the manual round completed; nothing is in flight";

    sched.stop();   /// joins loop + heartbeat threads (the test completing without hanging proves the join)

    EXPECT_TRUE(sched.isQuiescent()) << "no GC round may be in flight after stop joined the workers";
    EXPECT_FALSE(sched.gcHealth().is_leader)
        << "stop must clear the in-process leadership hint (the disk no longer leads GC)";
}

/// (b) START after STOP restarts the SAME scheduler: background rounds resume, and `is_leader` (the last
/// round ran as the writer) becomes true again only after a restarted background round.
/// Deterministic and sleep-free: a condition variable fed by the round logger waits for each background
/// Finish. This also exercises `start()`'s post-join re-entrancy -- a bug there would hang the wait.
TEST(CASGCStopStart, StartAfterStopResumesBackgroundRounds)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);

    RoundLogSink sink;
    /// 1s interval: the background loop's first round fires ~1s after start(); the cv wait (not a sleep)
    /// synchronizes on the actual Finish record.
    CasGcScheduler sched(store, std::chrono::seconds(1), "CasGcStopStartTest", "ca-disk", writerSources(), sink.logger());

    /// First run: background rounds start and one acquires the lease.
    sched.start();
    const auto first = sink.waitForSuccessFinish(/*from=*/0, kRoundWait);
    ASSERT_TRUE(first.has_value()) << "the background scheduler must run a round and acquire the lease after start()";
    EXPECT_TRUE(sched.gcHealth().is_leader) << "leadership is held after the first background round";

    /// Stop: leadership hint cleared, threads joined.
    sched.stop();
    EXPECT_FALSE(sched.gcHealth().is_leader) << "stop clears the leadership hint";
    const size_t after_stop = sink.mark();

    /// Restart the SAME instance: a NEW background round must land.
    sched.start();
    const auto second = sink.waitForSuccessFinish(/*from=*/after_stop, kRoundWait);
    ASSERT_TRUE(second.has_value()) << "background rounds must resume after START (start() is re-enterable post-join)";
    EXPECT_TRUE(sched.gcHealth().is_leader)
        << "leadership is re-entered via the restarted round's normal lease acquisition";

    sched.stop();
}

/// (c) STOP and START are both idempotent: a second STOP on an already-stopped scheduler is a safe no-op,
/// and a second START on a running one is a no-op that leaves it running (a manual round still works).
TEST(CASGCStopStart, StopAndStartAreIdempotent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasGcScheduler sched(store, std::chrono::seconds(3600), "CasGcStopStartTest", "ca-disk", writerSources());

    sched.start();
    EXPECT_NO_THROW(sched.start()) << "a second START on a running scheduler is a no-op";

    sched.stop();
    EXPECT_NO_THROW(sched.stop()) << "a second STOP on a stopped scheduler is a safe no-op";
    EXPECT_TRUE(sched.isQuiescent());
    EXPECT_FALSE(sched.gcHealth().is_leader);

    /// After the double-stop, START still restarts the same instance and it runs a round.
    sched.start();
    const RoundReport rep = sched.runOneRoundNow();
    EXPECT_TRUE(rep.writer) << "the restarted scheduler still runs rounds after idempotent stop/start";
    sched.stop();
}

/// (f) The disk stays fully usable while its GC scheduler is stopped: a store()-path write + read succeed
/// after `gcStop`. STOP controls ONLY the GC pacer, not the disk's data plane.
TEST(CASGCStopStart, DiskReadsWritesUnaffectedWhileGcStopped)
{
    auto storage = openGcStorage();
    storage->gcStart();   /// create + start the scheduler
    storage->gcStop();    /// stop it in place (scheduler retained, threads joined)

    /// A write (commit a part) and a read (existsFile) both succeed with GC stopped.
    EXPECT_NO_THROW(commitOnePart(*storage));
    EXPECT_TRUE(storage->existsFile(kPartFile)) << "reads/writes must be unaffected while the GC scheduler is stopped";

    /// And START brings the scheduler back (idempotent, re-enterable) without disturbing the data.
    EXPECT_NO_THROW(storage->gcStart());
    EXPECT_TRUE(storage->existsFile(kPartFile));
    storage->gcStop();
}

/// (T11 M3, acceptance matrix) Two threads hammering `gcStop`/`gcStart` on the SAME storage concurrently.
/// The verbs serialize on `lifecycle_mutex` (then `gc_scheduler_mutex`, always in that order — so there is
/// no lock-order inversion and hence no deadlock), so each call is atomic: the barrage interleaves in any
/// order but never tears the retained scheduler pointer or its worker-thread set. We bound each worker with
/// a `std::future` timeout (never a sleep) so a deadlock regression fails FAST instead of hanging the suite,
/// and — since the final serialized call determines the resting state — a single quiet STOP then START at
/// the end lands the object in a well-defined, usable state (last call wins). ASan/TSan running this proves
/// the racing start()/stop() thread spawns+joins never race the shared members.
TEST(CASGCStopStart, ConcurrentStopStartFromTwoThreadsStaysConsistent)
{
    auto storage = openGcStorage();

    /// 200 iterations each, opposite phase, so the two threads spend the whole run contending on the
    /// lifecycle mutex with one about to START while the other is about to STOP.
    constexpr int kIters = 200;
    auto worker = [&](bool start_first)
    {
        for (int i = 0; i < kIters; ++i)
        {
            if (start_first) { storage->gcStart(); storage->gcStop(); }
            else             { storage->gcStop();  storage->gcStart(); }
        }
    };

    auto a = std::async(std::launch::async, worker, true);
    auto b = std::async(std::launch::async, worker, false);
    ASSERT_EQ(a.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "two-thread GC stop/start must not deadlock (both verbs lock lifecycle_mutex then gc_scheduler_mutex)";
    ASSERT_EQ(b.wait_for(std::chrono::seconds(60)), std::future_status::ready)
        << "two-thread GC stop/start must not deadlock";
    a.get();
    b.get();

    /// No torn state: a scheduler exists (both workers created/re-entered one) and its health snapshot is
    /// coherently queryable rather than reading a half-published pointer.
    ASSERT_TRUE(storage->gcHealth().has_value()) << "the scheduler must exist and report coherent health after the barrage";

    /// Last call wins: once contention ends, one serialized STOP lands it stopped (leadership cleared,
    /// quiescent), and one serialized START lands it running again — each observed deterministically.
    storage->gcStop();
    ASSERT_TRUE(storage->gcHealth().has_value());
    EXPECT_FALSE(storage->gcHealth()->is_leader) << "a final serialized STOP clears leadership -- last call wins";

    storage->gcStart();
    EXPECT_TRUE(storage->gcHealth().has_value()) << "a final serialized START leaves the scheduler present";

    /// The data plane is unharmed by the whole barrage: a write + read still succeed.
    EXPECT_NO_THROW(commitOnePart(*storage));
    EXPECT_TRUE(storage->existsFile(kPartFile));
    storage->gcStop();
}

