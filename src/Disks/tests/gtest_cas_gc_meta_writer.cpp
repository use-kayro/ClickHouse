#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMetaWriter.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobMeta.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>

#include <Common/Logger.h>

#include <memory>
#include <variant>
#include <thread>
#include <utility>

using namespace DB::Cas;
using DB::Cas::tests::MetaWriteLatchBackend;
using DB::Cas::tests::awaitLatchEntered;
using DB::Cas::tests::openRequestsForTest;

namespace
{
/// A meta writer as the GC round owns one: pool size 16, the production default.
std::unique_ptr<GcMetaWriter> makeMetaWriter(const PoolPtr & store)
{
    return std::make_unique<GcMetaWriter>(store, getLogger("CasGcMetaWriterTest"), 16);
}

static_assert(noexcept(std::declval<GcMetaWriter &>().drainOnExitNoThrow()),
    "round-exit meta-pool cleanup must not throw from the scope guard");
}

/// A real condemn-marker job may be in flight when its `GcMetaWriter` is destroyed. The job holds everything it
/// touches, so the pool's join completes it correctly rather than racing member teardown -- and the
/// marker it was writing is durable afterwards.
///
/// This asserts function, not ordering: the release may land before, during or after destruction
/// begins, and all three are sound. Nothing here detects a job that wrongly captured its owner --
/// that is prevented by there being no API to write one.
TEST(CASGcMetaWriter, RealCondemnMarkerJobCompletesAcrossOwnerDestruction)
{
    auto backend = std::make_shared<MetaWriteLatchBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const BlobRef ref = DB::Cas::tests::idOf("1");
    const PersistedEtag token{"emulated", "tok-1"};

    auto writer = makeMetaWriter(store);
    backend->arm();
    writer->scheduleCondemnMarkerWrite(ref, token, /*condemn_round=*/1, /*size=*/128);

    awaitLatchEntered(*backend);

    std::thread releaser([&] { backend->release(); });
    writer.reset();
    releaser.join();

    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const auto meta = loadMeta(op, store->layout(), ref);
    ASSERT_TRUE(meta) << "the condemn marker was lost across owner destruction";
    EXPECT_EQ(meta->meta.state, MetaState::Condemned);
    EXPECT_EQ(meta->meta.condemn_round, 1u);
}

/// The confirmation registry is written by the pool thread and read by the graduation gate. Assert it
/// on a `GcMetaWriter` that is still alive, so the read is possible at all: after destruction there is no
/// registry left to consult, which is the documented behaviour a fresh leader relies on.
TEST(CASGcMetaWriter, CondemnMarkerConfirmationIsVisibleAfterDrain)
{
    auto backend = std::make_shared<MetaWriteLatchBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const BlobRef ref = DB::Cas::tests::idOf("1");
    const PersistedEtag token{"emulated", "tok-1"};

    auto writer = makeMetaWriter(store);
    EXPECT_FALSE(writer->condemnMarkerConfirmedInProcess(ref, token));

    writer->scheduleCondemnMarkerWrite(ref, token, /*condemn_round=*/1, /*size=*/128);
    writer->drain();

    EXPECT_TRUE(writer->condemnMarkerConfirmedInProcess(ref, token));
    EXPECT_EQ(writer->scheduled(), writer->completed());
}

/// Same lifetime property for the other production job. `deleteConfirmedMeta` RETURNS IMMEDIATELY when
/// no meta object exists (`Gc/CasGcMetaWriter.cpp`), so the meta must be seeded first -- otherwise the
/// job never reaches the latch and the wait above is waiting for something that will never happen.
TEST(CASGcMetaWriter, RealConfirmedMetaDeleteCompletesAcrossOwnerDestruction)
{
    auto backend = std::make_shared<MetaWriteLatchBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const BlobRef ref = DB::Cas::tests::idOf("2");
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    ASSERT_TRUE(std::holds_alternative<Committed>(putMetaIfAbsent(
        op, store->layout(), ref, BlobMeta{.state = MetaState::Condemned, .condemn_round = 1, .size = 64})));
    ASSERT_TRUE(loadMeta(op, store->layout(), ref));

    auto writer = makeMetaWriter(store);
    backend->arm();
    writer->scheduleConfirmedMetaDelete(ref);

    awaitLatchEntered(*backend);

    std::thread releaser([&] { backend->release(); });
    writer.reset();
    releaser.join();

    EXPECT_FALSE(loadMeta(op, store->layout(), ref))
        << "the confirmed-meta delete was lost across owner destruction";
}
