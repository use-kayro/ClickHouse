#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include "cas_test_helpers.h"
#include <algorithm>

/// Namespace files are keyed by an opaque LIFE, not by its name: `cas/ns/state/<life_id>/_files/<name>`
/// (Stage B Task 4b, directive design change 2). This file pins the reader assignment
/// side of that re-key.
///
/// THE HOLE IT CLOSES. Before the re-key, a namespace file lived at a name-keyed prefix shared by every
/// life of that name. A file the store's LIST omitted therefore survived namespace removal -- nothing
/// enumerated it, so nothing deleted it -- and then became VISIBLE to the next namespace created under
/// the same name, because that namespace read the same prefix. Deletion was load-bearing for
/// correctness, and deletion depends on enumeration, which is the one thing an object store is allowed
/// to be late about (`HintHoleBackendOn` is that lateness as an interface -- see its doc).

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// The non-minting reader assignment site accepts exactly a catalog `Live` row. `Creating`,
/// `Removing`, and absence neither install a runtime life nor mutate durable catalog/stream state.
TEST(CASNsFileIncarnation, FreshReaderAssignsOnlyLiveCatalogLifeWithoutMutation)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace creating{"00/creating@cas@"};
    const RootNamespace live{"00/live@cas@"};
    const RootNamespace removing{"00/removing@cas@"};
    const RootNamespace absent{"00/absent@cas@"};
    CasRequests catalog_requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation catalog_op = catalog_requests.admit();

    CasRefCatalog::casAdmitEntry(catalog_op, layout, CatalogEntry{
        .ns = creating,
        .state = NsState::Creating,
        .incarnation = UInt128{31},
        .creator = CreatorFence{.server_root_id = "foreign", .writer_epoch = 7, .fence_generation = 1}});
    CasRefCatalog::casAdmitEntry(catalog_op, layout, CatalogEntry{
        .ns = live, .state = NsState::Live, .incarnation = UInt128{32}});
    CasRefCatalog::casAdmitEntry(catalog_op, layout, CatalogEntry{
        .ns = removing, .state = NsState::Live, .incarnation = UInt128{33}});
    CasRefCatalog::casUpdate(catalog_op, layout, [&](const RefCatalog & current)
    {
        RefCatalog next = current;
        const auto it = std::find_if(next.entries.begin(), next.entries.end(), [&](const CatalogEntry & entry)
        {
            return entry.ns == removing;
        });
        chassert(it != next.entries.end());
        it->state = NsState::Removing;
        it->removal_started_round = 1;
        return next;
    });

    /// Only a `Live` catalog row is readable. Give that exact life the empty checkpoint authority
    /// that production creation publishes; the other rows deliberately remain raw lifecycle states.
    writeRecoverableCkptForRawFixture(*backend, layout, live, RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    backend->resetCounts();
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(creating));
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(removing));
    EXPECT_FALSE(store->namespaceFilesLifeIfReadable(absent));
    const std::optional<NamespaceLifeId> readable = store->namespaceFilesLifeIfReadable(live);
    ASSERT_TRUE(readable);
    EXPECT_EQ(readable->incarnation, UInt128{32});

    EXPECT_FALSE(store->refTableLifeForTest(creating));
    EXPECT_FALSE(store->refTableLifeForTest(removing));
    EXPECT_FALSE(store->refTableLifeForTest(absent));
    ASSERT_TRUE(store->refTableLifeForTest(live));
    EXPECT_EQ(store->refTableLifeForTest(live)->incarnation, UInt128{32});
    EXPECT_EQ(backend->putTotal(), 0u);
    EXPECT_EQ(backend->putOverwriteTotal(), 0u);
}
