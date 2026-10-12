#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

ManifestRef testRef(uint64_t seq)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = 1};
}

}

/// `listNamespaces` projects the authoritative catalog; physical streams never contribute names.
TEST(CASGCShardIncarnation, ListNamespacesFromCatalog)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});

    const RootNamespace ns_a{"srv1/tblA"};

    EXPECT_TRUE(store->listNamespaces("").namespaces.empty());

    /// The real writer path admits the catalog row for namespace A.
    writeManifestRaw(*backend, store->layout(), ns_a, testRef(1), {});
    publishCommittedTransition(*backend, store->layout(), ns_a, "part_1", std::nullopt, testRef(1), /*shard=*/0);

    const auto nss = store->listNamespaces("").namespaces;
    ASSERT_EQ(nss.size(), 1u);
    EXPECT_EQ(nss[0], "srv1/tblA");

    /// Prefix filter: no match.
    EXPECT_TRUE(store->listNamespaces("srv2/").namespaces.empty());
    /// Prefix filter: match.
    const auto filtered = store->listNamespaces("srv1/").namespaces;
    ASSERT_EQ(filtered.size(), 1u);
    EXPECT_EQ(filtered[0], "srv1/tblA");
}
