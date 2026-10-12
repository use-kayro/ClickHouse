#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Core/Defines.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <unistd.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// `listNamespaceFiles` derives its LIST prefix from `layout.namespaceFilesPrefix(life)` -- a physical
/// life-scoped stream, not the catalog. Listing under a held life must cost exactly one LIST of the
/// files prefix and nothing else.
TEST(CASNamespaceFileReadContract, ListThroughHeldLifeIssuesZeroCatalogRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend);
    const NamespaceLifeId life = fixture::fixtureLife(RootNamespace{"00/ns_file_list_contract@cas@"});
    const String prefix = store->layout().namespaceFilesPrefix(life);

    store->putNamespaceFile(life, "a.txt", "a\n");
    store->putNamespaceFile(life, "b.txt", "b\n");
    backend->resetCounts();

    const std::vector<String> names = store->listNamespaceFiles(life);

    std::vector<String> sorted_names = names;
    std::sort(sorted_names.begin(), sorted_names.end());
    EXPECT_EQ(sorted_names, (std::vector<String>{"a.txt", "b.txt"}));

    /// Positive control: the journal recorded exactly one LIST against the namespace-file stream
    /// prefix and nothing else, so the touched-set assertion below names an absence, not a
    /// recorder that never saw anything.
    EXPECT_EQ(backend->listCount(prefix), 1u);
    EXPECT_EQ(backend->listTotal(), 1u);
    EXPECT_EQ(backend->touchedKeys(), std::vector<String>{prefix});
}
