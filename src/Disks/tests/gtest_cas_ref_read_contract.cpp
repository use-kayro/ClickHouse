#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <algorithm>
#include <memory>
#include <optional>

/// The ref-side read contract (the ten `CasRefCatalog::read` sites in `CasRefLedger.cpp` were
/// classified elsewhere: every site is a mutation/admission authority or a per-key destructive
/// revalidation, and every live table reader reaches `acquireReadableRefTableRuntime`, whose warm path
/// returns the resident runtime before any catalog read). These are COVERAGE PINS for a contract the
/// classification predicted already holds, not a fix: a held reader runtime answers stale-or-absent
/// across a same-name rebirth, a warm read costs no catalog request, and the one held ref-writer seam
/// that exists (`dropNamespace(const NamespaceLifeId &)`) refuses across the same rebirth rather than
/// touching the successor.

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// One committed ref, born and published through the REAL production write path (`beginPartWrite` /
/// `stageManifest` / `precommitAdd` / `promote`) -- this is what mints `ns`'s catalog life for real,
/// exactly as an ordinary insert would, rather than a fixture sentinel.
ManifestId publishRefThroughPool(const PoolPtr & store, const RootNamespace & ns, const String & ref_name)
{
    PartWriteInfo info;
    info.intended_namespace = ns;
    info.intended_ref = ns.string() + "/" + ref_name;
    auto build = store->beginPartWrite(info);
    const ManifestId id = build->stageManifest({});
    build->precommitAdd(ns, ref_name, id);
    build->promote(ns, ref_name, build->buildId(), id);
    return id;
}

}

/// The disjoint half of the read-side contract: once a table's runtime is resident, an ordinary read
/// costs no catalog request at all -- the recovered-and-cached `RefTableState` is this process's
/// sole authority for a table it has already opened.
TEST(CASRefReadContract, HotRefReadsThroughHeldRuntimeIssueZeroCatalogRequests)
{
    auto backend = std::make_shared<CountingBackend>();
    PoolPtr store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/ref_read_contract_hot@cas@"};
    const String ref_name = "part_1";

    const ManifestId published = publishRefThroughPool(store, ns, ref_name);
    const auto warm = store->resolveRef(ns, ref_name);
    ASSERT_TRUE(warm.has_value());
    EXPECT_EQ(warm->manifest_id, published);
    ASSERT_TRUE(store->refTableLifeForTest(ns).has_value());
    const NamespaceLifeId life = *store->refTableLifeForTest(ns);

    /// Positive control, captured BEFORE the reset below: the cold admission above really did reach
    /// the catalog and this namespace's own ref stream, so the upcoming zero is an absence and not a
    /// recorder that never saw anything.
    EXPECT_GT(
        backend->headCount(layout.refCatalogKey()) + backend->getCount(layout.refCatalogKey())
            + backend->putOverwriteCount(layout.refCatalogKey()),
        0u) << "the cold admission above must have reached the catalog at least once";
    EXPECT_GT(backend->getCount(layout.refCkptKey(life)), 0u)
        << "the cold recovery above must have read this namespace's own checkpoint at least once";

    backend->resetCounts();

    (void)store->resolveRef(ns, ref_name);
    (void)store->listRefs(ns);
    (void)store->hasAnyRefWithPrefix(ns, "");

    EXPECT_EQ(backend->headCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->getCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putOverwriteCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putCount(layout.refCatalogKey()), 0u);
    EXPECT_EQ(backend->putOverwriteCount(layout.refCatalogKey()), 0u);
    /// Stronger than the catalog-only clauses above: a warm ref read is a pure map lookup over the
    /// recovered state (`ensureRefTableRecovered`'s early return once `rt.recovered`), so it issues no
    /// backend request whatsoever, not merely none against the catalog.
    EXPECT_TRUE(backend->touchedKeys().empty())
        << "a warm ref read must issue no backend requests at all";
}

