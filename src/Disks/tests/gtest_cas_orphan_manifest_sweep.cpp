#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCkpt.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Common/Exception.h>
#include "cas_test_helpers.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{
constexpr uint64_t kWriterEpoch = 7;
const String kServerRoot = "00";
ManifestRef ref(uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = kWriterEpoch, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}

bool headExists(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).head(key, Retry::standard()).has_value();
}

/// The §6 deletion premise (`manifestDeletionPremise`) is a SECOND precondition on every deletion below,
/// alongside the watermark eligibility these tests are about: a manifest of an epoch-`E` build is
/// deletable only once the namespace's sealed fold cursor sits in an epoch strictly above `E`. Tests
/// whose subject is the eligibility or ownership rule therefore have to establish it, or they would
/// assert a deletion the premise (not the rule under test) prevented. Tests whose subject is RETENTION
/// deliberately do NOT call this — see `CASSweepDeletionPremise` for the premise's own coverage.
void seedConsumedSealCursor(InMemoryBackend & backend, const Layout & layout, const RootNamespace & ns)
{
    seedFoldCursorForTest(backend, layout, ns, RefTxnId{kWriterEpoch + 1, 1});
}

/// The catalog cut and `_ckpt` are recovery's sole authority. A fixture that expects a catalog-named
/// life to be swept must establish the same empty, fully readable recovery state a real completed
/// creation would have, rather than relying on the retired sentinel fallback.
void seedEmptyRecoveryAuthority(InMemoryBackend & backend, const Layout & layout, const RootNamespace & ns)
{
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const CasRefCatalog::Snapshot catalog = CasRefCatalog::read(op, layout);
    const auto entry = std::find_if(catalog.catalog.entries.begin(), catalog.catalog.entries.end(),
        [&](const CatalogEntry & candidate) { return candidate.ns == ns; });
    ASSERT_NE(entry, catalog.catalog.entries.end());
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(entry->ns, entry->incarnation);
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = std::optional<uint64_t>{kWriterEpoch},
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt}), Retry::standard())));
}

/// Replaces the catalog row immediately before its second read after arming. The legacy orphan path
/// reads a catalog cut for coverage, then resolves the name again inside recovery; the second read can
/// splice a successor life into the old coverage decision. An authority-threaded path has no second
/// catalog read, so this seam must remain dormant.
class CatalogChangingOnSecondReadBackend : public InMemoryBackend
{
public:
    void arm(const Layout & layout, CatalogEntry predecessor_, CatalogEntry successor_)
    {
        catalog_key = layout.refCatalogKey();
        predecessor = std::move(predecessor_);
        successor = std::move(successor_);
        catalog_reads = 0;
        armed = true;
    }

    bool didSwitch() const { return did_switch; }

    std::optional<Raw> read(const String & key, TransportAccess & access) override
    {
        if (armed && key == catalog_key && ++catalog_reads == 2)
        {
            const auto current = InMemoryBackend::read(key, access);
            if (!current)
                throw std::runtime_error("test catalog disappeared");
            RefCatalog next = decodeRefCatalog(current->bytes);
            const auto it = std::find(next.entries.begin(), next.entries.end(), predecessor);
            if (it == next.entries.end())
                throw std::runtime_error("test predecessor catalog row disappeared");
            *it = successor;
            if (!InMemoryBackend::write(key, encodeRefCatalog(next), current->value, access).has_value())
                throw std::runtime_error("test catalog replacement conflicted");
            did_switch = true;
        }
        return InMemoryBackend::read(key, access);
    }

private:
    String catalog_key;
    CatalogEntry predecessor;
    CatalogEntry successor;
    uint64_t catalog_reads = 0;
    bool armed = false;
    bool did_switch = false;
};

}

/// A staged-but-unowned body in an ELIGIBLE prefix, absent from the owner view, is deleted (#7).
TEST(CASOrphanManifestSweep, EligibleAndUnownedIsDeleted)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    registerNamespaceRaw(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});   // body, no owner
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, /*min_active_build_sequence*/6);   // 6 > 5 => eligible
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));
    EXPECT_FALSE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})));
}

/// The orphan sweep must not turn a forged same-id snapshot at an OLDER `EpochSeal` into an empty owner
/// view. The base differs from `last_epoch_seal`, so metadata equality cannot catch it; the candidate
/// remains retained until the checkpoint is repaired.
TEST(CASOrphanManifestSweep, CheckpointSnapshotAtOlderEpochSealSkipsDeletion)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();
    const RootNamespace ns{"00/sweep-checkpoint-base-seal@cas@"};
    fixture::admitLive(*backend, layout, ns);
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, layout, ns);

    const RefLogTxn birth{
        .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, birth);
    RefOp seal;
    seal.kind = RefOpKind::EpochSeal;
    const RefLogTxn seal_txn{
        .ns = ns.string(), .txn_id = RefTxnId{1, 2}, .ops = {seal},
        .prev_epoch_seal = std::nullopt};
    fixture::writeRefLogRaw(*backend, layout, seal_txn);
    RefOp later_seal;
    later_seal.kind = RefOpKind::EpochSeal;
    fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
        .ns = ns.string(), .txn_id = RefTxnId{2, 1}, .ops = {later_seal},
        .prev_epoch_seal = RefTxnId{1, 2}});
    RefTableState through_seal;
    applyRefLogTxn(through_seal, birth);
    applyRefLogTxn(through_seal, seal_txn);
    writeRefSnapshotRaw(*backend, layout, snapshotOf(through_seal, ns.string()));
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(layout.refCkptKey(life), encodeRefCkpt(RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{2, 1},
        .checkpoint_snapshot_id = RefTxnId{1, 2},
        .last_epoch_seal = RefTxnId{2, 1}}), Retry::standard())));

    const ManifestRef candidate = ref(5, 0xAC);
    const String candidate_key = layout.manifestKey(ManifestId{ns, candidate});
    writeManifestRaw(*backend, layout, ns, candidate, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, layout, kServerRoot, kWriterEpoch, /*min_active_build_sequence=*/6);
    seedConsumedSealCursor(*backend, layout, ns);

    std::vector<String> warnings;
    EXPECT_EQ(sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5},
                             listServerRoots(*store), &warnings), 0u);
    EXPECT_TRUE(headExists(*backend, candidate_key));
    ASSERT_FALSE(warnings.empty());
}

/// A body that IS in the owner view (committed) is NEVER swept (#8).
TEST(CASOrphanManifestSweep, OwnedBodyIsSkipped)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);  // now owned
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})));
}

/// GC-WEDGE regression (2026-07-10): a COMMITTED ref that has been DROPPED but whose removal `-1` is NOT
/// yet sealed (transition_version above the sealed fold cursor, which is 0 for this fresh pool) must
/// SURVIVE the sweep — the GC fold still needs the body to emit the `-1` (delete-after-sealed-decrements).
/// A promoted build retires its build_seq, so the prefix is watermark-eligible; before the fix the sweep
/// deleted the body in the dropRef→fold window → the removal-fold then clamped FOREVER on the missing
/// committed body → pool-wide GC stop. The pending-removal protection now covers COMMITTED (not only
/// PRECOMMIT) removals.
///
/// SINCE THE §6 PREMISE, this shape is held by TWO independent facts: the tail-removal protection this
/// test is named for, and the premise's rule (1) — the fixture seals no fold cursor, so epoch
/// `kWriterEpoch`'s closing seal is not consumed either. They cannot be separated HERE: the removal log
/// sits in a lower epoch than the build, so any cursor high enough to satisfy rule (1) would also sit
/// above the log and stop the tail scan from reading it at all. The case where the tail-removal
/// protection is the ONLY thing standing — a removal in a LATER epoch, which is the direction removals
/// actually cross — is `CASSweepDeletionPremise.AnUnconsumedTailRemovalRetainsItsTarget`.
TEST(CASOrphanManifestSweep, PendingCommittedRemovalBodyIsSkipped)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, r);   // committed owner
    dropRefTransition(*backend, store->layout(), ns, "tbl", r);   // dropped: pending committed removal, -1 unsealed
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);   // 6 > 5 => prefix eligible

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})))
        << "a dropped-but-unsealed committed manifest body must survive the sweep (delete-after-sealed-"
           "decrements) — else the removal-fold clamps forever on the missing body (GC-WEDGE-2026-07-10)";
}

/// The sweep emits NO blob deltas: the in-degree generation is unchanged.
TEST(CASOrphanManifestSweep, EmitsNoBlobDeltas)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);
    seedEmptyRecoveryAuthority(*backend, store->layout(), ns);
    // The sweep must not advance the in-degree generation: capture it AFTER the fixture's own seal.
    const uint64_t gen_before = currentGenerationOf(*backend, store->layout());

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));
    EXPECT_EQ(currentGenerationOf(*backend, store->layout()), gen_before);
    EXPECT_EQ(inDegreeOf(*backend, store->layout(), DB::UInt128(1)), 0);
}

/// A NON-eligible prefix (no watermark fact) deletes NOTHING (#9: frozen-seq is not authority).
TEST(CASOrphanManifestSweep, NoWatermarkIsNotAuthority)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/aa@cas@"};
    const ManifestRef r = ref(5, 0xAB);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    // No setWatermarkMinActive — no durable fact => not eligible.
    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})));
}

/// A catalog-named life cannot be treated as an empty table merely because its mandatory recovery
/// checkpoint is missing. The orphan sweep is destructive, so it must retain the body until the
/// caller can recover from the same frozen catalog row and its exact `_ckpt`.
TEST(CASOrphanManifestSweep, MissingRequiredCheckpointSuppressesDestructiveDecision)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/authority-required@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    const ManifestRef r = ref(5, 0xB0);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);

    CasOperation op = store->openRequests().admit();
    const CatalogEntry entry = CasRefCatalog::read(op, store->layout()).catalog.entries.front();
    ASSERT_FALSE(readCkpt(op, store->layout(), NamespaceLifeId::fromCatalogEntry(entry.ns, entry.incarnation)));

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));

    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})))
        << "without the exact _ckpt required by a Live catalog row, the sweep must retain rather than "
           "derive an empty owner set";
}

/// The catalog row used to obtain coverage and the life used to recover ownership must be ONE frozen
/// authority cut. A later catalog row for the same name may not make the old life's committed manifest
/// look orphaned and therefore deletable.
TEST(CASOrphanManifestSweep, LaterCatalogCutCannotSpliceOwnershipAuthority)
{
    auto backend = std::make_shared<CatalogChangingOnSecondReadBackend>();
    auto store = openPoolForTest(backend);
    const RootNamespace ns{"00/frozen-catalog-cut@cas@"};
    fixture::admitLive(*backend, store->layout(), ns);
    CasOperation op = store->openRequests().admit();
    const CatalogEntry predecessor = CasRefCatalog::read(op, store->layout()).catalog.entries.front();

    const ManifestRef r = ref(5, 0xB1);
    writeManifestRaw(*backend, store->layout(), ns, r, {blobEntryFor("a", DB::UInt128(1))});
    ASSERT_EQ(publishCommittedTransition(*backend, store->layout(), ns, "live", std::nullopt, r), 1u);
    setWatermarkMinActive(*backend, store->layout(), kServerRoot, kWriterEpoch, 6);
    seedConsumedSealCursor(*backend, store->layout(), ns);

    CatalogEntry successor = predecessor;
    successor.incarnation = DB::UInt128(0xBEEF);
    backend->arm(store->layout(), predecessor, successor);

    sweepNamespace(*store, ns, BuildPrefix{.writer_epoch = kWriterEpoch, .build_sequence = 5}, listServerRoots(*store));

    EXPECT_FALSE(backend->didSwitch())
        << "the sweep must not resolve a second catalog cut after it starts using the frozen entry";
    EXPECT_TRUE(headExists(*backend, store->layout().manifestKey(ManifestId{ns, r})))
        << "the committed predecessor manifest must remain protected by the same frozen authority cut";
}

/// The LIST-based late-log detector that lived here is RETIRED with the sentinel seal, and it is worth
/// recording why rather than leaving a hole in this file's coverage story.
///
/// It existed because the old seal was a SNAPSHOT at a synthetic `{E-1, UINT64_MAX}` id: that object
/// occupied no `_log` key, so a dying predecessor's in-flight PUT could still land in the dead epoch and
/// the only possible response was to notice it afterwards and report it. INV-2's seal is a
/// TRANSACTION at exactly `{E, T+1}` -- the key that ghost would take -- so the store's own write-once
/// create refuses it. There is nothing left to detect at that shape: an id above the seal cannot be
/// minted either, because ids are state-derived and a writer that could derive `{E, T+2}` would have had
/// to observe the seal first.
///
/// `CasEventType::RefLateLogDetected` is retired WITH the detector -- pre-release, so a vocabulary entry
/// nothing can emit is just dead surface. Soak scenario S38 (`s38_late_put_injection.py`) keeps its
/// injection and FLIPS its assertion: from "the detection fired" to "the fence held" -- the late PUT's
/// conditional create must LOSE to the occupied slot, with zero data loss and the namespace folding
/// normally.
