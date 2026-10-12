#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

#include <vector>

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// Spec §6, the sweep deletion premise. A manifest of an epoch-`E` build is deletable only when the
/// namespace cursor has consumed epoch `E`'s seal AND no unconsumed tail record above the cursor names
/// it as a removal target; on ANY uncertainty the sweep RETAINS and says why.
///
/// WHY THE CURSOR AND NOT A LISTING. The sweep's pre-existing protection view is assembled from an
/// enumeration of the namespace's ref objects, and arithmetic ref intake demoted exactly that
/// enumeration to a hint: a store may omit a durable key from a `LIST`. A hidden `+1` above the cursor
/// therefore makes an owned manifest look unowned, and deleting it is data loss; a hidden `-1` makes a
/// removal target look unprotected, and deleting it clamps the fold forever on the missing body. The
/// premise closes the first by arithmetic (grants do not cross epochs, and an epoch is left only over
/// its consumed seal) and the second by refusing whenever the tail is not decidable.
namespace
{

/// The build's epoch. The namespace's seeded ref log lives at writer epoch 1 (`appendRefLogSeed`), so
/// naming the build's epoch 1 as well keeps the fixture coherent: a cursor at `{2, _}` is then a cursor
/// that genuinely crossed epoch 1's closing seal, not an invented number above an unrelated stream.
constexpr uint64_t kBuildEpoch = 1;
const String kServerRoot = "00";

ManifestRef ref(uint64_t seq, uint64_t ordinal)
{
    return ManifestRef{.writer_epoch = kBuildEpoch, .build_sequence = seq,
                       .manifest_ordinal = static_cast<uint32_t>(ordinal)};
}

BuildPrefix buildPrefix(uint64_t seq)
{
    return BuildPrefix{.writer_epoch = kBuildEpoch, .build_sequence = seq};
}

/// A pool with ONE eligible-but-unowned manifest body under build sequence 5: the shape the sweep is
/// meant to reclaim, so that every test below differs only in the durable fold state.
struct OrphanFixture
{
    std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
    PoolPtr store;
    RootNamespace ns{"00/aa@cas@"};
    ManifestRef orphan = ref(5, 0xAB);

    OrphanFixture()
    {
        store = openPoolForTest(backend);
        /// This fixture has no ref transaction, but it is a normal empty catalog life rather than the
        /// deliberate missing-checkpoint corruption shape. State that empty recovery frontier before
        /// exercising the independent sweep-deletion premise.
        casAdmitRecoverableEntry(*backend, store->layout(), ns);
        writeManifestRaw(*backend, store->layout(), ns, orphan, {blobEntryFor("a", DB::UInt128(1))});
        /// min_active_build_sequence 6 > build_sequence 5: the durable watermark fact makes the prefix ELIGIBLE, which
        /// is the half the premise sits on top of.
        setWatermarkMinActive(*backend, store->layout(), kServerRoot, kBuildEpoch, /*min_active_build_sequence*/6);
    }

    String orphanKey() const { return store->layout().manifestKey(ManifestId{ns, orphan}); }
    bool orphanExists() const
    {
        OperationForTest op(*backend);
        return (*op).head(orphanKey(), Retry::once()).has_value();
    }
};

}

/// Rule (1), the load-bearing case. The cursor is still INSIDE the build's own epoch, so epoch 1's
/// closing seal is not proven consumed and an unfolded `+1` naming this build may still exist above the
/// cursor. The body survives, and the sweep says so through its `warnings` out-param.
TEST(CASSweepDeletionPremise, AnUnconsumedEpochSealRetainsTheBuildsManifests)
{
    OrphanFixture f;
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch, 3});

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), listServerRoots(*f.store), &warnings);

    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(f.orphanExists())
        << "the cursor sits at {1,3}, inside the build's own epoch -- epoch 1's closing seal is not "
           "consumed, so a grant naming this build may still be unfolded above the cursor";
    ASSERT_EQ(warnings.size(), 1u) << "a retained manifest is a visible decision, not a silent one";
    EXPECT_NE(warnings[0].find(f.orphanKey()), String::npos);
    EXPECT_NE(warnings[0].find("seal"), String::npos);
}

/// Rule (1) satisfied and the tail clean: the ordinary reclaim still happens, by exact token.
TEST(CASSweepDeletionPremise, AConsumedEpochSealWithACleanTailDeletes)
{
    OrphanFixture f;
    /// A cursor at `{2, 1}` is in an epoch strictly above the build's. An epoch is left ONLY over its
    /// consumed `EpochSeal`, so this cursor is durable proof that every epoch-1 record is folded.
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch + 1, 1});

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), listServerRoots(*f.store), &warnings);

    EXPECT_EQ(deleted, 1u);
    EXPECT_FALSE(f.orphanExists());
    EXPECT_TRUE(warnings.empty()) << "nothing was retained, so nothing is warned about";
}

/// Uncertainty rule, hold arm. The cursor HAS consumed epoch 1's seal, so rule (1) alone would let the
/// body go -- but the namespace is held, which means the fold could not account for everything at or
/// above the held position. A held namespace retains everything under it.
TEST(CASSweepDeletionPremise, AHeldNamespaceRetainsEvenAboveAConsumedSeal)
{
    OrphanFixture f;
    const RefHold hold{.reason = HoldReason::GapBelowWitness,
                       .offending_position = RefTxnId{kBuildEpoch + 1, 4},
                       .retry_count = 2, .next_retry_round = 9};
    seedFoldCursorForTest(*f.backend, f.store->layout(), f.ns, RefTxnId{kBuildEpoch + 1, 3}, hold);

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), listServerRoots(*f.store), &warnings);

    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(f.orphanExists());
    ASSERT_EQ(warnings.size(), 1u);
    EXPECT_NE(warnings[0].find("held"), String::npos);
    EXPECT_NE(warnings[0].find(String{holdReasonToWord(HoldReason::GapBelowWitness)}), String::npos)
        << "the retain reason names WHAT stopped the namespace, not just that something did";
}

/// Uncertainty rule, unreached-frontier arm in its most complete form: the adopted seal carries no row
/// for this namespace at all, so no round has ever sealed a cursor for it and nothing about its ref
/// stream is proven. This is also the state of a pool whose GC has never run.
TEST(CASSweepDeletionPremise, ANamespaceWithNoSealedCursorRetains)
{
    OrphanFixture f;
    /// A seal exists and is adopted, but it covers a DIFFERENT namespace.
    seedFoldCursorForTest(*f.backend, f.store->layout(), RootNamespace{"00/zz@cas@"},
                          RefTxnId{kBuildEpoch + 1, 1});

    std::vector<String> warnings;
    const uint64_t deleted = sweepNamespace(*f.store, f.ns, buildPrefix(5), listServerRoots(*f.store), &warnings);

    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(f.orphanExists());
    ASSERT_EQ(warnings.size(), 1u);
    EXPECT_NE(warnings[0].find("coverage"), String::npos);
}

/// Rule (2). Removals cross epochs, so a record in a LATER epoch can name an earlier epoch's build as a
/// removal target; deleting the body before that `-1` folds clamps the fold forever on the missing body.
/// The predicate is exercised directly here because the sweep's own protection view already spares a
/// listed tail removal before the premise is ever consulted -- the point of the rule is that the SAME
/// answer is reached by the predicate both paths share, so neither path can lose it.
TEST(CASSweepDeletionPremise, AnUnconsumedTailRemovalRetainsItsTarget)
{
    OrphanFixture f;
    const String key = f.orphanKey();

    NamespaceFoldView view;
    RefCoverage cov;
    cov.classification = CoverageClass::Folded;
    cov.last_folded_ref_id = RefTxnId{kBuildEpoch + 1, 1};   /// rule (1) satisfied
    view.coverage = cov;
    view.tail_removal_targets.insert(key);

    String reason;
    EXPECT_FALSE(manifestDeletionPremise(view, ManifestKey{key, buildPrefix(5)}, &reason));
    EXPECT_NE(reason.find("removal"), String::npos);

    /// The same view without the removal target admits the deletion, so the retention above is the
    /// removal target's doing and nothing else's.
    view.tail_removal_targets.clear();
    reason.clear();
    EXPECT_TRUE(manifestDeletionPremise(view, ManifestKey{key, buildPrefix(5)}, &reason));
    EXPECT_TRUE(reason.empty());
}

/// WHAT THE PREMISE COSTS, pinned so it is a stated behaviour rather than something a later reader
/// discovers. The pure pre-precommit orphan -- a manifest body staged by a writer that crashed before
/// appending any ref record for it -- lives under a namespace whose ref stream may not exist at all.
/// Such a namespace never enters the fold's universe, so no round ever seals a cursor for it, so no
/// epoch's closing seal is ever consumed for it, so the premise retains its debris INDEFINITELY. That
/// is the safe direction and it is deliberate, but it is not "delay": reclaiming this class needs the
/// sweep's own rework (registers R2/R3, Stage B) -- the writer duty queue that knows what it staged, and
/// the nomination path. The premise ships as the safety floor, not as the reclaim policy.
TEST(CASSweepDeletionPremise, DebrisUnderANamespaceTheFoldNeverWalksIsRetainedIndefinitely)
{
    OrphanFixture f;
    /// No ref stream, no coverage row -- repeated passes change nothing.
    std::vector<String> warnings;
    for (int pass = 0; pass < 3; ++pass)
        EXPECT_EQ(sweepNamespace(*f.store, f.ns, buildPrefix(5), listServerRoots(*f.store), &warnings), 0u)
            << "pass " << pass;

    EXPECT_TRUE(f.orphanExists());
    EXPECT_EQ(warnings.size(), 3u) << "every pass reports the retention rather than going quiet";
}

/// STAGE B SEAM (registers R2/R3). The premise is the per-manifest SAFETY floor and nothing else: it
/// says when a body may go, never who nominates it or when. The sweep's own rework -- the writer duty
/// queue that reclaims its own live epoch's debris, and the nomination path -- attaches here, and must
/// satisfy this predicate rather than replace it.
