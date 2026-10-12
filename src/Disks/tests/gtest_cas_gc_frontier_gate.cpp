#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include "cas_test_helpers.h"
#include <optional>
#include <vector>

using namespace DB::Cas;
using namespace DB::Cas::tests;

/// ===================== CLEANUP RANGES ARE COMPUTED, NOT ENUMERATED =====================
///
/// `planRefCleanup` is pure, so the boundary arithmetic is pinned directly rather than inferred from a
/// round's side effects. Its sole coverage authority is the checkpoint-named base; a listed snapshot
/// is merely a physical observation until the same-id triple has been validated.

TEST(CASGCFrontierGateCleanupRange, CoveredLogsStopAtTheMinimumOfCheckpointAndCursor)
{
    RefTableListing listing;
    listing.logs = {{1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}};
    listing.snapshots = {{1, 5}};

    /// No checkpoint means no recovery base at all. A snapshot PUT that has not reached the `_ckpt`
    /// CAS must retain every listed object.
    const RefCleanupPlan without = planRefCleanup(listing, RefTxnId{1, 4});
    EXPECT_TRUE(without.deletable_logs.empty());
    EXPECT_TRUE(without.deletable_snapshots.empty());

    /// A checkpoint BELOW the cursor tightens it to {1,2}. Its exact `_log` witness must survive,
    /// so cleanup may remove only the strictly older entry.
    const RefCleanupPlan with = planRefCleanup(listing, RefTxnId{1, 4}, RefTxnId{1, 2});
    EXPECT_EQ(with.deletable_logs, (std::vector<RefTxnId>{{1, 1}}))
        << "the checkpoint witness and everything above it must survive";

    /// Once validation has established a later checkpoint base, its earlier covered history is
    /// reclaimable even if the hot fold cursor has not yet reached that base.
    const RefCleanupPlan ahead = planRefCleanup(listing, RefTxnId{1, 4}, RefTxnId{1, 9});
    EXPECT_EQ(ahead.deletable_logs, (std::vector<RefTxnId>{{1, 1}, {1, 2}, {1, 3}, {1, 4}}));
    EXPECT_EQ(ahead.deletable_snapshots, (std::vector<RefTxnId>{{1, 5}}));
}

TEST(CASGCFrontierGateCleanupRange, ASnapshotAtTheCheckpointSurvivesAndOnlyStrictlyOlderOnesGo)
{
    RefTableListing listing;
    listing.logs = {{1, 1}, {1, 2}, {1, 3}};
    listing.snapshots = {{1, 1}, {1, 2}, {1, 3}};

    /// A LIST-only newest snapshot is never a cleanup boundary.
    const RefCleanupPlan without = planRefCleanup(listing, RefTxnId{1, 3});
    EXPECT_TRUE(without.deletable_snapshots.empty());

    /// With the checkpoint AT {1,2}, only {1,1} is strictly below it. The snapshot the checkpoint names
    /// is the one a recovering reader samples, so it must survive its own cleanup.
    const RefCleanupPlan with = planRefCleanup(listing, RefTxnId{1, 3}, RefTxnId{1, 2});
    EXPECT_EQ(with.deletable_snapshots, (std::vector<RefTxnId>{{1, 1}}));

    /// The oldest checkpoint deletes nothing at all.
    const RefCleanupPlan oldest = planRefCleanup(listing, RefTxnId{1, 3}, RefTxnId{1, 1});
    EXPECT_TRUE(oldest.deletable_snapshots.empty());
}

/// Cleanup shares recovery's validator rather than inferring its own authority from a LIST. The
/// missing-base case is the no-checkpoint range above; the three physical triple failures below must
/// each reject exactly the checkpoint-named candidate.
TEST(CASGCFrontierGateCleanupRange, CheckpointBaseValidatorRejectsMissingLogSnapshotAndSeal)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    const RefTxnId base{1, 1};
    const RefCkpt checkpoint{
        .life_epoch = 1,
        .committed_through = base,
        .checkpoint_snapshot_id = base,
        .last_epoch_seal = std::nullopt};
    CasRefCatalog::initializeEmptyForNewPool(op, layout);

    {
        const RootNamespace ns{"00/cleanup-missing-base-log@cas@"};
        fixture::admitLive(*backend, layout, ns);
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(op, layout, ns).value();
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base));
        EXPECT_THROW((void)readCheckpointSnapshotBase(op, layout, life, checkpoint), DB::Exception);
    }
    {
        const RootNamespace ns{"00/cleanup-missing-base-snapshot@cas@"};
        fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(), .txn_id = base, .ops = {namespaceBirthOp()}, .prev_epoch_seal = std::nullopt});
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(op, layout, ns).value();
        EXPECT_THROW((void)readCheckpointSnapshotBase(op, layout, life, checkpoint), DB::Exception);
    }
    {
        const RootNamespace ns{"00/cleanup-seal-is-not-base@cas@"};
        writeSealAt(*backend, layout, ns, base);
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(op, layout, ns).value();
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base));
        EXPECT_THROW((void)readCheckpointSnapshotBase(op, layout, life, checkpoint), DB::Exception);
    }
}

TEST(CASGCFrontierGateCleanupRange, LaterEpochBaseWithoutItsContextualBacklinkCannotLicenseDeletion)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout{"p"};
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    const RefTxnId seal_id{1, 2};
    const RefTxnId base_id{2, 1};

    const auto expect_no_deletion_authority = [&](const RootNamespace & ns, std::optional<RefTxnId> backlink)
    {
        fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(), .txn_id = RefTxnId{1, 1}, .ops = {namespaceBirthOp()},
            .prev_epoch_seal = std::nullopt});
        writeSealAt(*backend, layout, ns, seal_id);
        fixture::writeRefLogRaw(*backend, layout, RefLogTxn{
            .ns = ns.string(), .txn_id = base_id, .ops = {}, .prev_epoch_seal = backlink});
        writeRefSnapshotRaw(*backend, layout, minimalLiveSnapshot(ns.string(), base_id));
        const NamespaceLifeId life = CasRefCatalog::lifeIfCataloged(op, layout, ns).value();

        std::optional<RefTxnId> validated_base;
        try
        {
            (void)readCheckpointSnapshotBase(op, layout, life, RefCkpt{
                .life_epoch = 1,
                .committed_through = base_id,
                .checkpoint_snapshot_id = base_id,
                .last_epoch_seal = seal_id});
            validated_base = base_id;
        }
        catch (const DB::Exception &) // NOLINT(bugprone-empty-catch): failure to validate is the tested case -- `validated_base` deliberately stays nullopt
        {
        }

        RefTableListing listing;
        listing.logs = {{1, 1}, seal_id, base_id};
        listing.snapshots = {{1, 1}, base_id};
        const RefCleanupPlan plan = planRefCleanup(listing, base_id, validated_base);
        EXPECT_TRUE(plan.deletable_logs.empty());
        EXPECT_TRUE(plan.deletable_snapshots.empty());
    };

    expect_no_deletion_authority(RootNamespace{"00/cleanup-base-missing-backlink@cas@"}, std::nullopt);
    expect_no_deletion_authority(RootNamespace{"00/cleanup-base-wrong-backlink@cas@"}, RefTxnId{1, 99});
}
