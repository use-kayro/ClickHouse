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

/// Recovery's checkpoint-base validator: the three physical triple failures below must each reject
/// exactly the checkpoint-named candidate.
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

        EXPECT_FALSE(validated_base.has_value());
    };

    expect_no_deletion_authority(RootNamespace{"00/cleanup-base-missing-backlink@cas@"}, std::nullopt);
    expect_no_deletion_authority(RootNamespace{"00/cleanup-base-wrong-backlink@cas@"}, RefTxnId{1, 99});
}
