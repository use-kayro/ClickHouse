#include <gtest/gtest.h>

#include <optional>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"
#include "config.h"

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{
ManifestRef ref(const String &, uint64_t seq, uint64_t inst)
{
    return ManifestRef{.writer_epoch = 1, .build_sequence = seq, .manifest_ordinal = static_cast<uint32_t>(inst)};
}
/// A one-shot `create`, asserting it committed (mirrors the retired `backend.putIfAbsent(key, bytes)`).
void createObj(Backend & backend, const String & key, const String & bytes)
{
    OperationForTest op(backend);
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).create(key, bytes, Retry::once())));
}

/// An exact read (mirrors the retired `backend.get(key)`).
std::optional<Object> readObj(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

class CkptReplacementConflictBackend : public InMemoryBackend
{
public:
    std::expected<String, RawConflict> write(
        const String & key, const String & bytes, const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        /// Only the CONDITIONAL shape is refused: the fixture's own creation of the object must land.
        if (conflict_once && expected_value && key == watched_key)
        {
            conflict_once = false;
            return std::unexpected(RawConflict{});
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    String watched_key;
    bool conflict_once = false;
};
}

TEST(CASSemanticRefFixture, WrapperCreatesInitialRecoverableCheckpoint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/semantic-create@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAB);

    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest);
    const RefTxnId expected_id{manifest.writer_epoch, sequence};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto ckpt = readCkpt(op, store->layout(), life);

    ASSERT_TRUE(ckpt.has_value());
    EXPECT_EQ(ckpt->ckpt.life_epoch, 1);
    EXPECT_EQ(ckpt->ckpt.committed_through, expected_id);
    EXPECT_FALSE(ckpt->ckpt.checkpoint_snapshot_id.has_value());
    EXPECT_FALSE(ckpt->ckpt.last_epoch_seal.has_value());
}

TEST(CASSemanticRefFixture, WrapperAdvancesCheckpointWithoutDiscardingSnapshot)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/semantic-advance@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAC);

    const uint64_t publish_sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest);
    const RefTxnId publish_id{manifest.writer_epoch, publish_sequence};
    writeRefSnapshotRaw(*backend, store->layout(), minimalLiveSnapshot(ns.string(), publish_id, {committedRow("tbl", manifest)}));
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto before_drop = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(before_drop.has_value());
    RefCkpt with_snapshot = before_drop->ckpt;
    with_snapshot.checkpoint_snapshot_id = publish_id;
    ASSERT_TRUE(std::holds_alternative<Committed>(op.replace(
        store->layout().refCkptKey(life), encodeRefCkpt(with_snapshot), before_drop->etag,
        Retry::once())));

    const uint64_t drop_sequence = dropRefTransition(*backend, store->layout(), ns, "tbl", manifest);
    const RefTxnId drop_id{manifest.writer_epoch, drop_sequence};
    const auto ckpt = readCkpt(op, store->layout(), life);

    ASSERT_TRUE(ckpt.has_value());
    EXPECT_EQ(ckpt->ckpt.committed_through, drop_id);
    EXPECT_EQ(ckpt->ckpt.checkpoint_snapshot_id, publish_id);
    EXPECT_FALSE(ckpt->ckpt.last_epoch_seal.has_value());
}

TEST(CASSemanticRefFixture, CheckpointAdvanceRejectsNonMonotoneAndInvalidState)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/semantic-refusal@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAD);

    const uint64_t sequence = publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest);
    const RefTxnId id{manifest.writer_epoch, sequence};
    EXPECT_THROW(advanceRecoverableCkptForRawFixture(*backend, store->layout(), ns, id), DB::Exception);

    const RootNamespace invalid_ns{"00/semantic-invalid@cas@"};
    fixture::admitLive(*backend, store->layout(), invalid_ns);
    const NamespaceLifeId invalid_life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), invalid_ns);
    const String invalid_key = store->layout().refCkptKey(invalid_life);
    createObj(*backend, invalid_key, "not a checkpoint");
    EXPECT_THROW(advanceRecoverableCkptForRawFixture(*backend, store->layout(), invalid_ns, id), DB::Exception);
    EXPECT_EQ(readObj(*backend, invalid_key)->bytes, "not a checkpoint");
}

TEST(CASRawRefFixture, RawLogWriteDoesNotCreateCheckpoint)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/raw-no-ckpt@cas@"};
    const RefTxnId id{1, 1};

    fixture::writeRefLogRaw(*backend, store->layout(), RefLogTxn{
        .ns = ns.string(),
        .txn_id = id,
        .ops = {namespaceBirthOp()},
        .prev_epoch_seal = std::nullopt,
    });

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    EXPECT_FALSE(readCkpt(op, store->layout(), life).has_value());
}

TEST(CASRawRefFixture, ReplaceRecoverableCheckpointWritesTheSuppliedFullState)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/replace-ckpt@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAE);
    const RefTxnId first_id{manifest.writer_epoch,
        publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest)};
    const RefTxnId seal_id{first_id.writer_epoch, first_id.ref_sequence + 1};
    writeRefSnapshotRaw(*backend, store->layout(), minimalLiveSnapshot(ns.string(), first_id, {committedRow("tbl", manifest)}));
    writeSealAt(*backend, store->layout(), ns, seal_id);

    const RefCkpt next{
        .life_epoch = 1,
        .committed_through = seal_id,
        .checkpoint_snapshot_id = first_id,
        .last_epoch_seal = seal_id,
    };
    replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, next);

    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto replaced = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(replaced.has_value());
    EXPECT_EQ(replaced->ckpt.life_epoch, next.life_epoch);
    EXPECT_EQ(replaced->ckpt.committed_through, next.committed_through);
    EXPECT_EQ(replaced->ckpt.checkpoint_snapshot_id, next.checkpoint_snapshot_id);
    EXPECT_EQ(replaced->ckpt.last_epoch_seal, next.last_epoch_seal);
}

TEST(CASRawRefFixture, ReplaceRecoverableCheckpointRejectsStaleRegressiveAndWrongLife)
{
    auto backend = std::make_shared<CkptReplacementConflictBackend>();
    auto store = openPoolForTest(backend);
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const RootNamespace ns{"00/replace-ckpt-refusal@cas@"};
    const ManifestRef manifest = ref("srv-a:1", 1, 0xAF);
    const RefTxnId id{manifest.writer_epoch,
        publishCommittedTransition(*backend, store->layout(), ns, "tbl", std::nullopt, manifest)};
    const NamespaceLifeId life = *CasRefCatalog::lifeIfCataloged(op, store->layout(), ns);
    const auto existing = readCkpt(op, store->layout(), life);
    ASSERT_TRUE(existing.has_value());

    RefCkpt wrong_life = existing->ckpt;
    wrong_life.life_epoch = *wrong_life.life_epoch + 1;
    wrong_life.committed_through = RefTxnId{*wrong_life.life_epoch, 1};
    EXPECT_THROW(replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, wrong_life), DB::Exception);

    RefCkpt regressive = existing->ckpt;
    regressive.committed_through = std::nullopt;
    EXPECT_THROW(replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, regressive), DB::Exception);

    backend->watched_key = store->layout().refCkptKey(life);
    backend->conflict_once = true;
    EXPECT_THROW(replaceRecoverableCkptForRawFixture(*backend, store->layout(), ns, existing->ckpt), DB::Exception);
    EXPECT_EQ(readCkpt(op, store->layout(), life)->ckpt.committed_through, id);
}
