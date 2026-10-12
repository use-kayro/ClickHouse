#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasKeeperGc.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Logger.h>

#include <set>

using namespace DB;
using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// One table on the disk, held in memory as Keeper would hold its records: a mark bumps the record's
/// version, a removal erases it unless the test made the table refuse it (its part was listed again).
struct FakeTable : public DB::ICasKeeperRecordSource
{
    DB::CasKeeperRecordSnapshot snapshot;
    std::set<String> refuse_removal;

    DB::CasKeeperRecordSnapshot snapshotCasRecords(const DB::IContentAddressedExchange &) override { return snapshot; }

    DB::Strings markCasRecords(const std::vector<const DB::CasKeeperRecordState *> & records) override
    {
        DB::Strings marked;
        for (const auto * r : records)
        {
            record(r->part).version += 1;
            marked.push_back(r->part);
        }
        return marked;
    }

    DB::Strings removeCasRecords(const DB::CasKeeperRecordSnapshot &, const std::vector<const DB::CasKeeperRecordState *> & records) override
    {
        DB::Strings removed;
        for (const auto * r : records)
            if (!refuse_removal.contains(r->part))
            {
                std::erase_if(snapshot.records, [&](const auto & s) { return s.part == r->part; });
                removed.push_back(r->part);
            }
        return removed;
    }

    DB::CasKeeperRecordState & record(const String & part)
    {
        for (auto & r : snapshot.records)
            if (r.part == part)
                return r;
        throw std::logic_error("no record " + part);
    }

    bool has(const String & part) const
    {
        return std::ranges::any_of(snapshot.records, [&](const auto & r) { return r.part == part; });
    }
};

struct CASKeeperGc : public ::testing::Test
{
    std::shared_ptr<InMemoryBackend> backend = std::make_shared<InMemoryBackend>();
    PoolPtr store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .writer = true});
    std::shared_ptr<FakeTable> table = std::make_shared<FakeTable>();
    KeeperGc gc{store, [this] { return KeeperRecordTables{{table, table->snapshot}}; }, getLogger("CASKeeperGcTest")};

    void SetUp() override
    {
        table->snapshot.writer = true;
        table->snapshot.retention_ms = 0;   /// a record marked in one round is due the next
    }

    /// A record of part `part`, listed or not, referencing the blobs of `payloads` (each written to the pool).
    void addRecord(const String & part, bool listed, const std::vector<String> & payloads)
    {
        DB::CasKeeperRecordState r;
        r.part = part;
        r.listed = listed;
        for (const auto & p : payloads)
        {
            DB::Cas::tests::writeBlobRaw(*backend, store->layout(), p, store->poolMeta().blob_header_len, 0);
            r.blobs.push_back({p + ".bin", p.size(), u128Of(p)});
        }
        table->snapshot.records.push_back(std::move(r));
    }

    bool blobExists(const String & payload)
    {
        DB::Cas::tests::OperationForTest op(*backend);
        return (*op).head(store->layout().blobKey(idOf(payload)), Retry::standard()).has_value();
    }

    RoundReport round()
    {
        RoundReport report;
        gc.runRound(report);
        return report;
    }
};

}

TEST_F(CASKeeperGc, MarksAnUnlistedRecordAndWritesNothingToThePool)
{
    addRecord("all_1_1_0", /*listed=*/ false, {"only-dropped", "shared"});
    addRecord("all_2_2_0", /*listed=*/ true, {"shared"});
    const RoundReport report = round();
    EXPECT_TRUE(report.writer);
    EXPECT_EQ(table->record("all_1_1_0").version, 1);
    EXPECT_TRUE(blobExists("only-dropped"));
}

TEST_F(CASKeeperGc, RemovesTheRecordOnceDueThenDeletesWhatNoRecordReferences)
{
    addRecord("all_1_1_0", /*listed=*/ false, {"only-dropped", "shared"});
    addRecord("all_2_2_0", /*listed=*/ true, {"shared"});
    round();
    const RoundReport report = round();
    EXPECT_FALSE(table->has("all_1_1_0"));
    EXPECT_EQ(report.deleted, 1u);
    EXPECT_FALSE(blobExists("only-dropped"));
    EXPECT_TRUE(blobExists("shared"));
}

TEST_F(CASKeeperGc, KeepsTheBlobsOfARecordItsTableDidNotRemove)
{
    addRecord("all_1_1_0", /*listed=*/ false, {"a-only", "both"});
    addRecord("all_2_2_0", /*listed=*/ false, {"both"});
    table->refuse_removal.insert("all_2_2_0");   /// listed again between the round's read and its removal
    round();
    const RoundReport report = round();
    EXPECT_FALSE(table->has("all_1_1_0"));
    EXPECT_TRUE(table->has("all_2_2_0"));
    EXPECT_EQ(report.deleted, 1u);
    EXPECT_FALSE(blobExists("a-only"));
    EXPECT_TRUE(blobExists("both"));
}

/// The delete half of `BlobRefCount`'s invariant: a blob a part this server holds (or builds) references is
/// not deleted, though no record references it any more; it stays pending until released.
TEST_F(CASKeeperGc, KeepsABlobThisServerStillHoldsUntilReleased)
{
    addRecord("all_1_1_0", /*listed=*/ false, {"dropped"});
    store->blobRefs()->add({idOf("dropped")});
    round();
    round();
    EXPECT_FALSE(table->has("all_1_1_0"));
    EXPECT_TRUE(blobExists("dropped"));
    EXPECT_EQ(gc.pendingCount(), 1u);

    store->blobRefs()->release({idOf("dropped")});
    const RoundReport report = round();
    EXPECT_EQ(report.deleted, 1u);
    EXPECT_FALSE(blobExists("dropped"));
    EXPECT_EQ(gc.pendingCount(), 0u);
}

/// A pending candidate a record references again (a part with the same content committed meanwhile) leaves
/// the pipeline.
TEST_F(CASKeeperGc, SparesAPendingBlobARecordReferencesAgain)
{
    addRecord("all_1_1_0", /*listed=*/ false, {"dropped"});
    store->blobRefs()->add({idOf("dropped")});
    round();
    round();
    ASSERT_EQ(gc.pendingCount(), 1u);
    addRecord("all_3_3_0", /*listed=*/ true, {"dropped"});
    const RoundReport report = round();
    EXPECT_EQ(report.spared, 1u);
    EXPECT_EQ(gc.pendingCount(), 0u);
    EXPECT_TRUE(blobExists("dropped"));
}
