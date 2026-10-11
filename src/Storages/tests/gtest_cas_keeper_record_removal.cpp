#include <Storages/MergeTree/CasKeeperPartRecord.h>
#include <Core/ServerUUID.h>
#include <Common/ZooKeeper/TestKeeper.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperArgs.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// A table with replicas `writer` and `reader` and the record of part `all_1_1_0`, marked once by the GC.
struct CASKeeperRecordRemoval : public ::testing::Test
{
    const String table = "/t";
    const String writer = "/t/replicas/writer";
    const Strings replicas{"reader", "writer"};
    const String part = "all_1_1_0";
    const String record = "/t/cas_parts/all_1_1_0";
    zkutil::ZooKeeperPtr zk;
    Coordination::Stat is_active;
    Coordination::Stat host;
    Coordination::Stat marked;

    void SetUp() override
    {
        ServerUUID::setRandomForUnitTests();
        zk = zkutil::ZooKeeper::createFromImpl([] { return std::make_unique<Coordination::TestKeeper>(zkutil::ZooKeeperArgs{}); });
        for (const auto * path : {"/t", "/t/cas_parts", "/t/replicas", "/t/replicas/writer", "/t/replicas/writer/parts",
                                  "/t/replicas/reader", "/t/replicas/reader/parts"})
            zk->create(path, "", zkutil::CreateMode::Persistent);
        zk->create(writer + "/is_active", "", zkutil::CreateMode::Ephemeral);
        zk->create(writer + "/host", "h", zkutil::CreateMode::Persistent);
        zk->set(writer + "/host", "h");   /// an activation sets it
        zk->create(record, "bytes", zkutil::CreateMode::Persistent);
        zk->set(record, "bytes");          /// the GC's mark
        zk->exists(writer + "/is_active", &is_active);
        zk->exists(writer + "/host", &host);
        zk->exists(record, &marked);
    }

    Coordination::Error remove()
    {
        Coordination::Responses responses;
        return zk->tryMulti(
            casRecordRemovalOps(*zk, table, writer, replicas, part, marked.version, is_active.version, host.version), responses);
    }
};

}

TEST_F(CASKeeperRecordRemoval, RemovesARecordNoReplicaLists)
{
    ASSERT_EQ(marked.version, 1);
    EXPECT_EQ(remove(), Coordination::Error::ZOK);
    EXPECT_FALSE(zk->exists(record));
}

TEST_F(CASKeeperRecordRemoval, KeepsTheRecordOfAPartAReplicaLists)
{
    /// A reader still lists it (a query holds it, it attached it late, a clone fetched it).
    zk->create("/t/replicas/reader/parts/" + part, "", zkutil::CreateMode::Persistent);
    EXPECT_NE(remove(), Coordination::Error::ZOK);
    EXPECT_TRUE(zk->exists(record));
}

TEST_F(CASKeeperRecordRemoval, KeepsARecordMarkedAgain)
{
    zk->set(record, "bytes");   /// listed and unlisted again since the round read it: its retention restarted
    EXPECT_EQ(remove(), Coordination::Error::ZBADVERSION);
    EXPECT_TRUE(zk->exists(record));
}

TEST_F(CASKeeperRecordRemoval, KeepsTheRecordWhenTheWriterWasReactivated)
{
    zk->set(writer + "/host", "h2");   /// another activation of the writer replica: a new epoch
    EXPECT_EQ(remove(), Coordination::Error::ZBADVERSION);
    EXPECT_TRUE(zk->exists(record));
}

TEST_F(CASKeeperRecordRemoval, KeepsTheRecordWhenTheWriterIsNotActive)
{
    zk->remove(writer + "/is_active");
    EXPECT_EQ(remove(), Coordination::Error::ZNONODE);
    EXPECT_TRUE(zk->exists(record));
}
