#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasKeyReader.h>
#include <Disks/tests/cas_test_helpers.h>

/// A reader hands a sequential walk its next object and lets the walk say which keys it will want
/// (hint) and which hinted keys it will never take (discard). The inline reader ignores hints.

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::openRequestsForTest;

namespace
{

struct Rig
{
    std::shared_ptr<CountingBackend> backend = std::make_shared<CountingBackend>();
    CasRequests requests = openRequestsForTest(backend);
    CasOperation op = requests.admit();

    void put(const String & key, const String & bytes)
    {
        ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, bytes, Retry::once()))) << key;
    }
};

}

TEST(CASGCKeyReader, InlineReaderHintsNothingAndReadsOnTake)
{
    Rig rig;
    rig.put("k1", "one");
    InlineKeyReader reader(rig.op);
    rig.backend->resetCounts();
    EXPECT_EQ(reader.window(), 0u);
    reader.hint("k1");
    EXPECT_EQ(rig.backend->getCount("k1"), 0u);
    EXPECT_EQ(reader.pending(), 0u);
    const auto got = reader.take("k1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes, "one");
    EXPECT_EQ(rig.backend->getCount("k1"), 1u);
    reader.discard("k1");
}
