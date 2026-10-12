#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>

using DB::Cas::partFileMustStayBlob;

TEST(CASInlinePlacement, ColumnAndMarkFilesStayBlob)
{
    EXPECT_TRUE(partFileMustStayBlob("data.bin"));
    EXPECT_TRUE(partFileMustStayBlob("data.mrk"));
    EXPECT_TRUE(partFileMustStayBlob("data.mrk2"));
    EXPECT_TRUE(partFileMustStayBlob("data.mrk3"));
    EXPECT_TRUE(partFileMustStayBlob("data.cmrk"));
    EXPECT_TRUE(partFileMustStayBlob("data.cmrk2"));
    EXPECT_TRUE(partFileMustStayBlob("data.cmrk3"));
    EXPECT_TRUE(partFileMustStayBlob("primary.idx"));   // potentially large; stays blob (follow-up tuning)
}

TEST(CASInlinePlacement, StatisticsStayBlob)
{
    /// Statistics are the small-file class that grows large; inline they would dominate the Keeper part
    /// record (`cas_part_files`).
    EXPECT_TRUE(partFileMustStayBlob("statistics.packed"));
    EXPECT_TRUE(partFileMustStayBlob("statistics_value.stats"));
    EXPECT_TRUE(partFileMustStayBlob("p.proj/statistics.packed"));
    EXPECT_FALSE(partFileMustStayBlob("p.proj/columns.txt"));
}

TEST(CASInlinePlacement, EagerMetadataFilesAreInlineCandidates)
{
    EXPECT_FALSE(partFileMustStayBlob("checksums.txt"));
    EXPECT_FALSE(partFileMustStayBlob("columns.txt"));
    EXPECT_FALSE(partFileMustStayBlob("count.txt"));
    EXPECT_FALSE(partFileMustStayBlob("serialization.json"));
    EXPECT_FALSE(partFileMustStayBlob("metadata_version.txt"));
    EXPECT_FALSE(partFileMustStayBlob("partition.dat"));
    EXPECT_FALSE(partFileMustStayBlob("minmax_date.idx"));
    EXPECT_FALSE(partFileMustStayBlob("default_compression_codec.txt"));
}
