#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasServerRootFormats.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasOrphanManifestSweep.h>

#include <limits>

/// The orphan-manifest sweep's pure eligibility predicate.

using namespace DB::Cas;

TEST(CASOrphanSweepRequests, PrefixEligibleUnderIsTheFourComparisons)
{
    MountLease floor;
    floor.writer_epoch = 3;
    floor.min_active_build_sequence = 10;
    EXPECT_TRUE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 2, .build_sequence = 999}));
    EXPECT_FALSE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 4, .build_sequence = 1}));
    EXPECT_TRUE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 3, .build_sequence = 9}));
    EXPECT_FALSE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 3, .build_sequence = 10}));
    floor.min_active_build_sequence = std::numeric_limits<uint64_t>::max();
    EXPECT_TRUE(prefixEligibleUnder(floor, BuildPrefix{.writer_epoch = 3, .build_sequence = 10}));
    EXPECT_FALSE(prefixEligibleUnder(std::nullopt, BuildPrefix{.writer_epoch = 1, .build_sequence = 1}));
}
