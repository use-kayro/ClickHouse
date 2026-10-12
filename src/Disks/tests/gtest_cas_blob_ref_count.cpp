#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobRefCount.h>

#include <atomic>
#include <chrono>
#include <thread>

using namespace DB::Cas;

namespace
{

BlobRef ref(UInt64 n)
{
    return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(static_cast<UInt128>(n) + 1)};
}

}

/// TEC-851 step 3c: the in-memory count is the only liveness and dedup state.
TEST(CasBlobRefCount, ReferencesHoldUntilReleased)
{
    BlobRefCount count;
    EXPECT_TRUE(count.unreferencedNow(ref(1)));
    count.add({ref(1), ref(2)});   /// a held part
    count.add({ref(2)});           /// another held part sharing a blob
    EXPECT_FALSE(count.unreferencedNow(ref(1)));
    EXPECT_EQ(count.stats().blobs, 2u);
    EXPECT_EQ(count.stats().references, 3u);

    count.release({ref(1), ref(2)});
    EXPECT_TRUE(count.unreferencedNow(ref(1)));
    EXPECT_FALSE(count.unreferencedNow(ref(2)));
    count.release({ref(2)});
    EXPECT_TRUE(count.unreferencedNow(ref(2)));
    EXPECT_EQ(count.stats().blobs, 0u);
}

/// A build claims before it decides: an unreferenced blob is uploaded, a referenced one adopted. A claim is
/// visible to GC (`unreferencedNow` false) from the instant it is made, upload in flight included.
TEST(CasBlobRefCount, ClaimDecidesUploadOrAdoptAndIsVisibleToGc)
{
    BlobRefCount count;
    EXPECT_EQ(count.claimForBuild(ref(1)), BlobRefCount::Claim::Upload);
    EXPECT_FALSE(count.unreferencedNow(ref(1)));   /// GC skips a blob being uploaded
    count.uploadFinished(ref(1), /*succeeded=*/true);
    EXPECT_EQ(count.claimForBuild(ref(1)), BlobRefCount::Claim::Present);   /// a second build adopts, no request

    count.add({ref(1)});              /// the committed part holds it before the builds release theirs
    count.release({ref(1), ref(1)});  /// both builds' claims
    EXPECT_FALSE(count.unreferencedNow(ref(1)));
    count.release({ref(1)});
    EXPECT_TRUE(count.unreferencedNow(ref(1)));
    EXPECT_EQ(count.claimForBuild(ref(1)), BlobRefCount::Claim::Upload);   /// unconditional re-upload
}

/// A second build never adopts a body whose upload is still in flight: it waits, and adopts on success.
TEST(CasBlobRefCount, SecondClaimWaitsForTheUploadInFlight)
{
    BlobRefCount count;
    ASSERT_EQ(count.claimForBuild(ref(7)), BlobRefCount::Claim::Upload);
    std::atomic<int> decided{-1};
    std::thread second([&] { decided = static_cast<int>(count.claimForBuild(ref(7))); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(decided.load(), -1);
    count.uploadFinished(ref(7), /*succeeded=*/true);
    second.join();
    EXPECT_EQ(decided.load(), static_cast<int>(BlobRefCount::Claim::Present));
}

/// A failed upload releases its claim, so the waiting build uploads instead of adopting a body that never
/// landed.
TEST(CasBlobRefCount, FailedUploadHandsTheUploadToTheNextClaim)
{
    BlobRefCount count;
    ASSERT_EQ(count.claimForBuild(ref(9)), BlobRefCount::Claim::Upload);
    std::atomic<int> decided{-1};
    std::thread second([&] { decided = static_cast<int>(count.claimForBuild(ref(9))); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    count.uploadFinished(ref(9), /*succeeded=*/false);
    second.join();
    EXPECT_EQ(decided.load(), static_cast<int>(BlobRefCount::Claim::Upload));
    count.uploadFinished(ref(9), /*succeeded=*/true);
    EXPECT_FALSE(count.unreferencedNow(ref(9)));
    count.release({ref(9)});
    EXPECT_TRUE(count.unreferencedNow(ref(9)));
}

/// The GC ordering, interleaved by hand: GC reads the ETag (outside the count), a build claims, GC checks.
/// Either GC sees the claim, or the build saw no reference and uploads after GC's ETag read (a fresh
/// envelope, so GC's exact delete mismatches). Both cases leave the build's blob present.
TEST(CasBlobRefCount, GcCheckAfterEtagReadSeesAConcurrentClaim)
{
    BlobRefCount count;
    /// GC: HEAD happened here (ETag e0 of a body nothing references).
    EXPECT_EQ(count.claimForBuild(ref(3)), BlobRefCount::Claim::Upload);   /// build re-uploads
    EXPECT_FALSE(count.unreferencedNow(ref(3)));   /// GC's check after its HEAD: skip
}
