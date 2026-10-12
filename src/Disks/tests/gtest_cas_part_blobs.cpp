#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartBlobs.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

#include <stdexcept>
#include <vector>

/// `PartBlobs::upload`, the one blob-upload path of the content-addressed disk (TEC-851 step 3c): one task
/// per distinct blob on the server-wide upload pool, an unconditional fresh-envelope PUT for a blob nothing
/// on this server references, no request for one something references, and a refusal on a server that
/// does not write blobs.

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::expectThrowsCode;

namespace
{

BlobUploadRequest request(const String & payload)
{
    BlobUploadRequest req;
    req.ref = poolContentHash(BlobHashAlgo::CityHash128, payload);
    req.source = BlobSource::fromString(payload);
    req.declared_size = payload.size();
    return req;
}

PoolPtr openPool(const std::shared_ptr<InMemoryBackend> & backend)
{
    DB::Cas::tests::ensureBlobUploadPoolForTest();
    return Pool::open(backend, PoolConfig{.pool_prefix = "p", .mint_pool_meta = true});
}

/// A deterministic local failure on every blob publication.
class FailingPublishBackend final : public InMemoryBackend
{
public:
    void publish(const BlobPublishRequest &, TransportAccess &) override { throw std::runtime_error("injected publish failure"); }
};

}

TEST(CASPartBlobs, UploadsEachDistinctBlobOnceAndAdoptsWhatIsReferenced)
{
    auto backend = std::make_shared<CountingBackend>();
    auto pool = openPool(backend);
    const std::vector<BlobUploadRequest> first{request("a"), request("b"), request("a")};
    {
        PartBlobs build(pool, "ns/all_1_1_0", /*may_upload=*/true);
        build.upload(first);
        EXPECT_EQ(backend->publishTotal(), 2u);   /// one task per distinct blob

        /// A second part with the same content, while the first build still references it: adopted.
        PartBlobs second(pool, "ns/all_2_2_0", /*may_upload=*/true);
        second.upload(std::vector<BlobUploadRequest>{request("a"), request("b")});
        EXPECT_EQ(backend->publishTotal(), 2u);
        EXPECT_FALSE(pool->blobRefs()->unreferencedNow(first[0].ref));
    }
    /// Both builds are gone and no part holds the blobs: nothing on this server references them.
    EXPECT_TRUE(pool->blobRefs()->unreferencedNow(first[0].ref));
    EXPECT_TRUE(pool->blobRefs()->unreferencedNow(first[1].ref));

    /// With nothing referencing it, the next build uploads again, unconditionally.
    PartBlobs third(pool, "ns/all_3_3_0", /*may_upload=*/true);
    third.upload(std::vector<BlobUploadRequest>{request("a")});
    EXPECT_EQ(backend->publishTotal(), 3u);
}

TEST(CASPartBlobs, RefusedOnAServerThatDoesNotWriteBlobs)
{
    auto backend = std::make_shared<CountingBackend>();
    auto pool = openPool(backend);
    const auto req = request("reader-payload");
    PartBlobs build(pool, "ns/all_1_1_0", /*may_upload=*/false);
    expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&] { build.upload(std::vector<BlobUploadRequest>{req}); });
    EXPECT_EQ(backend->publishTotal(), 0u);
    EXPECT_TRUE(pool->blobRefs()->unreferencedNow(req.ref)) << "a refused upload must not keep its claim";
}

TEST(CASPartBlobs, FailedUploadRethrowsAndReleasesTheClaim)
{
    auto backend = std::make_shared<FailingPublishBackend>();
    auto pool = openPool(backend);
    const auto req = request("doomed");
    PartBlobs build(pool, "ns/all_1_1_0", /*may_upload=*/true);
    EXPECT_THROW(build.upload(std::vector<BlobUploadRequest>{req, request("doomed-too")}), std::exception);
    EXPECT_TRUE(pool->blobRefs()->unreferencedNow(req.ref)) << "a failed upload must not be adopted later";
}
