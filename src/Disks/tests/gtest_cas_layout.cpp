#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Parts/PartPathParser.h>

using namespace DB::Cas;

namespace
{
/// A `BlobRef` at `algo` whose first bytes are `0x00, 0xaa, 0xbb` (the rest zero) -- for key-shape
/// tests that need a stable, recognizable hex prefix. `Layout` no longer captures an algo (Phase 3
/// T2/T3): every blob key is built from a `BlobRef` alone, so key-shape tests construct one directly.
BlobRef prefixedRef(BlobHashAlgo algo)
{
    BlobDigest d{};
    d.bytes[0] = 0x00; d.bytes[1] = 0xaa; d.bytes[2] = 0xbb;
    return BlobRef{algo, d};
}
}

TEST(CASLayout, KeyShapes)
{
    /// Per design §10 EVERY algo carries an explicit path segment: `blobs/ch128/...`, not the legacy
    /// `blobs/...`.
    Layout l{"p"};
    const BlobRef ref = prefixedRef(BlobHashAlgo::CityHash128);
    const String hex = codecFor(BlobHashAlgo::CityHash128).toHex(ref.digest);
    EXPECT_EQ(l.blobKey(ref), "p/blobs/ch128/" + hex.substr(0, 2) + "/" + hex);
    EXPECT_EQ(l.poolMetaKey(), "p/_pool_meta");
}

TEST(CASLayout, BlobKeyCarriesAlgoSegment)
{
    /// Every algo gets its own segment (design §3/§10), so two algos can never collide in the key
    /// space even after a config change on a fresh pool. `Layout` itself carries no algo anymore --
    /// the segment comes from the `BlobRef` passed to `blobKey`.
    const Layout l("p");

    const BlobRef ch128_ref = prefixedRef(BlobHashAlgo::CityHash128);
    const String ch128_hex = codecFor(BlobHashAlgo::CityHash128).toHex(ch128_ref.digest);
    EXPECT_EQ(l.blobKey(ch128_ref), "p/blobs/ch128/" + ch128_hex.substr(0, 2) + "/" + ch128_hex);

    const BlobRef xxh3_ref = prefixedRef(BlobHashAlgo::XXH3_128);
    const String xxh3_hex = codecFor(BlobHashAlgo::XXH3_128).toHex(xxh3_ref.digest);
    EXPECT_EQ(l.blobKey(xxh3_ref), "p/blobs/xxh3/" + xxh3_hex.substr(0, 2) + "/" + xxh3_hex);

    const BlobRef sha256_ref = prefixedRef(BlobHashAlgo::Sha256);
    const String sha256_hex = codecFor(BlobHashAlgo::Sha256).toHex(sha256_ref.digest);
    EXPECT_EQ(l.blobKey(sha256_ref), "p/blobs/sha256/" + sha256_hex.substr(0, 2) + "/" + sha256_hex);

    EXPECT_EQ(l.blobsPrefix(), "p/blobs/");
}

TEST(CASLayout, CasArchiveSuffixConstant)
{
    EXPECT_EQ(DB::Cas::kCasArchiveSuffix, "@cas@");
}

TEST(CASVfsPaths, MirroredArchiveNamespace)
{
    using DB::Cas::mirroredArchiveNamespace;
    /// Atomic: bare uuid -> store/<u3>/<uuid>@cas@
    EXPECT_EQ(mirroredArchiveNamespace("3f2a0000-0000-0000-0000-000000000001"),
              "store/3f2/3f2a0000-0000-0000-0000-000000000001@cas@");
    /// Non-Atomic: a full data/db/tbl path is used verbatim, @cas@ appended to the last segment.
    EXPECT_EQ(mirroredArchiveNamespace("data/mydb/events"),
              "data/mydb/events@cas@");
}
