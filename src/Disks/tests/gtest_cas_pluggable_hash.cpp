#include <gtest/gtest.h>

/// P1-T2 (CAS pluggable-blob-hash Phase 1):
/// `PoolMeta` records the pool-wide `blob_hash_algo` and `PoolMeta::createOrValidate` fail-closes on a
/// disk config that disagrees with an existing pool's recorded algo -- the pool-wide durability
/// invariant (never silently re-hash an existing pool).
///
/// Phase 3 T4 RELAXES that single fail-closed
/// value into `PoolMeta::algos_used` (sorted, append-only): a config algo already a MEMBER is
/// accepted with no write (steady state); a non-member is admitted via a CAS-union ONLY when the
/// disk opts in (`blob_hash_allow_new`), and refused (`BAD_ARGUMENTS`, same as before) otherwise --
/// a changed config alone must never silently turn a pool mixed. See `AdmissionIsFlagGated` and
/// `ConcurrentAdmissionUnions` below.
///
/// P1-T3a (this file, extended): the pool's `blob_hash_algo` is threaded into the three hash sites
/// (spec §5/§6) -- `Cas::CaContentWriteBuffer` (streaming blob-body hash),
/// `PartWriteTxn`'s envelope `hash_algo` field, and (transitively, via `Cas::blobHashHexOneShot`) the
/// `poolContentHash` content-key mint on the write path. `poolContentHash` itself is a static
/// helper in `CasPartWriteTxn.cpp` and not directly reachable from a gtest; its production callers already
/// exercise the default `CityHash128` path, and it delegates to the SAME `Cas::blobHashHexOneShot`
/// this file tests directly below.

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include "cas_test_helpers.h"

#include <IO/WriteBufferFromFile.h>


#include <algorithm>
#include <filesystem>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_FORMAT_VERSION;
}

using namespace DB::Cas;
using namespace DB::Cas::tests;


namespace
{

/// ---- Small raw-fixture request-engine wrappers shared by the tests below ----

/// True iff `key` exists.
bool existsAt(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).head(key, Retry::once()).has_value();
}

/// The durable object at `key`, or `nullopt`.
std::optional<Object> readAt(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::once());
}

/// A deterministic, non-repeating-byte payload spanning several `DBMS_DEFAULT_HASHING_BLOCK_SIZE`
/// (2048 B) blocks, so a chunked-vs-one-shot divergence (the CityHash128 pitfall documented on
/// `poolContentHash`) would not accidentally go unnoticed.
std::string makeMultiBlockPayload(size_t size = 5000)
{
    std::string s;
    s.reserve(size);
    for (size_t i = 0; i < size; ++i)
        s.push_back(static_cast<char>('a' + (i % 23)));
    return s;
}

}

TEST(CASPluggableHash, PoolMetaRoundTripsAlgosUsed)
{
    PoolMeta pm;
    pm.pool_id = u128Of("pool-a");
    pm.blob_header_len = 256;
    pm.algos_used = {static_cast<uint8_t>(BlobHashAlgo::CityHash128), static_cast<uint8_t>(BlobHashAlgo::XXH3_128)};

    const PoolMeta back = decodePoolMeta(encodePoolMeta(pm));
    EXPECT_EQ(back.algos_used, pm.algos_used);
    EXPECT_EQ(back.blob_header_len, 256u);
}

TEST(CASPluggableHash, CreateOrValidateRecordsConfigAlgoOnFreshPool)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest meta_op(*backend);

    const PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, /*blob_header_len*/ 256, BlobHashAlgo::XXH3_128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::XXH3_128)}));

    /// Reopening with the SAME algo is a no-op reopen: the recorded value comes back unchanged.
    const PoolMeta reopened = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::XXH3_128);
    EXPECT_EQ(reopened.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::XXH3_128)}));
    EXPECT_EQ(reopened.pool_id, pm.pool_id);
}

TEST(CASPluggableHash, CreateOrValidateDefaultsToCityHash128)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest meta_op(*backend);

    const PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
    EXPECT_EQ(pm.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));
}

/// Phase 3 T4 (spec §5, replaces the Phase 1/2 unconditional-fail-close test of the same shape):
/// admission of a NEW algo is EXPLICIT OPT-IN -- the default reopen with a non-member algo still
/// fails closed (`BAD_ARGUMENTS`), but the message names `<cas_blob_hash_allow_new>` and the pool
/// is truly extensible with the flag set. See `AdmissionIsFlagGated` below for the full flow.
TEST(CASPluggableHash, CreateOrValidateFailsClosedOnAlgoMismatchWithoutFlag)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest meta_op(*backend);

    PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);

    expectThrowsCodeWithMessage(
        DB::ErrorCodes::BAD_ARGUMENTS,
        "<cas_blob_hash_allow_new>1</cas_blob_hash_allow_new>",
        [&]
        {
            PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::XXH3_128, /*allow_new*/ false);
        });

    /// The pool is untouched by the refused reopen: a subsequent open with the ORIGINAL algo still
    /// succeeds and returns the same pool_id.
    const PoolMeta reopened = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128);
    EXPECT_EQ(reopened.algos_used, (std::vector<uint8_t>{static_cast<uint8_t>(BlobHashAlgo::CityHash128)}));
}

/// spec §9.1 at the unit level: admission of a new algo requires the flag; once admitted, membership
/// alone is the steady-state check (the flag is not needed again for the same algo).
TEST(CASPluggableHash, AdmissionIsFlagGated)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest meta_op(*backend);
    PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);

    /// without the flag: refuse, pool untouched
    expectThrowsCode(DB::ErrorCodes::BAD_ARGUMENTS, [&]
    { PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::Sha256, false); });

    /// with the flag: admitted
    const PoolMeta admitted = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::Sha256, true);
    EXPECT_EQ(admitted.algos_used, (std::vector<uint8_t>{1, 3}));

    /// steady state: admitted algo reopens WITHOUT the flag
    const PoolMeta steady = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::Sha256, false);
    EXPECT_EQ(steady.algos_used, (std::vector<uint8_t>{1, 3}));
}

TEST(CASPluggableHash, ConcurrentAdmissionUnions)
{
    auto backend = std::make_shared<InMemoryBackend>();
    const Layout layout("p");
    OperationForTest meta_op(*backend);
    PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128, false, /*allow_mint*/ true);
    PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::XXH3_128, true);
    PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::Sha256, true);
    const PoolMeta final_pm = PoolMeta::createOrValidate(*meta_op, layout, 256, BlobHashAlgo::CityHash128, false);
    EXPECT_EQ(final_pm.algos_used, (std::vector<uint8_t>{1, 2, 3}));   /// union, sorted, nothing lost
}

/// ---- P1-T3a: the pool's blob_hash_algo threaded into the streaming write-buffer hash site ----

/// `Cas::CaContentWriteBuffer`'s LOCAL-staging constructor (the everyday spill-to-temp-file
/// mode `ContentAddressedTransaction::writeFile` uses), built with `BlobHashAlgo::XXH3_128`, must hash
/// the streamed payload with xxh3 -- agreeing with the standalone `blobHashHexOneShot` one-shot helper
/// (the same convention `poolContentHash`'s re-hash uses).
TEST(CASPluggableHash, ContentWriteBufferLocalModeHashesWithSelectedAlgoXxh3)
{
    const std::string payload = makeMultiBlockPayload();
    const auto temp_dir = (std::filesystem::temp_directory_path() / "cas_pluggable_hash_xxh3_local").string();

    std::string got_hash_hex;
    size_t got_size = 0;
    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        temp_dir,
        BlobHashAlgo::XXH3_128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string & hash_hex, size_t size, const std::string &)
        {
            got_hash_hex = hash_hex;
            got_size = size;
        });

    /// Write in two chunks so more than one nextImpl flush happens (exercises the streaming state, not
    /// just a single call).
    buf->write(payload.data(), 1234);
    buf->write(payload.data() + 1234, payload.size() - 1234);
    buf->finalize();

    EXPECT_EQ(got_size, payload.size());
    EXPECT_EQ(got_hash_hex, blobHashHexOneShot(BlobHashAlgo::XXH3_128, payload));
    /// A wrong-but-plausible result (e.g. accidentally still hashing with cityHash128) would silently
    /// produce a DIFFERENT hex string -- pin that the two algos disagree on this payload, so the
    /// assertion above is actually discriminating.
    EXPECT_NE(got_hash_hex, blobHashHexOneShot(BlobHashAlgo::CityHash128, payload));
}

/// The DEFAULT algo (`CityHash128`) through the SAME write buffer must stay byte-for-byte unchanged --
/// the CAS pluggable-blob-hash invariant (spec §8). Compares against `blobHashHexOneShot`, which
/// `gtest_cas_blob_hasher.cpp`'s `CityHash128ByteIdenticalToHashingWriteBuffer` already proves is
/// byte-identical to the pre-existing plain `HashingWriteBuffer` convention.
TEST(CASPluggableHash, ContentWriteBufferLocalModeCityHash128Unchanged)
{
    const std::string payload = makeMultiBlockPayload();
    const auto temp_dir = (std::filesystem::temp_directory_path() / "cas_pluggable_hash_ch128_local").string();

    std::string got_hash_hex;
    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        temp_dir,
        BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string & hash_hex, size_t, const std::string &)
        {
            got_hash_hex = hash_hex;
        });

    buf->write(payload.data(), payload.size());
    buf->finalize();

    EXPECT_EQ(got_hash_hex, blobHashHexOneShot(BlobHashAlgo::CityHash128, payload));
}

/// (codecs-v3 phase 7) The two former `Pool...StampsEnvelopeHashAlgo...` tests were REMOVED: the v3
/// blob envelope no longer carries a `hash_algo` field (the algo identity lives in the blob KEY, spec
/// §blob-envelope). Algo correctness for the write path is covered by the P1-T3b blob-body-PATH-key
/// tests below (they assert the blob key uses the pool's algo), which is the surviving source of truth.

/// ---- P1-T3b: the pool's blob_hash_algo threaded into blob-body PATH keys (spec §3/§10) ----

/// A blob written and promoted through a live ref on an xxh3-128 pool lands under the
/// `blobs/xxh3/<shard>/<hex>` path segment (not the bare `blobs/<shard>/<hex>` shape), is readable at
/// that key, and `runFsck`'s LIST-based discovery (`Layout::blobsPrefix`, deliberately algo-agnostic)
/// finds it reachable and clean -- proving the GC/fsck key-parse (which takes only the LAST path
/// component as the hex digest, `CasFsck.cpp`) still works with the extra segment.
TEST(CASPluggableHash, Xxh3BlobLandsUnderAlgoSegmentAndIsDiscoveredCleanByFsck)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::XXH3_128});

    const RootNamespace ns{"srv1/tbl"};
    const std::string payload = makeMultiBlockPayload();
    const BlobRef id{BlobHashAlgo::XXH3_128, codecFor(BlobHashAlgo::XXH3_128).fromHex(blobHashHexOneShot(BlobHashAlgo::XXH3_128, payload))};

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/rb";
    auto build = store->beginPartWrite(info);

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = id;

    e.blob_size = payload.size();
    const ManifestId mid = build->stageManifest({e});
    build->precommitAdd(ns, "rb", mid);
    build->putBlob(id, BlobSource::fromString(payload));

    /// The blob body landed under the algo-segmented path -- readable there, not at the legacy
    /// no-segment shape.
    const String blob_key = store->layout().blobKey(id);
    EXPECT_NE(blob_key.find("/blobs/xxh3/"), String::npos) << blob_key;
    EXPECT_EQ(blob_key.find("/blobs/ch128/"), String::npos) << blob_key;
    EXPECT_TRUE(existsAt(*backend, blob_key));

    build->promote(ns, "rb", build->buildId(), mid);
    store->renewWatermarkOnce();

    const FsckReport rep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_GE(rep.reachable, 1u);

    /// Not merely "clean by omission" (e.g. a bug that silently LISTed nothing): the physical listing
    /// actually walked the algo-segmented key.
    const bool found = std::any_of(rep.objects.begin(), rep.objects.end(),
        [](const FsckObject & o) { return o.key.find("/blobs/xxh3/") != String::npos; });
    EXPECT_TRUE(found);
}

/// ============================================================================================
/// End-to-end SHA-256 write path. Every `PartWriteTxn` representation downstream of digest creation
/// must preserve the pool's variable-width digest; truncation would address a different blob. This
/// test drives a REAL `PartWriteTxn` (`putBlob` -> `stageManifest` ->
/// `precommitAdd` -> `promote`) on a `Sha256` pool and asserts:
///   1. the blob lands under `blobs/sha256/<64-hex>` and the manifest entry's `blob_hash`, read back via
///      `decodePartManifest`, is the FULL 32-byte digest (bytes beyond 16 are non-zero for a real sha256
///      digest, i.e. NOT truncated to `.toU128()`'s low 16 bytes);
///   2. an inline file and a standalone blob of IDENTICAL content get the SAME 32-byte `file_hash` under
///      sha256 -- mirroring the (fixed) `ContentAddressedTransaction.cpp` inline-candidate formula
///      (`blobHashHexOneShot(pool_algo, bytes)` -> pool-scoped `DigestCodec::fromHex`) directly at the
///      Core level, since exercising the wiring itself is Task 7's job;
///   3. `runFsck` on the pool is clean (no dangling, no foreign) -- the whole write -> GC -> fsck loop
///      agrees on the 64-hex key.
TEST(CASPluggableHash, Sha256BuildWritesFullWidthDigestAndInlineEqualsBlob)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = Pool::open(backend,
        PoolConfig{.pool_prefix = "p", .server_root_id = "test",
                   .blob_hash_algo = BlobHashAlgo::Sha256});
    ASSERT_EQ(blobHashLenFor(store->writeAlgo()), 32u) << "sha256 must derive a 32-byte digest width";
    const DigestCodec codec = codecFor(store->writeAlgo());

    const RootNamespace ns{"srv1/tbl"};
    const std::string payload = makeMultiBlockPayload();
    const std::string hex = blobHashHexOneShot(BlobHashAlgo::Sha256, payload);
    ASSERT_EQ(hex.size(), 64u) << "sha256 renders 64 lowercase hex chars";
    const BlobRef id{BlobHashAlgo::Sha256, codec.fromHex(hex)};

    PartWriteInfo info;
    info.intended_ref = ns.string() + "/part1";
    auto build = store->beginPartWrite(info);

    /// Mirror the (fixed) inline-candidate hash site directly: same content, same pool algo, via the
    /// SAME public formula ContentAddressedTransaction.cpp's writeFile now uses -- NOT the old hardcoded
    /// CityHash128 (which would produce a DIFFERENT, 128-bit-then-zero-padded value here).
    const BlobDigest inline_hash = codec.fromHex(blobHashHexOneShot(BlobHashAlgo::Sha256, payload));
    const BlobDigest blob_hash = codec.fromHex(hex);
    EXPECT_EQ(inline_hash, blob_hash) << "inline == blob: identical content must hash identically under sha256";

    /// THE CRUX (width): a genuine 32-byte sha256 digest must NOT be zero-padded past byte 16 -- the
    /// shape `BlobDigest::fromU128` (or a reverted hardcoded-CityHash128 inline site) would produce.
    const bool tail_nonzero = std::any_of(blob_hash.bytes.begin() + 16, blob_hash.bytes.end(),
        [](uint8_t b) { return b != 0; });
    EXPECT_TRUE(tail_nonzero) << "a genuine sha256 digest must not be zero-padded past byte 16";

    ManifestEntry blob_entry;
    blob_entry.path = "data.bin";
    blob_entry.placement = EntryPlacement::Blob;
    blob_entry.ref = BlobRef{BlobHashAlgo::Sha256, blob_hash};
    blob_entry.blob_size = payload.size();

    ManifestEntry inline_entry;
    inline_entry.path = "checksums.txt";
    inline_entry.placement = EntryPlacement::Inline;
    inline_entry.ref = BlobRef{BlobHashAlgo::Sha256, inline_hash};
    inline_entry.blob_size = payload.size();
    inline_entry.inline_bytes = payload;

    const ManifestId mid = build->stageManifest({blob_entry, inline_entry});
    build->precommitAdd(ns, "part1", mid);
    const PutBlobResult ref = build->putBlob(id, BlobSource::fromString(payload));
    EXPECT_EQ(ref.size, payload.size());

    /// THE CRUX (blob side): the blob body lands under the sha256-segmented path, addressed by the
    /// FULL 64-hex key -- `PartWriteTxn::putBlob`'s internal `logical_hash` must not have silently narrowed it
    /// to a 32-hex (128-bit) key.
    const String blob_key = store->layout().blobKey(id);
    EXPECT_NE(blob_key.find("/blobs/sha256/"), String::npos) << blob_key;
    ASSERT_TRUE(existsAt(*backend, blob_key));

    build->promote(ns, "part1", build->buildId(), mid);
    store->renewWatermarkOnce();

    /// Read the committed manifest back -- the on-disk `blob_hash` must be the FULL 32-byte digest, not
    /// truncated by the manifest codec or by anything upstream of `stageManifest`.
    const auto manifest_bytes = readAt(*backend, store->layout().manifestKey(mid));
    ASSERT_TRUE(manifest_bytes.has_value());
    const PartManifest read_back = decodePartManifest(openObject(FormatId::PartManifest, manifest_bytes->bytes));
    ASSERT_EQ(read_back.entries.size(), 2u);
    const auto read_blob_it = std::find_if(read_back.entries.begin(), read_back.entries.end(),
        [](const ManifestEntry & e) { return e.placement == EntryPlacement::Blob; });
    ASSERT_NE(read_blob_it, read_back.entries.end());
    EXPECT_EQ(read_blob_it->ref.digest, blob_hash);
    const bool read_tail_nonzero = std::any_of(read_blob_it->ref.digest.bytes.begin() + 16,
        read_blob_it->ref.digest.bytes.end(), [](uint8_t b) { return b != 0; });
    EXPECT_TRUE(read_tail_nonzero) << "the manifest's on-disk blob_hash must not be truncated either";

    /// The write -> GC -> fsck loop must agree end-to-end on the 64-hex key: clean, no dangling.
    const FsckReport rep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(rep.clean());
    EXPECT_EQ(rep.dangling, 0u);
    EXPECT_GE(rep.reachable, 1u);
}

/// ============================================================================================
/// CAS reader-generation gate (`CasFormat.h`'s `G_BUILD`). This test's assertions read `G_BUILD`
/// itself rather than a hardcoded generation number, so a future bump does not silently make them
/// false. `PoolMeta::createOrValidate`'s open-time CAS-raise targets `G_BUILD`, and `decodePoolMeta`
/// fail-closes BOTH on a FUTURE `min_reader_generation` AND on a BACKWARD pool whose header
/// `compatibility_version` is below the format-generation baseline (see `CasPoolMetaFormat.cpp`).
/// ============================================================================================

TEST(CASPluggableHash, ReaderGenerationIsRaisedToGBuild)
{
    /// A freshly opened/created pool records `min_reader_generation == G_BUILD` (the open-time
    /// CAS-raise, `PoolMeta::createOrValidate`, always targets this build's own floor).
    {
        auto backend = std::make_shared<InMemoryBackend>();
        auto store = Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
        EXPECT_EQ(store->poolMeta().min_reader_generation, G_BUILD);

        const auto meta_bytes = readAt(*backend, store->layout().poolMetaKey());
        ASSERT_TRUE(meta_bytes.has_value());
        EXPECT_EQ(decodePoolMeta(meta_bytes->bytes).min_reader_generation, G_BUILD);
    }

    /// FORWARD gate: a pool-meta carrying `min_reader_generation == G_BUILD + 1` (one generation past
    /// THIS build's floor) fails closed at open -- the startup gate (`decodePoolMeta`) rejects it.
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p");
        OperationForTest meta_op(*backend);
        PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, /*blob_header_len*/ 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
        pm.min_reader_generation = G_BUILD + 1;
        ASSERT_TRUE(std::holds_alternative<Committed>((*meta_op).replace(layout.poolMetaKey(), encodePoolMeta(pm), (*meta_op).head(layout.poolMetaKey(), Retry::once())->etag, Retry::once())));

        expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&]
        { Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });
    }

    /// BACKWARD floor: a pool whose header `v` (compatibility_version) is below the format-generation
    /// baseline predates every build this reader can trust. Craft it at the text layer: take a fresh
    /// pool-meta and rewrite its line-1 version gate down to `G_BUILD - 1`.
    {
        auto backend = std::make_shared<InMemoryBackend>();
        const Layout layout("p");
        OperationForTest meta_op(*backend);
        PoolMeta pm = PoolMeta::createOrValidate(*meta_op, layout, /*blob_header_len*/ 256, BlobHashAlgo::CityHash128, /*allow_new*/ false, /*allow_mint*/ true);
        const String fresh_bytes = encodePoolMeta(pm);

        const String from = "\"v\":" + std::to_string(G_BUILD);
        const String to = "\"v\":" + std::to_string(G_BUILD - 1);
        const auto pos = fresh_bytes.find(from);
        ASSERT_NE(pos, String::npos);   // sanity: a fresh pool stamps the header at the floor
        String downgraded = fresh_bytes;
        downgraded.replace(pos, from.size(), to);
        ASSERT_TRUE(std::holds_alternative<Committed>((*meta_op).replace(layout.poolMetaKey(), downgraded, (*meta_op).head(layout.poolMetaKey(), Retry::once())->etag, Retry::once())));

        /// `decodePoolMeta`'s backward floor rejects the downgraded bytes directly...
        expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { decodePoolMeta(downgraded); });
        /// ...and so does a full `Pool::open` (decoding the pool-meta is its first fail-closed step).
        expectThrowsCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&]
        { Pool::open(backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"}); });
    }
}
