#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include "config.h"
#if USE_AWS_S3
#include <IO/S3Common.h>
#endif
#include <Disks/tests/cas_test_helpers.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes { extern const int ABORTED; extern const int CORRUPTED_DATA; extern const int NOT_IMPLEMENTED; }

using namespace DB::Cas;

namespace
{
UInt128 b(uint64_t n) { return UInt128(n); }
UInt128 s(uint64_t n) { return UInt128(n); }   // source-edge id

/// A `RunMarker::Condemned` sentinel record for `h` at the zero source_id, carrying the condemned incarnation.
SourceEdgeRecord condemnedRec(UInt128 h, const CondemnedRow & row)
{
    return SourceEdgeRecord{.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)},
                            .source_id = UInt128{0}, .marker = RunMarker::Condemned,
                            .delete_pending = row.delete_pending, .token = row.token,
                            .size = row.size, .condemn_round = row.condemn_round};
}

/// An active-edge record (`RunMarker::Edge`) for `h` at source `sid`.
SourceEdgeRecord edgeRec(UInt128 h, UInt128 sid)
{
    return SourceEdgeRecord{.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(h)},
                            .source_id = sid, .marker = RunMarker::Edge};
}

/// Build a source-edge run (`kSourceEdgeKeySchema128`) carrying the given `RunMarker::Condemned` sentinel rows
/// and surviving edges, write it under `blobTargetRunKey(gen, attempt, shard, 0)`, and return its
/// `RunRef`. Rows are emitted in (blob_hash, source_id) order (sentinels at source_id 0 sort first
/// per blob).
RunRef writeSourceEdgeRun(InMemoryBackend & backend, const Layout & layout,
                          uint64_t gen, uint64_t attempt, uint64_t shard,
                          const std::vector<std::pair<UInt128, CondemnedRow>> & condemned,
                          const std::vector<std::pair<UInt128, UInt128>> & edges = {})
{
    std::vector<SourceEdgeRecord> recs;
    for (const auto & [h, row] : condemned)
        recs.push_back(condemnedRec(h, row));
    for (const auto & [h, sid] : edges)
        recs.push_back(edgeRec(h, sid));
    /// The writer requires non-decreasing (ref, source_id) order (sentinels at source_id 0 sort first
    /// per blob, exactly reproducing the old raw-key order).
    std::stable_sort(recs.begin(), recs.end(), [](const SourceEdgeRecord & a, const SourceEdgeRecord & bb)
    {
        if (a.ref < bb.ref)
            return true;
        if (bb.ref < a.ref)
            return false;
        return a.source_id < bb.source_id;
    });

    DB::WriteBufferFromOwnString out;
    SourceEdgeRunWriter writer(out);
    for (const auto & rec : recs)
        writer.append(rec);
    writer.finish();
    out.finalize();

    const String bytes = out.str();
    const String key = layout.blobTargetRunKey(gen, attempt, shard, 0);
    DB::Cas::tests::OperationForTest op(backend);
    (*op).create(key, bytes, Retry::once());
    return RunRef{.key = key, .checksum = sourceEdgeRunChecksum(bytes), .shard = shard, .key_generation = gen};
}

}


TEST(CASBlobInDegree, ZeroInDegreeSealChecksumMismatchFailsClosed)
{
    InMemoryBackend backend;
    DB::Cas::tests::OperationForTest backend_req(backend);
    Layout layout{"pool"};
    const RunRef good = writeSourceEdgeRun(backend, layout, /*gen*/1, /*attempt*/0, /*shard*/0,
                                           /*condemned*/{}, /*edges*/{{b(1), s(1)}});
    RunRef bad = good;
    bad.checksum = good.checksum + 1;
    std::vector<RunRef> runs{bad};
    EXPECT_THROW(zeroInDegree(*backend_req, runs), DB::Exception);
}

/// ==== RunMarker::Condemned row codec + typed source-edge open (retired-in-snapshot T2, spec §2.1) ====

TEST(CASCondemnedRow, RoundTripAllTokenTypes)
{
    /// Walked over the vocabulary's own entries rather than a hand-copied list, so a dialect the
    /// encoder can construct but this test forgot cannot exist.
    for (const auto & entry : DB::Cas::kTokenTypeWords.entries)
    {
        DB::Cas::CondemnedRow row;
        row.delete_pending = (entry.value == DB::Cas::Dialect::Generation);
        row.marker_confirmed = (entry.value == DB::Cas::Dialect::Emulated);
        row.token = DB::Cas::PersistedEtag{String(entry.word), "etag-abc-123"};
        row.size = 4096;
        row.condemn_round = 7;
        const auto bytes = DB::Cas::encodeCondemnedRow(row);
        ASSERT_EQ(bytes[0], DB::Cas::runMarkerByte(DB::Cas::RunMarker::Condemned));
        EXPECT_EQ(DB::Cas::decodeCondemnedRow(bytes), row);
    }
}

TEST(CASCondemnedRow, UnknownMarkerByteFailsClosedWithCorruptedData)
{
    /// This pins the condemned-row decoder's own marker validation.
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::PersistedEtag{"etag", "t"};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes[0] = 0x03;

    try
    {
        static_cast<void>(DB::Cas::decodeCondemnedRow(bytes));
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
}

TEST(CASRecordStream, RunMarkerByteContractFailsClosed)
{
    /// This helper is defense-in-depth; upstream word validation means no input path reaches it.
    for (const auto marker : {DB::Cas::RunMarker::Zero, DB::Cas::RunMarker::Edge, DB::Cas::RunMarker::Condemned})
        EXPECT_EQ(DB::Cas::runMarkerFromByte(DB::Cas::runMarkerByte(marker), "CAS test"), marker);

    try
    {
        static_cast<void>(DB::Cas::runMarkerFromByte(0x03, "CAS test"));
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
    }
}

TEST(CASCondemnedRow, UnknownFlagBitsFailClosed)
{
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::PersistedEtag{"etag", "t"};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes[1] = 4;   // flags byte: only bits 0 (delete_pending) and 1 (marker_confirmed) are defined
    EXPECT_THROW(DB::Cas::decodeCondemnedRow(bytes), DB::Exception);
}

TEST(CASCondemnedRow, UnknownTokenTypeFailsClosed)
{
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::PersistedEtag{"etag", "t"};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes[2] = 99;   // token_type byte (offset: [0]=0x02 [1]=flags [2]=token_type)
    EXPECT_THROW(DB::Cas::decodeCondemnedRow(bytes), DB::Exception);
}

TEST(CASCondemnedRow, TruncatedPayloadFailsClosed)
{
    DB::Cas::CondemnedRow row;
    row.token = DB::Cas::PersistedEtag{"etag", "0123456789"};
    auto bytes = DB::Cas::encodeCondemnedRow(row);
    bytes.resize(bytes.size() - 3);   // token bytes shorter than declared token_len
    EXPECT_THROW(DB::Cas::decodeCondemnedRow(bytes), DB::Exception);
}

TEST(CASSourceEdgeRun, SourceEdgeIdZeroIsReserved)
{
    /// The zero source_id is the sentinel namespace; producers fail closed on a zero hash
    /// (probability 2^-128 — the check documents the reservation).
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            DB::Cas::assertValidSourceEdgeId(UInt128{0});
        },
        "source_id 0 is the reserved sentinel key");
    EXPECT_NO_THROW(DB::Cas::assertValidSourceEdgeId(UInt128{1}));
}

/// ==== schema 3 key codec (Phase 3 T3, mixed-algo pools) ====

TEST(CASSourceEdgeKeySchema3, MixedWidthKeysOrderAlgoFirst)
{
    const BlobDigest d16 = BlobDigest::fromU128((UInt128(0xFFFFFFFFFFFFFFFFULL) << 64) | 0xFFULL);
    BlobDigest d32{};                                    /// sha256 digest starting 0x00,0x01 — small bytes
    d32.bytes[1] = 0x01;
    const BlobRef ch{BlobHashAlgo::CityHash128, d16};    /// algo=1, digest all-FF prefix
    const BlobRef sh{BlobHashAlgo::Sha256, d32};         /// algo=3, tiny digest
    const String k_ch = SourceEdgeKeyCodec::key(ch, UInt128(7));   /// 33 bytes
    const String k_sh = SourceEdgeKeyCodec::key(sh, UInt128(7));   /// 49 bytes
    EXPECT_EQ(k_ch.size(), 33u);
    EXPECT_EQ(k_sh.size(), 49u);
    /// algo byte decides BEFORE any digest byte can: ch128(1) < sha256(3) even though the ch128
    /// digest bytes are all 0xFF and the sha256 digest bytes are almost all zero.
    EXPECT_LT(k_ch, k_sh);
    /// sentinel-first inside one blob group:
    EXPECT_LT(SourceEdgeKeyCodec::key(ch, UInt128(0)), k_ch);
}

TEST(CASSourceEdgeKeySchema3, ParseFailsClosed)
{
    BlobRef r; UInt128 sid;
    String k = SourceEdgeKeyCodec::key(BlobRef{BlobHashAlgo::XXH3_128, BlobDigest::fromU128(UInt128(5))}, UInt128(9));
    SourceEdgeKeyCodec::parse(k, r, sid);
    EXPECT_EQ(r.algo, BlobHashAlgo::XXH3_128);
    EXPECT_EQ(r.digest.toU128(), UInt128(5));
    EXPECT_EQ(sid, UInt128(9));
    k[0] = static_cast<char>(99);                        /// unknown algo byte
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NOT_IMPLEMENTED, [&]{ SourceEdgeKeyCodec::parse(k, r, sid); });
    k[0] = static_cast<char>(1);                         /// known algo, wrong length (33 expected, this is 33 — truncate)
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]{ SourceEdgeKeyCodec::parse(std::string_view(k).substr(0, 20), r, sid); });
}
