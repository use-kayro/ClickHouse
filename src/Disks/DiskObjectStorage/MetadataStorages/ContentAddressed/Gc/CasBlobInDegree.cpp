#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasBlobInDegree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <city.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

UInt128 sourceEdgeId(const ManifestId & id, const String & path)
{
    String canon;
    canon += id.root_namespace.string();
    canon += '\0';
    auto beU64 = [&](uint64_t v) { for (int i = 7; i >= 0; --i) canon += static_cast<char>((v >> (8 * i)) & 0xFF); };
    auto beU32 = [&](uint32_t v) { for (int i = 3; i >= 0; --i) canon += static_cast<char>((v >> (8 * i)) & 0xFF); };
    beU64(id.ref.writer_epoch); beU64(id.ref.build_sequence); beU32(id.ref.manifest_ordinal);
    canon += '\0';
    canon += path;
    const auto h = CityHash_v1_0_2::CityHash128(canon.data(), canon.size());
    const UInt128 result = (static_cast<UInt128>(h.high64) << 64) | static_cast<UInt128>(h.low64);
    if (result == UInt128{0})
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS source edge: hash collided with the reserved sentinel id 0");
    return result;
}

void assertValidSourceEdgeId(const UInt128 & source_id)
{
    if (source_id == UInt128{0})
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS source edge: source_id 0 is the reserved sentinel key");
}

String encodeCondemnedRow(const CondemnedRow & row)
{
    String out;
    out.push_back(runMarkerByte(RunMarker::Condemned));
    out.push_back(static_cast<char>((row.delete_pending ? 1 : 0) | (row.marker_confirmed ? 2 : 0)));
    out.push_back(static_cast<char>(dialectByteFromWord(row.token.dialect, "condemned row")));
    auto beU64 = [&](uint64_t v) { for (int i = 7; i >= 0; --i) out += static_cast<char>((v >> (8 * i)) & 0xFF); };
    beU64(row.condemn_round);
    beU64(row.size);
    if (row.token.value.size() > 0xFFFF)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: token too long ({})", row.token.value.size());
    out += static_cast<char>((row.token.value.size() >> 8) & 0xFF);
    out += static_cast<char>(row.token.value.size() & 0xFF);
    out += row.token.value;
    return out;
}

CondemnedRow decodeCondemnedRow(std::string_view p)
{
    /// [0]=0x02 [1]=flags [2]=token_type [3..10]=round [11..18]=size [19..20]=len [21..]=value
    constexpr size_t kFixed = 21;
    if (p.size() < kFixed || runMarkerFromByte(p[0], "CAS condemned row") != RunMarker::Condemned)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: malformed header");
    CondemnedRow row;
    const uint8_t flags = static_cast<uint8_t>(p[1]);
    if (flags > 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: unknown flags 0x{:02x}", flags);
    row.delete_pending = flags & 1;
    row.marker_confirmed = flags & 2;
    row.token.dialect = String(dialectWordFromByte(static_cast<uint8_t>(p[2]), "condemned row"));
    auto beU64 = [&](size_t off) { uint64_t v = 0; for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(p[off + i]); return v; };
    row.condemn_round = beU64(3);
    row.size = beU64(11);
    const size_t len = (static_cast<uint8_t>(p[19]) << 8) | static_cast<uint8_t>(p[20]);
    if (p.size() != kFixed + len)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS condemned row: declared token_len {} vs payload {}", len, p.size() - kFixed);
    row.token.value = String(p.substr(kFixed, len));
    return row;
}

SourceEdgeRunView::SourceEdgeRunView(std::unique_ptr<ReadBuffer> stream_)
    : stream(std::move(stream_))
    , reader(std::make_unique<SourceEdgeRunReader>(*stream))   /// the reader borrows *stream; both are members
{
}

bool SourceEdgeRunView::next(String & key, String & payload)
{
    SourceEdgeRecord rec;
    if (!reader->next(rec))
        return false;
    /// Reconstruct the packed SourceEdgeKeyCodec key + the ORIGINAL payload bytes (a single marker byte,
    /// or the encoded condemned row) so the zeroInDegree / fsck consumers keep
    /// their exact parse/compare logic against the NDJSON codec.
    key = SourceEdgeKeyCodec::key(rec.ref, rec.source_id);
    switch (rec.marker)
    {
        case RunMarker::Edge:
        case RunMarker::Zero:
            payload = String(1, runMarkerByte(rec.marker));
            break;
        case RunMarker::Condemned:
            payload = encodeCondemnedRow(CondemnedRow{.delete_pending = rec.delete_pending,
                                                      .token = rec.token, .size = rec.size,
                                                      .condemn_round = rec.condemn_round,
                                                      .marker_confirmed = rec.marker_confirmed});
            break;
    }
    return true;
}

void SourceEdgeRunView::verifyAgainst(const UInt128 & expected)
{
    reader->verifyAgainst(expected);
}

UInt128 SourceEdgeRunView::accumulatedChecksum()
{
    return reader->accumulatedChecksum();
}

SourceEdgeRunView openSourceEdgeRun(std::string_view bytes)
{
    return SourceEdgeRunView(std::make_unique<ReadBufferFromMemory>(bytes.data(), bytes.size()));
}

SourceEdgeRunView openSourceEdgeRun(CasOperation & op, const String & key)
{
    /// Streaming: a forward-only read of the write-once run — nothing is materialized whole
    /// (cas_run is object_cap = 0). Absent object => fail-closed.
    auto stream = op.stream(key, Retry::standard());
    if (!stream)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS source-edge run: object {} is absent", key);
    return SourceEdgeRunView(std::move(stream));
}

namespace
{
/// len-drift guard (mirrors DigestCodec::checkZeroTail, CasBlobDigest.h): a caller passing a digest
/// wider than the algo's own width is a programming bug, not corrupted on-disk data — chassert, not
/// throw.
void checkZeroTailForAlgo(const BlobDigest & d, uint8_t digest_len, [[maybe_unused]] const char * what)
{
    for (size_t i = digest_len; i < d.bytes.size(); ++i)
        chassert(d.bytes[i] == 0, fmt::format("SourceEdgeKeyCodec::{}: non-zero byte at tail position {} (digest_len={})", what, i, digest_len));
}
}

String SourceEdgeKeyCodec::key(const BlobRef & ref, const UInt128 & source_id)
{
    const uint8_t digest_len = static_cast<uint8_t>(blobHashLenFor(ref.algo));
    checkZeroTailForAlgo(ref.digest, digest_len, "key");
    String out;
    out.push_back(static_cast<char>(static_cast<uint8_t>(ref.algo)));
    out += String(reinterpret_cast<const char *>(ref.digest.bytes.data()), digest_len);
    out += u128ToBytesBE(source_id);
    return out;
}

void SourceEdgeKeyCodec::parse(std::string_view key, BlobRef & ref, UInt128 & source_id)
{
    if (key.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS source-edge run: empty key");
    const uint8_t algo_byte = static_cast<uint8_t>(key[0]);
    BlobHashAlgo algo{};
    switch (algo_byte)
    {
        case static_cast<uint8_t>(BlobHashAlgo::CityHash128): algo = BlobHashAlgo::CityHash128; break;
        case static_cast<uint8_t>(BlobHashAlgo::XXH3_128):    algo = BlobHashAlgo::XXH3_128; break;
        case static_cast<uint8_t>(BlobHashAlgo::Sha256):      algo = BlobHashAlgo::Sha256; break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "CAS source-edge run: unknown algo byte {} in key", algo_byte);
    }
    const uint8_t digest_len = static_cast<uint8_t>(blobHashLenFor(algo));
    if (key.size() != 1 + static_cast<size_t>(digest_len) + 16)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS source-edge run: malformed key ({} bytes, expected {})", key.size(), 1 + digest_len + 16);
    ref = BlobRef{};
    ref.algo = algo;
    memcpy(ref.digest.bytes.data(), key.data() + 1, digest_len);
    source_id = u128FromBytesBE(String(key.substr(1 + digest_len, 16)), "src-edge run key source_id");
}

std::vector<BlobCandidate> zeroInDegree(CasOperation & op, const std::vector<RunRef> & runs)
{
    std::vector<BlobCandidate> result;
    for (const RunRef & run : runs)
    {
        /// The caller passes the exact object key, so a run sealed
        /// for a later generation but physically living under an older key is reached directly. The run is
        /// streamed at O(one block) resident memory, never materialized whole. `openSourceEdgeRun` enforces
        /// the run kind + key schema; `RunMarker::Condemned` sentinel rows are skipped (only `RunMarker::Zero` counts).
        SourceEdgeRunView r = openSourceEdgeRun(op, run.key);
        String k;
        String p;
        while (r.next(k, p))
            if (!p.empty() && runMarkerFromByte(p[0], "CAS source-edge run") == RunMarker::Zero)
            {
                BlobRef bh;
                UInt128 sid;
                /// `parse` throws `CORRUPTED_DATA` on a malformed key; malformed rows must not be silently
                /// treated as absent candidates.
                SourceEdgeKeyCodec::parse(k, bh, sid);
                result.push_back(BlobCandidate{.ref = bh});
            }
        /// Whole-file checksum: verify the drained run against the seal's
        /// RunRef.checksum BEFORE its candidates feed a GC delete decision. Fail-closed on mismatch.
        r.verifyAgainst(run.checksum);
    }
    return result;
}

}
