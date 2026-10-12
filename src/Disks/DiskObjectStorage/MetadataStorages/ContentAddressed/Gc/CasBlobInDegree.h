#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRecordStreamFormat.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// In-memory description of a blob incarnation condemned by the in-degree merge. The exact token and
/// size are captured from the blob HEAD so the GC caller can issue an exact-token deletion later;
/// `condemn_round` controls round-paced graduation. Entries are decoded from `RunMarker::Condemned` rows;
/// the type itself has no serialized representation.
struct RetiredEntry
{
    ObjectKind kind = ObjectKind::Blob;
    BlobRef ref{};
    PersistedEtag token;   /// the exact incarnation GC observed; the delete re-heads and compares it
    uint64_t size = 0;
    uint64_t condemn_round = 0;   /// the GC round that condemned this incarnation (round-paced
                                  /// graduation: an entry graduates only once condemn_round < the
                                  /// current round). Consulted by GC only; the writer never reads it.
    bool delete_pending = false;  /// Two-phase graduation: floor-passed and
                                  /// published for deletion; the NEXT pass executes the exact-token
                                  /// delete (pre-CAS, safe at any leader staleness) and drops the entry.
                                  /// Terminal: a pending entry is never un-pended (writers keep seeing
                                  /// it condemned and recreate).
    bool marker_confirmed = false;   /// Durable `Condemned` meta CONFIRMED for this entry — the
                                     /// graduation gate (triage 2026-07-17 §3.4): the per-hash condemn
                                     /// marker is the writer's adopt gate, so graduation to
                                     /// delete_pending requires confirmed durable evidence; an
                                     /// unconfirmed entry is CARRIED, never fail-open deleted. Set at
                                     /// graduation (delete_pending rows always carry it).
};

/// Backend-independent codec for source-edge keys. A key is `algo` (u8), the digest at that algorithm's
/// native width, and `source_id` (16 bytes, big-endian). The packed byte order is exactly
/// `(BlobRef, source_id)` order, which lets the fold merge compare keys directly. The leading algorithm
/// byte makes the digest width self-describing; supported algorithms may therefore be mixed in one run.
class SourceEdgeKeyCodec
{
public:
    SourceEdgeKeyCodec() = delete;

    /// key = algo(u8) ++ digest[blobHashLenFor(algo)] ++ source_id(16 BE); 33 or 49 bytes.
    static String key(const BlobRef & ref, const UInt128 & source_id);
    /// Parse a key. Throws `NOT_IMPLEMENTED` on an unknown algo byte, `CORRUPTED_DATA` on a wrong
    /// total length for a known algo. Zero-tails the digest (beyond the algo's own width).
    static void parse(std::string_view key, BlobRef & ref, UInt128 & source_id);
};

/// Deterministic 16-byte id of a source edge (ManifestId, path). Distinctness only — not reconstructable.
UInt128 sourceEdgeId(const ManifestId & id, const String & path);

/// The zero source_id is the reserved sentinel key (used internally for the zero-marker row) —
/// producers of real source edges must fail closed on a hash collision with it.
void assertValidSourceEdgeId(const UInt128 & source_id);

/// Serialized payload of a condemned source-edge sentinel. The payload retains the full incarnation,
/// dialect included, because deletion must remain exact-token guarded. Its fixed prefix is
/// `[0x02][flags][token_type][round BE64][size BE64][token_len BE16]`, followed by token bytes;
/// `token_type` is the dialect byte of `CasWireVocab`'s vocabulary.
/// `flags` bit 0 is `delete_pending`, bit 1 is `marker_confirmed`.
struct CondemnedRow
{
    bool delete_pending = false;
    PersistedEtag token;   // the incarnation the exact-token delete must re-observe
    uint64_t size = 0;
    uint64_t condemn_round = 0;
    bool marker_confirmed = false;   // durable Condemned meta confirmed (graduation gate)
    /// Spelled out rather than defaulted because `PersistedEtag` carries no equality of its
    /// own. A field added above belongs here too.
    bool operator==(const CondemnedRow & o) const
    {
        return delete_pending == o.delete_pending
            && token.dialect == o.token.dialect && token.value == o.token.value
            && size == o.size && condemn_round == o.condemn_round
            && marker_confirmed == o.marker_confirmed;
    }
};

/// Encode a condemned-row payload. Throws `CORRUPTED_DATA` if the token cannot fit in its u16 length
/// or the dialect is not one this vocabulary knows.
String encodeCondemnedRow(const CondemnedRow & row);

/// Decode and validate a condemned-row payload. Unknown flags, dialect bytes, or inconsistent lengths
/// throw `CORRUPTED_DATA`.
CondemnedRow decodeCondemnedRow(std::string_view payload);

/// Bridges the backend-free `Formats/CasRecordStreamFormat` NDJSON reader to the
/// `(key, payload)` BYTE interface the `zeroInDegree` / `fsck` consumers use:
/// `next` reconstructs the packed `SourceEdgeKeyCodec` key and the original payload bytes (a single
/// marker byte for an edge / zero row, or the `encodeCondemnedRow` blob for a condemned row) from the
/// decoded NDJSON record. So the codec stays backend-free while the consumers keep their exact parse /
/// compare logic. The whole-object chained CityHash128 is accumulated as the run streams; `verifyAgainst`
/// checks it against the fold seal's `RunRef.checksum` AFTER the run is fully drained and BEFORE the
/// caller acts on it (a deletion decision).
class SourceEdgeRunView
{
public:
    /// false once the run's `{"n"}` trailer is consumed (the trailer count is verified there). `key` is
    /// the reconstructed `SourceEdgeKeyCodec::key(ref, source_id)`; `payload` is the original marker byte
    /// or `encodeCondemnedRow` bytes.
    bool next(String & key, String & payload);
    /// Verify the accumulated whole-file checksum against the seal's `RunRef.checksum`; CORRUPTED_DATA on
    /// mismatch. Call after draining the run and before acting on its records.
    void verifyAgainst(const UInt128 & expected);
    /// The accumulated whole-file checksum after draining the run — non-throwing, for a read-only auditor
    /// (fsck) that catalogues a mismatch and continues instead of failing closed. Call after draining.
    UInt128 accumulatedChecksum();

private:
    friend SourceEdgeRunView openSourceEdgeRun(std::string_view bytes);
    friend SourceEdgeRunView openSourceEdgeRun(CasOperation & op, const String & key);
    /// Keep the underlying stream alive for the reader, which borrows it rather than owning it.
    explicit SourceEdgeRunView(std::unique_ptr<ReadBuffer> stream_);

    std::unique_ptr<ReadBuffer> stream;             /// owns the backend stream / memory buffer the reader borrows
    std::unique_ptr<SourceEdgeRunReader> reader;    /// over *stream (non-movable => held by pointer, destroyed before stream)
};

/// Open a typed source-edge run. The NDJSON header must identify a `cas_run` of kind `source_edge`;
/// otherwise opening fails closed. The memory overload borrows caller-owned bytes. The backend overload
/// streams the write-once object: the reader itself holds one record-sized buffer, but the open stream
/// underneath it buffers on its own terms, unbounded and unmeasured here.
SourceEdgeRunView openSourceEdgeRun(std::string_view bytes);
SourceEdgeRunView openSourceEdgeRun(CasOperation & op, const String & key);

/// A blob whose active source-edge set became empty this generation — a retire candidate.
struct BlobCandidate
{
    BlobRef ref{};
};

/// Cap on the ref-log GET/decode work the orphan sweep's committed-tail recovery walk may spend
/// (`activeManifestKeys` in `CasOrphanManifestSweep.cpp`). `0` is unbounded.
struct GcRoundWorkBudget
{
    uint64_t max_sweep_recovery_ops = 0;
    uint64_t sweep_recovery_ops_used = 0;

    bool sweepRecoveryOpAvailable() const { return max_sweep_recovery_ops == 0 || sweep_recovery_ops_used < max_sweep_recovery_ops; }
};

/// Stream the sealed in-degree runs named by `runs` (the current seal's `blob_target_runs` filtered to one
/// shard) and return every blob written at in-degree 0 (the candidates that transitioned to zero). An
/// empty `runs` is an empty baseline. Each `RunRef` supplies the exact object key, so resolution never
/// reconstructs a key from generation metadata.
std::vector<BlobCandidate> zeroInDegree(CasOperation & op, const std::vector<RunRef> & runs);

}
