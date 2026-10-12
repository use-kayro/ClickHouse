#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <string_view>

namespace DB::Cas
{

/// The durable lease portion of the `gc/state` control object. `owner` identifies the current GC
/// leader and `seq` changes when that leader renews the lease. A contender compares the pair across
/// observations before stealing a stalled lease; a zero owner means that the lease has never been
/// held.
struct GcLease
{
    UInt128 owner{};  /// Random GC leader identifier; zero means the lease has never been held.
    uint64_t seq = 0; /// Renewal counter observed by contenders when deciding whether a lease stalled.
};

/// The `gc/state` control object the S3-listing GC round wrote; nothing writes it any more. It is
/// materialized as one versioned JSON control object by `encodeGcState` and read back by
/// `decodeGcState`. `gc_shards` must be at least one; decoders reject an absent or zero value instead
/// of silently accepting the C++ default.
struct GcState
{
    uint64_t round = 0;            /// the highest GC round whose retire sets are durable
    uint64_t gc_shards = 1;        /// GC blob-target-shard count; set once, immutable; must be >= 1
    uint64_t snap_generation = 0;  /// monotone; the authoritative snap objects' generation
    uint64_t snap_pruned_through = 0;   /// highest snap generation fully pruned (retention cursor)
    uint64_t snap_attempt = 0;     /// adopted attempt id (folding leader's lease.seq) for snap_generation
    String manifest_sweep_cursor;  /// best-effort orphan part-manifest cleanup cursor; reachability ignores it
    GcLease lease;
};

/// Encode `state` as the canonical `cas_gc_state` text object: a versioned header line followed by
/// one JSON body object. The writer always emits the complete field set and asserts the invariant
/// that `gc_shards` is nonzero.
String encodeGcState(const GcState & state);

/// Decode a complete `cas_gc_state` text object. The header and size limits are checked before the
/// body is parsed; unknown non-reserved fields are tolerated for forward evolution, but malformed
/// input, trailing bytes, a missing `gc_shards`, or a zero shard count raises `CORRUPTED_DATA` rather than
/// falling back to a default state.
GcState decodeGcState(std::string_view data);

}
