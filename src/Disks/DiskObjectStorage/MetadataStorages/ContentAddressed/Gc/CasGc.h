#pragma once
#include <base/types.h>
#include <cstdint>
#include <functional>
#include <map>

namespace DB::Cas
{

/// What one round of `Cas::KeeperGc` did.
struct RoundReport
{
    bool writer = false;      /// this server writes every table on the disk: the round ran; otherwise it read only records
    uint64_t round = 0;       /// this process's round number (0 when not the writer)
    uint64_t deleted = 0;     /// blobs deleted, exact ETag
    uint64_t absent = 0;      /// candidate blobs already gone when deleted
    uint64_t replaced = 0;    /// candidate blobs re-uploaded since their ETag was read: kept
    uint64_t spared = 0;      /// candidate blobs a record references again
};

/// One phase of one GC round. Pure data with no Interpreters dependency -- the same discipline
/// `GcRoundLogRecord` follows, so the round engine can be instrumented without linking the system-log
/// machinery. `CasGcScheduler` converts it into one `Phase` row of
/// `system.cas_gc_log`.
///
/// DELIBERATELY NO VERB COLUMNS. `profile_events` is this phase's delta of the ordinary process
/// counters, so `ProfileEvents['S3ListObjects']` on a `Phase` row answers "which phase burns the LIST
/// budget" with no schema invention; `metrics` carries only the semantic counts a phase computes for
/// itself and no counter can supply.
struct GcPhaseRecord
{
    String phase;
    UInt64 duration_us = 0;
    std::map<String, UInt64> metrics;
    std::map<String, UInt64> profile_events;   /// this phase's ProfileEvents delta
};

/// Where a phase record goes. Installed for the duration of one round by `CasGcScheduler`; empty
/// elsewhere (a unit test driving `KeeperGc` directly emits nothing).
using GcPhaseSink = std::function<void(const GcPhaseRecord &)>;

}
