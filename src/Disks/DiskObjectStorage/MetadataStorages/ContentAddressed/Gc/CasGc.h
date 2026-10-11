#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <cstdint>
#include <functional>
#include <map>
#include <vector>

namespace DB::Cas
{

/// One anomaly the fold surfaced (a clamped cursor — a missing committed/removal body, or the fold
/// barrier on a live missing-body precommit). Surfaced to fsck/logs; recording an anomaly never throws:
/// the round records the problem and continues, while the affected shard remains conservatively clamped.
struct RoundAnomaly
{
    RootNamespace ns;
    uint64_t shard = 0;
    ManifestId id;
    String reason;
};

/// Counters and diagnostics returned by one GC round (`KeeperGc::runRound`).
struct RoundReport
{
    bool acquired_lease = false;  /// false => another leader is alive; nothing else was done
    /// True iff this round deferred (re-adopted the sealed
    /// in-degree generation instead of folding). A deferred round performs no fold, no pre-CAS
    /// deletes, and no gc/state CAS -- every other RoundReport field below is meaningless/zero on it,
    /// EXCEPT `round` (below).
    bool deferred = false;
    uint64_t round = 0;
    uint64_t candidates = 0;      /// retired entries WRITTEN this round (absent candidates are skipped)
    uint64_t deleted = 0;
    uint64_t absent = 0;
    uint64_t replaced = 0;        /// 412-saves — a health metric
    uint64_t spared = 0;
    uint64_t manifests_deleted = 0;  /// owner-removed manifest bodies deleted, distinct from blob deletes
    /// Retired-cursor pipeline observability: the pipeline's per-round transitions.
    size_t condemned = 0;         /// entries newly condemned into the retired list this round
    size_t graduated = 0;         /// entries newly floor-passed (published delete_pending) this round
    size_t redeleted = 0;         /// pending deletes executed this round (exact-token blob deletes)
    size_t fence_outs = 0;        /// expired mounts fenced-out by the round's heartbeat floor
    std::vector<RoundAnomaly> anomalies;   /// fold clamps surfaced this round (never wedge the round)

    /// Retire-pipeline REMAINING sizes as of the gc/state this round's single CAS just published --
    /// unlike `condemned`/`graduated`/`redeleted` above (this round's DELTAs), these are the pipeline's
    /// current outstanding totals, summed over every gc-shard's sealed `CondemnedSummary`
    /// (`Formats/CasFoldSealFormat.h`). `pending_retired` = still delete_pending (graduated, awaiting the
    /// exact-token delete next pass); `pending_candidates` = condemned but not yet floor-passed;
    /// `pending_condemned` = their total (candidates + retired), the overall pipeline gauge. Left at 0 on
    /// a `!acquired_lease`/`deferred` round -- those flags already mark the row non-authoritative.
    size_t pending_candidates = 0;
    size_t pending_condemned = 0;
    size_t pending_retired = 0;
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
