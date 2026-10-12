#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcMetaWriter.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

namespace DB::Cas
{

/// One table on the disk and its records, read this round.
struct KeeperRecordTable
{
    std::shared_ptr<ICasKeeperRecordSource> source;
    CasKeeperRecordSnapshot snapshot;
};
using KeeperRecordTables = std::vector<KeeperRecordTable>;
/// Reads the records of every table on the disk; nullopt when some table on it cannot be read this round.
using KeeperRecordTablesFn = std::function<std::optional<KeeperRecordTables>()>;

/// The pool's GC, derived from the Keeper part records of the tables on it (`ICasKeeperRecordSource`).
///
/// LIVENESS: a blob is live while some record references it. A record outlives the removal of its part
/// from the replicas: it is the persistent delete queue.
///
/// ONE ROUND, run only on the writer (every table on this disk is written by this server):
///  1. read every table's records (one batched read per table);
///  2. a record whose part no replica lists is MARKED: re-`set` with its own bytes, version-checked, so its
///     mtime says when it was found unlisted; its blobs that no listed part references are CONDEMNED:
///     the blob's ETag is observed and its `.meta` set to `Condemned`, so a writer that adopts the blob
///     from then on re-uploads it (new ETag) instead;
///  3. a marked record whose part is still unlisted `retention_ms` (`old_parts_lifetime`) after its mark is
///     removed by its table in one multi-op that checks no replica lists the part and the writer epoch
///     holds; a part listed again in between fails the check and the cycle restarts;
///  4. then each condemned blob no remaining record references is deleted, exact-ETag: a blob a writer
///     re-uploaded since its condemnation (different ETag) is spared.
///
/// A blob is deleted only once every build of this server that existed when it was condemned has retired
/// (`Pool::minActive` passed the build sequence at condemnation): a build that adopted it before the marker
/// either committed -- its part has a record, read in step 1 -- or was abandoned. A build that started
/// after the marker saw `Condemned` and re-uploaded.
///
/// Nothing is deleted while a listed part has no record (its blobs are unknown). A crash between a record
/// removal and the blob deletes leaks those blobs (`.meta = Condemned`, no record), never loses one.
/// Condemnations and marks are kept in memory: after a restart a blob is condemned, and a record marked,
/// again before anything is deleted.
class KeeperGc
{
public:
    KeeperGc(PoolPtr store_, KeeperRecordTablesFn tables_, LoggerPtr log_);

    void runRound(RoundReport & report);

    /// Blobs condemned and not yet deleted or spared.
    size_t pendingCount() const { return condemned.size(); }

    void setPhaseSink(GcPhaseSink sink) { phase_sink = std::move(sink); }

private:
    struct Condemned
    {
        std::optional<PersistedEtag> token;   /// none: the blob was absent when observed
        uint64_t build_cut = 0;               /// `peekNextBuildSeq` once the marker was durable
    };

    /// Observe each blob of `refs` not yet condemned and condemn it (`.meta = Condemned`). Returns how many.
    size_t condemn(CasOperation & op, const std::map<BlobRef, UInt64> & refs, uint64_t round);

    const PoolPtr store;
    const KeeperRecordTablesFn tables;
    const LoggerPtr log;
    GcPhaseSink phase_sink;
    std::unique_ptr<GcMetaWriter> meta_writer;
    MountObservationMap mount_obs;
    std::map<BlobRef, Condemned> condemned;
    /// The records this process marked while their part was unlisted, and that are unlisted still: a record
    /// not in it (marked by an earlier process, or listed again since its mark) is marked anew, which restarts
    /// its retention. A heuristic for re-listing between rounds; a restart redoes the marks.
    using MarkedRecord = std::pair<const ICasKeeperRecordSource *, String>;
    std::set<MarkedRecord> marked_records;
    uint64_t rounds = 0;
};

}
