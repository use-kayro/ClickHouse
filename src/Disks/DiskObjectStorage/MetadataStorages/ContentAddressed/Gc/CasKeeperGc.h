#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <vector>

namespace DB::Cas
{

/// The blob a Keeper record's checksummed file names: `{ch128, its MergeTree file hash}`.
BlobRef keeperBlobRef(const CasKeeperBlobFile & file);

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
/// LIVENESS: a blob is live while some record references it, or while this server holds it in memory (a
/// part it holds, a part under construction: `BlobRefCount`). A record outlives the removal of its part from
/// the replicas: it is the persistent delete queue.
///
/// ONE ROUND, run only on the writer (every table on this disk is written by this server, and only this
/// server writes blobs):
///  1. read every table's records (one batched read per table);
///  2. a record whose part no replica lists is MARKED: re-`set` with its own bytes, version-checked, so its
///     mtime says when it was found unlisted;
///  3. a marked record whose part is still unlisted `retention_ms` (`old_parts_lifetime`) after its mark is
///     removed by its table in one multi-op that checks no replica lists the part and the writer epoch
///     (`is_active`/`host` versions) holds; a part listed again in between fails the check;
///  4. each blob a removed record referenced that no remaining record references is a candidate. A candidate
///     is deleted only like this: its ETag is read FIRST, then `BlobRefCount` must say nothing on this server
///     references it, then the delete is exact on that ETag. A build that claimed the blob after the check saw
///     it unreferenced and re-uploaded it under a fresh envelope, so the exact delete finds another ETag (or
///     lands first and the upload recreates it). A candidate still held in memory stays pending to a later
///     round; one a record references again leaves the pipeline.
///
/// No object-store metadata: no `.meta` marker, no heartbeat floor, no LIST (TEC-851 step 3c). The pending
/// candidates live in memory only: a writer restart between a record removal and the deletes leaks them
/// (at most `blobs_pending` of the last `cas_gc_log` round plus the blobs of the records that round removed),
/// never loses a blob.
class KeeperGc
{
public:
    KeeperGc(PoolPtr store_, KeeperRecordTablesFn tables_, LoggerPtr log_);

    /// One round. `report.writer` is true when this server ran it as the writer.
    void runRound(RoundReport & report);

    /// Candidates a part this server holds still references.
    size_t pendingCount() const { return pending.size(); }

    void setPhaseSink(GcPhaseSink sink) { phase_sink = std::move(sink); }

private:
    const PoolPtr store;
    const KeeperRecordTablesFn tables;
    const LoggerPtr log;
    GcPhaseSink phase_sink;
    std::set<BlobRef> pending;
    /// The records this process marked while their part was unlisted, and that are unlisted still.
    using MarkedRecord = std::pair<const ICasKeeperRecordSource *, String>;
    std::set<MarkedRecord> marked_records;
    uint64_t rounds = 0;
};

}
