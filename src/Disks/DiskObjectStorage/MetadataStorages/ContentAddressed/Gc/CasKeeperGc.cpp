#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasKeeperGc.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGcPhaseTimer.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include <chrono>
#include <set>
#include <unordered_map>

namespace ProfileEvents
{
    extern const Event CASGCRetiredRedeleted;
    extern const Event CASGCRetireReplaced;
}

namespace DB::Cas
{

BlobRef keeperBlobRef(const CasKeeperBlobFile & file)
{
    return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(file.hash)};
}

namespace
{

uint64_t wallMs()
{
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

/// One record of one source, as the round handles it.
struct Entry
{
    size_t source = 0;
    const CasKeeperRecordState * record = nullptr;
};

}

KeeperGc::KeeperGc(PoolPtr store_, KeeperRecordTablesFn tables_, LoggerPtr log_)
    : store(std::move(store_))
    , tables(std::move(tables_))
    , log(std::move(log_))
{
}

void KeeperGc::runRound(RoundReport & report)
{
    report = RoundReport{};

    std::optional<KeeperRecordTables> read;
    {
        GcPhaseTimer t(phase_sink, "keeper_records");
        read = tables();
        if (!read)
        {
            t.metric("unreadable", 1);
            LOG_INFO(log, "CAS GC: some table on the disk cannot be read this round (not loaded yet, or not "
                     "replicated); removing nothing");
            return;
        }
        size_t records = 0;
        for (const auto & table : *read)
        {
            if (!table.snapshot.writer)
            {
                t.metric("not_writer", 1);
                return;   /// only the writer deletes
            }
            records += table.snapshot.records.size();
        }
        t.metric("tables", read->size());
        t.metric("records", records);
        /// The size of what this server holds in memory (`BlobRefCount`).
        const auto held = store->blobRefs()->stats();
        t.metric("held_blobs", held.blobs);
        t.metric("held_references", held.references);
    }
    if (read->empty())
        return;
    const KeeperRecordTables & tbls = *read;
    report.writer = true;
    report.round = ++rounds;

    std::vector<Entry> unlisted;
    std::unordered_map<BlobRef, size_t, BlobRefHash> recorded_refs;
    for (size_t s = 0; s < tbls.size(); ++s)
        for (const auto & record : tbls[s].snapshot.records)
        {
            if (!record.listed)
                unlisted.push_back({s, &record});
            for (const auto & f : record.blobs)
                ++recorded_refs[keeperBlobRef(f)];
        }

    /// A pending candidate a record references again is live: it leaves the pipeline.
    for (auto it = pending.begin(); it != pending.end();)
    {
        if (recorded_refs.contains(*it))
        {
            it = pending.erase(it);
            ++report.spared;
        }
        else
            ++it;
    }

    /// Mark the records found unlisted for the first time; collect the marked ones whose retention passed.
    const uint64_t now_ms = wallMs();
    std::vector<Entry> removable;
    {
        GcPhaseTimer t(phase_sink, "mark");
        std::vector<std::vector<const CasKeeperRecordState *>> to_mark(tbls.size());
        std::set<MarkedRecord> still_marked;
        for (const Entry & e : unlisted)
        {
            /// Its retention counts from a mark this process wrote while the part was unlisted (`marked_records`).
            MarkedRecord key{tbls[e.source].source.get(), e.record->part};
            if (!marked_records.contains(key))
                to_mark[e.source].push_back(e.record);
            else
            {
                if (now_ms >= static_cast<uint64_t>(e.record->mtime_ms) + tbls[e.source].snapshot.retention_ms)
                    removable.push_back(e);
                still_marked.insert(std::move(key));
            }
        }
        size_t marked = 0;
        for (size_t s = 0; s < tbls.size(); ++s)
            if (!to_mark[s].empty())
                for (auto & part : tbls[s].source->markCasRecords(to_mark[s]))
                {
                    still_marked.emplace(tbls[s].source.get(), std::move(part));
                    ++marked;
                }
        marked_records = std::move(still_marked);
        t.metric("marked", marked);
        t.metric("due", removable.size());
    }

    GcPhaseTimer t(phase_sink, "remove");
    std::vector<std::vector<const CasKeeperRecordState *>> to_remove(tbls.size());
    for (const Entry & e : removable)
        to_remove[e.source].push_back(e.record);
    std::set<std::pair<size_t, String>> removed;
    for (size_t s = 0; s < tbls.size(); ++s)
        if (!to_remove[s].empty())
            for (auto & part : tbls[s].source->removeCasRecords(tbls[s].snapshot, to_remove[s]))
                removed.emplace(s, std::move(part));
    for (const Entry & e : removable)
        if (removed.contains({e.source, e.record->part}))
            for (const auto & f : e.record->blobs)
                --recorded_refs[keeperBlobRef(f)];
    for (const Entry & e : removable)
        if (removed.contains({e.source, e.record->part}))
            for (const auto & f : e.record->blobs)
                if (recorded_refs[keeperBlobRef(f)] == 0)
                    pending.insert(keeperBlobRef(f));

    /// The delete half of `BlobRefCount`'s invariant: ETag first, then the count, then the exact delete.
    const auto & refs = store->blobRefs();
    CasOperation op = store->openRequests().admit();
    for (auto it = pending.begin(); it != pending.end();)
    {
        const BlobRef ref = *it;
        const String key = store->layout().blobKey(ref);
        const std::optional<Meta> observed = op.head(key, Retry::standard());
        if (observed && !refs->unreferencedNow(ref))
        {
            ++it;   /// a part this server holds still references it: a later round
            continue;
        }
        const Removal del = observed ? op.remove(key, observed->etag, Retry::standard()) : Removal::Gone;
        switch (del)
        {
            case Removal::Removed: ++report.deleted; ProfileEvents::increment(ProfileEvents::CASGCRetiredRedeleted); break;
            case Removal::Gone: ++report.absent; break;
            case Removal::Mismatch: ++report.replaced; ProfileEvents::increment(ProfileEvents::CASGCRetireReplaced); break;
        }
        it = pending.erase(it);
    }
    t.metric("records_removed", removed.size());
    t.metric("blobs_deleted", report.deleted);
    t.metric("blobs_absent", report.absent);
    t.metric("blobs_replaced", report.replaced);
    t.metric("blobs_pending", pending.size());
}

}
