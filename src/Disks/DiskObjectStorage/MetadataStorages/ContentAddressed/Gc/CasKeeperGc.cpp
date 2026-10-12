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
    extern const Event CASGCRetiredCondemned;
    extern const Event CASGCRetiredSpared;
    extern const Event CASGCRetiredRedeleted;
    extern const Event CASGCRetireReplaced;
    extern const Event CASGCHeartbeatFenceOuts;
}

namespace DB::Cas
{

namespace
{

BlobRef blobRefOf(const CasKeeperBlobFile & f)
{
    return BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(f.hash)};
}

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
    , meta_writer(std::make_unique<GcMetaWriter>(store, log, static_cast<size_t>(store->poolConfig().gc_meta_pool_size)))
{
}

size_t KeeperGc::condemn(CasOperation & op, const std::map<BlobRef, UInt64> & refs, uint64_t round)
{
    std::vector<std::pair<BlobRef, std::optional<PersistedEtag>>> observed_refs;
    for (const auto & [ref, size] : refs)
    {
        if (condemned.contains(ref))
            continue;
        const std::optional<Meta> observed = op.head(store->layout().blobKey(ref), Retry::standard());
        if (!observed)
        {
            observed_refs.emplace_back(ref, std::nullopt);   /// nothing to delete, nothing to mark
            continue;
        }
        const PersistedEtag token = PersistedEtag::capture(observed->etag);
        meta_writer->scheduleCondemnMarkerWrite(ref, token, round, size);
        observed_refs.emplace_back(ref, token);
    }
    meta_writer->drain();
    /// Taken once every marker is durable: a build with this sequence or later observes `Condemned`.
    const uint64_t cut = store->peekNextBuildSeq();
    size_t count = 0;
    for (const auto & [ref, token] : observed_refs)
    {
        /// A refused marker write leaves the pair unconfirmed: the blob is condemned again next round.
        if (token && !meta_writer->condemnMarkerConfirmedInProcess(ref, *token))
            continue;
        condemned[ref] = Condemned{token, cut};
        ++count;
    }
    ProfileEvents::increment(ProfileEvents::CASGCRetiredCondemned, count);
    return count;
}

void KeeperGc::runRound(RoundReport & report)
{
    report = RoundReport{};
    SCOPE_EXIT({ meta_writer->drainOnExitNoThrow(); });

    /// Read BEFORE the records: a build retired by now has its record in the snapshot below if it committed.
    const uint64_t build_floor = store->minActive();

    std::optional<KeeperRecordTables> read;
    size_t unrecorded = 0;
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
            unrecorded += table.snapshot.unrecorded_listed;
            records += table.snapshot.records.size();
        }
        t.metric("tables", read->size());
        t.metric("records", records);
        t.metric("unrecorded_listed", unrecorded);
    }
    if (read->empty())
        return;
    const KeeperRecordTables & tbls = *read;
    report.writer = true;
    report.round = ++rounds;

    CasOperation op = store->openRequests().admit();

    /// One LIST of `gc/server-roots/` and the fence-out of mounts whose lease stopped renewing.
    {
        GcPhaseTimer t(phase_sink, "heartbeat_floor");
        const HeartbeatFloor floor = computeHeartbeatFloor(
            op, store->layout(), wallMs(), store->bootMsNow(),
            mountObservationThresholdMs(
                static_cast<uint64_t>(store->poolConfig().mount_lease_ttl_ms.count()),
                static_cast<uint64_t>(store->poolConfig().mount_renew_period.count())),
            mount_obs);
        report.fence_outs = floor.fenced_now;
        ProfileEvents::increment(ProfileEvents::CASGCHeartbeatFenceOuts, floor.fenced_now);
        t.metric("fenced_now", floor.fenced_now);
    }

    std::vector<Entry> unlisted;
    std::unordered_map<BlobRef, size_t, BlobRefHash> listed_refs;
    for (size_t s = 0; s < tbls.size(); ++s)
        for (const auto & record : tbls[s].snapshot.records)
        {
            if (!record.listed)
            {
                unlisted.push_back({s, &record});
                continue;
            }
            for (const auto & f : record.blobs)
                ++listed_refs[blobRefOf(f)];
        }

    /// A condemned blob a listed part references again is live: it leaves the pipeline. Its `.meta` stays
    /// `Condemned` until a writer re-uploads it, as with the previous GC.
    for (auto it = condemned.begin(); it != condemned.end();)
    {
        if (listed_refs.contains(it->first))
        {
            if (it->second.token)
                meta_writer->forgetCondemnMarker(it->first, *it->second.token);
            it = condemned.erase(it);
            ++report.spared;
            ProfileEvents::increment(ProfileEvents::CASGCRetiredSpared);
        }
        else
            ++it;
    }

    /// Mark the records found unlisted for the first time and condemn the blobs only they reference.
    const uint64_t now_ms = wallMs();
    std::vector<Entry> removable;
    {
        GcPhaseTimer t(phase_sink, "mark");
        std::vector<std::vector<const CasKeeperRecordState *>> to_mark(tbls.size());
        std::map<BlobRef, UInt64> to_condemn;
        std::set<MarkedRecord> still_marked;
        for (const Entry & e : unlisted)
        {
            /// Its retention counts from a mark this process wrote while the part was unlisted (`marked_records`).
            MarkedRecord key{tbls[e.source].source.get(), e.record->part};
            if (!marked_records.contains(key))
            {
                to_mark[e.source].push_back(e.record);
                for (const auto & f : e.record->blobs)
                    if (!listed_refs.contains(blobRefOf(f)))
                        to_condemn.emplace(blobRefOf(f), f.size);
            }
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
        report.condemned = condemn(op, to_condemn, report.round);
        t.metric("marked", marked);
        t.metric("condemned", report.condemned);
        t.metric("due", removable.size());
    }

    if (removable.empty())
        return;
    if (unrecorded > 0)
    {
        LOG_INFO(log, "CAS GC round {}: {} listed parts have no Keeper record, so their blobs are unknown; "
                 "removing nothing ({} records due)", report.round, unrecorded, removable.size());
        return;
    }

    GcPhaseTimer t(phase_sink, "remove");
    /// The blobs a record about to be removed references that no record staying references. A record goes
    /// only when each of those blobs is condemned and every build that existed then has retired; otherwise
    /// it stays (condemned now if it was not), which can make other blobs it shares stay as well.
    std::unordered_map<BlobRef, size_t, BlobRefHash> kept_refs = listed_refs;
    for (const Entry & e : unlisted)
        for (const auto & f : e.record->blobs)
            ++kept_refs[blobRefOf(f)];
    for (const Entry & e : removable)
        for (const auto & f : e.record->blobs)
            --kept_refs[blobRefOf(f)];
    std::map<BlobRef, UInt64> uncondemned;
    for (bool changed = true; changed;)
    {
        changed = false;
        for (auto it = removable.begin(); it != removable.end();)
        {
            bool ready = true;
            for (const auto & f : it->record->blobs)
            {
                const BlobRef ref = blobRefOf(f);
                if (kept_refs[ref] > 0)
                    continue;
                const auto c = condemned.find(ref);
                if (c == condemned.end())
                    uncondemned.emplace(ref, f.size);
                if (c == condemned.end() || c->second.build_cut > build_floor)
                    ready = false;
            }
            if (ready)
            {
                ++it;
                continue;
            }
            for (const auto & f : it->record->blobs)
                ++kept_refs[blobRefOf(f)];
            it = removable.erase(it);
            changed = true;
        }
    }
    /// Condemned now, they hold their records back until a later round (a blob observed absent enters with no
    /// token: nothing to delete, and a later upload of it shows as replaced).
    report.condemned += condemn(op, uncondemned, report.round);

    std::vector<std::vector<const CasKeeperRecordState *>> to_remove(tbls.size());
    for (const Entry & e : removable)
        to_remove[e.source].push_back(e.record);
    std::set<std::pair<size_t, String>> removed;
    for (size_t s = 0; s < tbls.size(); ++s)
        if (!to_remove[s].empty())
            for (auto & part : tbls[s].source->removeCasRecords(tbls[s].snapshot, to_remove[s]))
                removed.emplace(s, std::move(part));

    std::map<BlobRef, UInt64> dead;
    for (const Entry & e : removable)
    {
        if (removed.contains({e.source, e.record->part}))
            continue;
        for (const auto & f : e.record->blobs)   /// listed again, or its removal failed: it stays
            ++kept_refs[blobRefOf(f)];
    }
    for (const Entry & e : removable)
        if (removed.contains({e.source, e.record->part}))
            for (const auto & f : e.record->blobs)
                if (kept_refs[blobRefOf(f)] == 0)
                    dead.emplace(blobRefOf(f), f.size);

    for (const auto & [ref, size] : dead)
    {
        const auto c = condemned.find(ref);
        const String key = store->layout().blobKey(ref);
        const std::optional<Meta> observed = op.head(key, Retry::standard());
        Removal del = Removal::Gone;
        if (observed)
            del = c->second.token && c->second.token->matches(observed->etag)
                ? op.remove(key, observed->etag, Retry::standard())
                : Removal::Mismatch;   /// re-uploaded since its condemnation (or uploaded after it was absent)
        switch (del)
        {
            case Removal::Removed: ++report.deleted; ProfileEvents::increment(ProfileEvents::CASGCRetiredRedeleted); break;
            case Removal::Gone: ++report.absent; break;
            case Removal::Mismatch: ++report.replaced; ProfileEvents::increment(ProfileEvents::CASGCRetireReplaced); break;
        }
        if (del != Removal::Mismatch)
            meta_writer->scheduleConfirmedMetaDelete(ref);
        if (c->second.token)
            meta_writer->forgetCondemnMarker(ref, *c->second.token);
        condemned.erase(c);
    }
    meta_writer->drain();
    t.metric("records_removed", removed.size());
    t.metric("blobs_deleted", report.deleted);
    t.metric("blobs_absent", report.absent);
    t.metric("blobs_replaced", report.replaced);
}

}
