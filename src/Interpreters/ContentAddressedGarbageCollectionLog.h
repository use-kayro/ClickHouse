#pragma once
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct ContentAddressedGarbageCollectionLogElement
{
    /// `PHASE`: one row per GC phase, emitted between the round's `START` and `FINISH` and correlated
    /// with them by `round_id`.
    enum EventType : int8_t { START = 1, FINISH = 2, PHASE = 3 };
    /// `SUCCESS`: the round completed; `writer` says whether it ran as the writer or only read records.
    /// `ABORTED`: the round threw an exception whose code names a transient condition (backend or Keeper
    /// unavailability); the next scheduled round retries it.
    /// `STOPPED`: a transient failure observed after the disk's teardown began -- the round was cut
    /// short by a server shutdown or the storage's destructor, so neither had to wait for it;
    /// `error` carries the engine's refusal. Decommission does not arm that flag and cannot produce
    /// this outcome.
    /// `FAILED` is everything else -- fail-closed, an unclassified error reads as real.
    enum Outcome   : int8_t { UNKNOWN = 1, SUCCESS = 2, FAILED = 4, ABORTED = 6, STOPPED = 7 };
    enum Trigger   : int8_t { SCHEDULED = 1, MANUAL = 2 };

    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;

    EventType event_type = START;
    String disk_name;
    String srid;         /// server_root_id of the mount whose GC scheduler ran this round
    Trigger trigger = SCHEDULED;

    UInt64 round = 0;
    Outcome outcome = UNKNOWN;      /// UNKNOWN on START
    /// The `Cas::RoundReport` of a FINISH row.
    bool writer = false;
    UInt64 objects_condemned = 0;
    UInt64 objects_deleted = 0;
    UInt64 objects_absent = 0;
    UInt64 objects_replaced = 0;
    UInt64 objects_spared = 0;
    UInt64 fence_outs = 0;
    UInt64 duration_ms = 0;
    String error;
    Int32 error_code = 0;           /// exception code on an Aborted/Error FINISH; 0 otherwise
    std::map<String, UInt64> profile_events;   /// per-round delta (FINISH); per-phase delta (PHASE)

    String round_id;                           /// correlator for every row of one round attempt
    String phase;                              /// empty on START/FINISH
    UInt64 phase_duration_microseconds = 0;              /// PHASE rows only
    std::map<String, UInt64> phase_metrics;    /// PHASE rows only

    static std::string name() { return "ContentAddressedGarbageCollectionLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ContentAddressedGarbageCollectionLog : public SystemLog<ContentAddressedGarbageCollectionLogElement>
{
    using SystemLog<ContentAddressedGarbageCollectionLogElement>::SystemLog;
};

}
