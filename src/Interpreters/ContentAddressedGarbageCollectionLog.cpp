#include <Interpreters/ContentAddressedGarbageCollectionLog.h>
#include <base/getFQDNOrHostName.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/DateLUTImpl.h>

namespace DB
{

ColumnsDescription ContentAddressedGarbageCollectionLogElement::getColumnsDescription()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Start", static_cast<Int8>(START)}, {"Finish", static_cast<Int8>(FINISH)},
        {"Phase", static_cast<Int8>(PHASE)}});
    auto outcome_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Unknown", static_cast<Int8>(UNKNOWN)}, {"Success", static_cast<Int8>(SUCCESS)},
        {"Error", static_cast<Int8>(FAILED)}, {"Aborted", static_cast<Int8>(ABORTED)},
        {"Stopped", static_cast<Int8>(STOPPED)}});
    auto trigger_enum = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"Scheduled", static_cast<Int8>(SCHEDULED)}, {"Manual", static_cast<Int8>(MANUAL)}});
    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"hostname", lc_string, "Host name of the server executing the round."},
        {"event_date", std::make_shared<DataTypeDate>(), "Event date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Event time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds."},
        {"event_type", type_enum, "Start or Finish of a GC round, or one Phase of it."},
        {"disk_name", lc_string, "Content-addressed disk the round ran on."},
        {"server_root_id", lc_string, "Identifies the mount whose GC scheduler ran this round. Distinguishes concurrent mounters of the same shared pool; join on this column when correlating rounds against `system.cas_mounts`."},
        {"trigger", trigger_enum, "Scheduled (background tick) or Manual (SYSTEM command)."},
        {"round", std::make_shared<DataTypeUInt64>(), "This process's round number on this disk (0 on Start, and on a round that did not run as the writer)."},
        {"outcome", outcome_enum, "Unknown (Start) / Success (the round completed) / Aborted (the round threw a transient error -- backend or Keeper unavailability -- and the next scheduled round retries) / Stopped (a transient error observed after the disk\'s teardown began: the round was cut short by a server shutdown or the storage\'s destructor; a correlation, not a cause) / Error (the round threw a non-transient error -- during a teardown too)."},
        {"writer", std::make_shared<DataTypeUInt8>(), "1 when the round ran as the writer (this server writes every table on the disk; only the writer deletes); 0 when it only read the tables\' records."},
        {"objects_condemned", std::make_shared<DataTypeUInt64>(), "Blobs condemned this round (`.meta = Condemned`): referenced only by records of parts no replica lists."},
        {"objects_deleted", std::make_shared<DataTypeUInt64>(), "Blobs deleted this round (exact ETag), after their last record was removed."},
        {"objects_absent", std::make_shared<DataTypeUInt64>(), "Condemned blobs already gone when deleted."},
        {"objects_replaced", std::make_shared<DataTypeUInt64>(), "Condemned blobs re-uploaded since their condemnation (ETag changed), so spared."},
        {"objects_spared", std::make_shared<DataTypeUInt64>(), "Condemned blobs a listed part references again."},
        {"fence_outs", std::make_shared<DataTypeUInt64>(), "Expired mounts fenced out by this round's heartbeat floor."},
        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Round wall-clock duration (Finish)."},
        {"error", std::make_shared<DataTypeString>(), "Exception text when outcome = Aborted, Stopped or Error. On a Stopped row it names the engine\'s refusal, not the teardown."},
        {"error_code", std::make_shared<DataTypeInt32>(), "Exception code when outcome = Aborted, Stopped or Error; 0 otherwise. The structured twin of `error`: key monitoring on this column, not on message text."},
        {"ProfileEvents", std::make_shared<DataTypeMap>(lc_string, std::make_shared<DataTypeUInt64>()),
            "On a Start/Finish row: the per-round ProfileEvents delta (the Cas* counters and S3 events for this round). On a Phase row: THAT PHASE's delta, so `GROUP BY phase` over `ProfileEvents['S3ListObjects']` attributes the round's LIST budget to the phase that spent it."},
        {"round_id", std::make_shared<DataTypeString>(),
            "Correlator for every row of one round attempt (its Start, each Phase, and its Finish). Minted per attempt; unlike `round` it exists even for a round that did not run as the writer. Group by this column to reconstruct one round."},
        {"phase", lc_string,
            "The GC phase this row describes (empty on Start/Finish), in execution order: keeper_records, heartbeat_floor, mark, remove. A round that does not run as the writer, or has nothing to remove, emits only the phases it reached."},
        {"phase_duration_microseconds", std::make_shared<DataTypeUInt64>(),
            "Wall-clock duration of this phase in microseconds (Phase rows only). Microseconds because several phases are routinely sub-millisecond and the point is to see when they are not. Phase durations do not sum to the round's `duration_ms`: the round also does untimed bookkeeping between phases."},
        {"phase_metrics", std::make_shared<DataTypeMap>(lc_string, std::make_shared<DataTypeUInt64>()),
            "Phase-specific semantic counts a phase computes for itself and no ProfileEvent can supply (Phase rows only) — for example `records`/`unrecorded_listed` on keeper_records, `marked`/`condemned`/`due` on mark, `records_removed` on remove. The verb counts ride the `ProfileEvents` column of the same row."},
    };
}

void ContentAddressedGarbageCollectionLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);
    columns[i++]->insert(static_cast<Int8>(event_type));
    columns[i++]->insert(disk_name);
    columns[i++]->insert(srid);
    columns[i++]->insert(static_cast<Int8>(trigger));
    columns[i++]->insert(round);
    columns[i++]->insert(static_cast<Int8>(outcome));
    columns[i++]->insert(static_cast<UInt8>(writer));
    columns[i++]->insert(objects_condemned);
    columns[i++]->insert(objects_deleted);
    columns[i++]->insert(objects_absent);
    columns[i++]->insert(objects_replaced);
    columns[i++]->insert(objects_spared);
    columns[i++]->insert(fence_outs);
    columns[i++]->insert(duration_ms);
    columns[i++]->insert(error);
    columns[i++]->insert(error_code);
    {
        Map map;
        map.reserve(profile_events.size());
        for (const auto & [k, v] : profile_events)
            map.push_back(Tuple{k, v});
        columns[i++]->insert(map);
    }
    columns[i++]->insert(round_id);
    columns[i++]->insert(phase);
    columns[i++]->insert(phase_duration_microseconds);
    {
        Map map;
        map.reserve(phase_metrics.size());
        for (const auto & [k, v] : phase_metrics)
            map.push_back(Tuple{k, v});
        columns[i++]->insert(map);
    }
}

}
