---
description: 'System table containing per-round records of the content-addressed (CAS) MergeTree garbage collector.'
sidebar_label: 'cas_gc_log'
sidebar_position: 30
slug: /operations/system-tables/cas_gc_log
title: 'system.cas_gc_log'
doc_type: 'reference'
---

## Description {#description}

The `system.cas_gc_log` table contains per-round records of the
content-addressed (CAS) MergeTree garbage collector. For every garbage-collection round it stores a
`Start` row and a `Finish` row (like `system.part_log` stores events per data part), with the counts
of blobs deleted, the round duration, the outcome, and a per-round `ProfileEvents`
delta.

Between them it also stores one `Phase` row per GC phase the round reached, each carrying that
phase's own duration, its `ProfileEvents` delta, and its phase-specific counts. All rows of one round
share a `round_id`. See [Per-phase rows](#per-phase-rows).

Rounds are emitted both by the background GC scheduler (`trigger = 'Scheduled'`) and by the
synchronous [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run)
command (`trigger = 'Manual'`).

The table is created only if the `cas_gc_log` server setting is
specified (it is enabled by default in the shipped `config.xml`).

## Columns {#columns}

- `hostname` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — Host name of the server executing the round.
- `event_date` ([Date](/sql-reference/data-types/date)) — Event date.
- `event_time` ([DateTime](/sql-reference/data-types/datetime)) — Event time.
- `event_time_microseconds` ([DateTime64(6)](/sql-reference/data-types/datetime64)) — Event time with microseconds precision.
- `event_type` ([Enum8](/sql-reference/data-types/enum)) — `Start` or `Finish` of a GC round, or one `Phase` of it.
- `disk_name` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — The content-addressed disk the round ran on.
- `server_root_id` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — The disk's `cas_server_root_id` on the server that ran this round.
- `trigger` ([Enum8](/sql-reference/data-types/enum)) — `Scheduled` (background tick) or `Manual` (`SYSTEM` command).
- `round` ([UInt64](/sql-reference/data-types/int-uint)) — This process's round number on this disk (`0` on a `Start` row, and on a round that did not run as the writer).
- `outcome` ([Enum8](/sql-reference/data-types/enum)) — `Unknown` (on a `Start` row), `Success` (the round completed), `Aborted` (the round threw a transient error — backend or Keeper unavailability; the next scheduled round retries it), `Stopped` (a transient error observed after the disk began shutting down: the round was cut short so the shutdown need not wait for it), or `Error` (the round threw a non-transient error — investigate).
- `writer` ([UInt8](/sql-reference/data-types/int-uint)) — `1` when the round ran as the writer (this server is the merge leader of every table on the disk); `0` when it stopped after reading the tables.
- `objects_deleted` ([UInt64](/sql-reference/data-types/int-uint)) — Blobs deleted this round (exact `ETag`), after their last record was removed.
- `objects_absent` ([UInt64](/sql-reference/data-types/int-uint)) — Candidate blobs already gone when deleted.
- `objects_replaced` ([UInt64](/sql-reference/data-types/int-uint)) — Candidate blobs re-uploaded since their `ETag` was read, so kept.
- `objects_spared` ([UInt64](/sql-reference/data-types/int-uint)) — Candidate blobs a record references again.
- `duration_ms` ([UInt64](/sql-reference/data-types/int-uint)) — The round wall-clock duration (on a `Finish` row).
- `error` ([String](/sql-reference/data-types/string)) — The exception text when `outcome = 'Aborted'`, `'Stopped'` or `'Error'`. On a `Stopped` row it names the request the shutdown refused, not the shutdown itself.
- `error_code` ([Int32](/sql-reference/data-types/int-uint)) — The exception code when `outcome = 'Aborted'`, `'Stopped'` or `'Error'`; `0` otherwise. Key monitoring on this column rather than on the `error` text. On an `Aborted`, `Stopped` or `Error` row the counters still report everything the round completed before it threw.
- `ProfileEvents` ([Map(LowCardinality(String), UInt64)](/sql-reference/data-types/map)) — On a `Start`/`Finish` row, the per-round `ProfileEvents` delta (the `CAS*` counters and S3/disk events for this round). On a `Phase` row, **that phase's** delta, so `GROUP BY phase` over `ProfileEvents['S3ListObjects']` attributes the round's `LIST` budget to the phase that spent it.
- `round_id` ([String](/sql-reference/data-types/string)) — The correlator for every row of one round attempt: its `Start`, each of its `Phase` rows, and its `Finish`. Minted per attempt, so unlike `round` it exists even for a round that did not run as the writer. Group by this column to reconstruct one round.
- `phase` ([LowCardinality(String)](/sql-reference/data-types/lowcardinality)) — The GC phase this row describes; empty on `Start`/`Finish`. See [Per-phase rows](#per-phase-rows) for the phase list.
- `phase_duration_microseconds` ([UInt64](/sql-reference/data-types/int-uint)) — The wall-clock duration of this phase, in microseconds (`Phase` rows only). Microseconds rather than milliseconds because several phases are routinely sub-millisecond and the point of the row is to see when they are not.
- `phase_metrics` ([Map(LowCardinality(String), UInt64)](/sql-reference/data-types/map)) — Phase-specific semantic counts (`Phase` rows only) that a phase computes for itself and no `ProfileEvents` counter can supply. The verb counts ride the `ProfileEvents` column of the same row.

## Per-phase rows {#per-phase-rows}

Besides the `Start` and `Finish` row of each round, the collector emits one `Phase` row per GC phase.
Every row of one round attempt — `Start`, each `Phase`, and `Finish` — shares a `round_id`. A round
that does not run as the writer, or has nothing to remove, emits only the phases it actually reached; a round
that throws still emits the row of the phase it died in.

The phases, in execution order:

| `phase` | What it covers | Dominant I/O |
|---|---|---|
| `keeper_records` | Read every registered table's Keeper part records (`tables`, `records`, and the size of the in-memory count: `held_blobs`, `held_references`). | one batched Keeper read per table |
| `mark` | Mark the records whose part no replica lists (`marked`, `due`). | one Keeper `set` per newly marked record |
| `remove` | Remove the records unlisted past `old_parts_lifetime`, then delete the blobs no remaining record and nothing on this server references (`records_removed`, `blobs_deleted`, `blobs_absent`, `blobs_replaced`, `blobs_pending`). | one Keeper multi-op per removed record; per candidate blob a `HEAD`, then an exact `DELETE` |

Which phase dominates a round:

```sql
SELECT phase,
       count() AS rounds,
       quantile(0.5)(phase_duration_microseconds) AS p50_microseconds,
       quantile(0.99)(phase_duration_microseconds) AS p99_microseconds,
       sum(phase_duration_microseconds) AS total_microseconds
FROM system.cas_gc_log
WHERE event_type = 'Phase' AND disk_name = 'ca'
GROUP BY phase
ORDER BY total_microseconds DESC;
```

Which phase spends the `LIST` budget:

```sql
SELECT phase, sum(ProfileEvents['S3ListObjects']) AS lists
FROM system.cas_gc_log
WHERE event_type = 'Phase' AND disk_name = 'ca'
GROUP BY phase
ORDER BY lists DESC;
```

One round, in order — including a round that failed, which is why the correlator is `round_id` and
not `round`:

```sql
SELECT phase, phase_duration_microseconds, phase_metrics, ProfileEvents['S3ListObjects'] AS lists
FROM system.cas_gc_log
WHERE round_id = '...' AND event_type = 'Phase'
ORDER BY event_time_microseconds;
```

Phase durations do not sum to the round's `duration_ms`: the round also performs untimed bookkeeping
between phases, and the `Finish` row's `duration_ms` remains the authority on total round time.

## Example {#example}

```sql
SELECT
    event_type,
    disk_name,
    trigger,
    outcome,
    objects_deleted,
    duration_ms
FROM system.cas_gc_log
ORDER BY event_time_microseconds DESC
LIMIT 2
FORMAT Vertical;
```

## See Also {#see-also}

- [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run) — run one GC round synchronously.
- [`system.cas_mounts`](/operations/system-tables/cas_mounts) — per-disk GC health.
- [`system.part_log`](/operations/system-tables/part_log) — the analogous per-part event log.
