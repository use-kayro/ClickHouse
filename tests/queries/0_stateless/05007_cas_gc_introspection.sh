#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# Introspection coverage for the content-addressed (CA) garbage collector: the
# `SYSTEM CAS GC RUN <disk>` command runs one GC round synchronously and
# the round is recorded in `system.cas_gc_log` (a Start + Finish row
# per round, like `part_log`). We build a CA disk inline (named, so the SYSTEM command can target it),
# create garbage by inserting then truncating, run one round, flush the log, and assert the rows are
# there with the right shape.
#
# This is a .sh test (not .sql) because `SYSTEM CAS GC RUN` now returns a
# one-row-per-disk result set (UX pass); the synchronous round below only cares about its side
# effects on the log, so its own output is redirected to /dev/null.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP TABLE IF EXISTS t_cas_gc_introspection;

-- A named inline CA disk: the \`name\` is what \`SYSTEM CAS GC RUN <name>\`
-- targets and what lands in the log's \`disk_name\` column.
CREATE TABLE t_cas_gc_introspection (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${CLICKHOUSE_DATABASE}_05007',
    name = '${CLICKHOUSE_DATABASE}_05007_cas_gc_introspection',
    path = '${CLICKHOUSE_DATABASE}_05007_cas_gc_introspection_pool/',
    cas_gc_enabled = 1,
    cas_gc_interval_sec = 1),
    old_parts_lifetime = 1;

-- Two distinct inserts => distinct blobs (not deduped away), then TRUNCATE drops every ref so the
-- blobs/trees become unreferenced GC fodder.
INSERT INTO t_cas_gc_introspection SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_cas_gc_introspection SELECT number, toString(number) FROM numbers(1000, 1000);
TRUNCATE TABLE t_cas_gc_introspection;
"""

${CLICKHOUSE_CLIENT} -q "SYSTEM CAS GC RUN '${CLICKHOUSE_DATABASE}_05007_cas_gc_introspection'" > /dev/null

${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM FLUSH LOGS cas_gc_log;

-- A Start, a Finish, and a Manual-triggered row were all recorded for this disk.
SELECT
    countIf(event_type = 'Start') > 0,
    countIf(event_type = 'Finish') > 0,
    countIf(trigger = 'Manual') > 0
FROM system.cas_gc_log
WHERE disk_name LIKE '%${CLICKHOUSE_DATABASE}_05007_cas_gc_introspection%';

-- The correlator: every row of the most recent round of this disk -- its Start, each Phase, and its
-- Finish -- shares one \`round_id\`, and that round emitted at least one Phase row between them.
SELECT countIf(event_type = 'Start') = 1,
       countIf(event_type = 'Finish') = 1,
       countIf(event_type = 'Phase') > 0
FROM system.cas_gc_log
WHERE round_id = (
    SELECT round_id FROM system.cas_gc_log
    WHERE disk_name LIKE '%${CLICKHOUSE_DATABASE}_05007_cas_gc_introspection%' AND event_type = 'Finish'
    ORDER BY event_time_microseconds DESC LIMIT 1);

-- The error path: a non-CA disk (the always-present local \`default\`) is rejected.
SYSTEM CAS GC RUN 'default'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_cas_gc_introspection;
SELECT 'ok';
"""

# FORGET logs an operator WARNING; the harness runs the client at --send_logs_level=warning, which would
# stream that expected warning to stderr and be flagged as a failure. Suppress it for the FORGET call only.
${CLICKHOUSE_CLIENT} --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${CLICKHOUSE_DATABASE}_05007_cas_gc_introspection'" || {
    echo "FORGET failed"; exit 1; }
