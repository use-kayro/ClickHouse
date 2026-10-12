#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree, no-replicated-database
# ^ cas is an object-storage metadata type (keep it off the minimal fasttest image);
#   no-parallel because we inspect a known on-disk pool directory from the shell and must not race
#   another test sharing the same path;
#   no-shared-merge-tree: this exercises open-source ReplicatedMergeTree on a cas disk;
#   no-replicated-database: the replica path is hard-coded per the database, not per the replica.

# North-star "no S3 leftovers" oracle for the content-addressed pool, exercised over a `local`
# object_storage backend so the pool is a plain directory the test shell can inspect directly.
#
# We put the pool under CLICKHOUSE_USER_FILES_UNIQUE (an absolute path both the server and this
# shell can see on a local run) and run the GC aggressively (interval=1s on the disk; the table's
# old_parts_lifetime/cleanup_delay_period = 1s). The GC reclaims a blob only through the Keeper part
# records of a ReplicatedMergeTree table (a single replica is the writer), so the table is replicated.
# We then:
#   (1) snapshot the pool's blob files (baseline, ~empty),
#   (2) CREATE a ReplicatedMergeTree on the CA disk and INSERT several distinct batches to make many blobs,
#   (3) assert the inserts wrote blobs (the blob files not in the baseline),
#   (4) DROP PARTITION, so the parts are unlisted by the replica and their records become GC fodder
#       (not DROP TABLE: it removes the records with the table's Keeper path, so its blobs leak by design),
#   (5) run `SYSTEM CAS GC RUN` rounds until none of the blobs the inserts wrote is left (bounded loop on
#       that observable, NOT a fixed sleep), then run `FSCK` directly on the running disk (T13) and expect
#       zero `dangling` objects (nothing the live refs still reference was deleted). `unreachable` is not
#       asserted: DROP's empty covering part has no record and the writer's metadata objects are not the
#       GC's to reclaim.
# `_pool_meta` (durable single-owner marker) and the `store/` metadata tree are expected to remain.
# Teardown is fail-closed (spec rev.8 §5/§9): `SYSTEM CAS FORGET` the disk (force-Vanish,
# node-local), verify it reads `vanished(forgotten)` in system.cas_mounts, and only then
# `rm -rf` — FORGET stopped and joined every CAS background thread for this disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POOL_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}_04290_${RANDOM}"

# Fresh pool dir for this run.
rm -rf "${POOL_DIR:?}"
mkdir -p "${POOL_DIR}"

# The blob files (not their `.meta` sidecars) currently living under blobs/ in the pool, sorted.
list_pool_blobs() {
    find "${POOL_DIR}/ca/blobs" -type f ! -name '*.meta' 2>/dev/null | sort
}

DISK_NAME="ca_04290_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
DISK_DEF="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${CLICKHOUSE_DATABASE}_04290',
    name = '${DISK_NAME}',
    path = '${POOL_DIR}/',
    cas_gc_enabled = 1,
    cas_gc_interval_sec = 1)"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_cas_leftovers SYNC"

# (1) Baseline.
BASELINE=$(list_pool_blobs)

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_cas_leftovers (a UInt64, s String, d Date)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cas_leftovers', 'r1')
PARTITION BY intDiv(a, 1000000) ORDER BY a
SETTINGS disk = ${DISK_DEF}, old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1"

# (2) Several distinct inserts -> several distinct parts/blobs (distinct data => no dedup-away).
for i in 0 1 2 3 4 5; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO t_cas_leftovers
        SELECT number + ${i} * 100000, toString(number + ${i} * 100000), toDate('2020-01-01') + (number % 1000)
        FROM numbers(100000)"
done

$CLICKHOUSE_CLIENT --query "SELECT 'rows', count() FROM t_cas_leftovers"

# (3) The inserts must have written blobs.
WRITTEN=$(comm -13 <(printf '%s\n' "${BASELINE}") <(list_pool_blobs))
if [ -n "${WRITTEN}" ]; then
    echo "grew_above_baseline 1"
else
    echo "grew_above_baseline 0"
fi

# (4) Drop the partition: the replica unlists its parts, and their records become GC fodder.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_cas_leftovers DROP PARTITION 0"

# (5) Run `SYSTEM CAS GC RUN` rounds until no blob the inserts wrote is left. Bounded (~60 rounds,
#     half-second spacing), not a fixed sleep: the parts' removal from the replica, the record's mark and
#     its removal `old_parts_lifetime` later each take a cleanup or GC round.
LEFT=$(printf '%s\n' "${WRITTEN}" | grep -c .)
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT --query "SYSTEM CAS GC RUN '${DISK_NAME}'" > /dev/null
    LEFT=$(comm -12 <(printf '%s\n' "${WRITTEN}") <(list_pool_blobs) | grep -c .)
    [ "${LEFT}" = "0" ] && break
    sleep 0.5
done
echo "blobs_left ${LEFT}"

# (6) FSCK runs directly on the running disk (T13): no live ref may reference a missing object.
$CLICKHOUSE_CLIENT --query "SYSTEM CAS FSCK '${DISK_NAME}'" --format TSVWithNames \
    | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                  { print "fsck_dangling", $col["dangling"] }'

# _pool_meta must still be present (durable single-owner marker is never GC'd).
if [ -f "${POOL_DIR}/ca/_pool_meta" ]; then
    echo "pool_meta_present 1"
else
    echo "pool_meta_present 0"
fi

# The table's remaining blobs (DROP's empty covering part) leak with its Keeper path by design: not asserted.
$CLICKHOUSE_CLIENT --query "DROP TABLE t_cas_leftovers SYNC"

# (7) Fail-closed teardown (spec rev.8 §5/§9): FORGET the disk (force-Vanish, node-local; the table is
#     already dropped above), verify it reads exactly `vanished(forgotten)` in the mounts table, and
#     only then rm. A failed FORGET or an unexpected lifecycle aborts with the pool dir left in place.
#     FORGET logs an operator WARNING; the harness runs the client at --send_logs_level=warning, so that
#     expected warning would stream to stderr and be flagged as a failure -- suppress it for this call.
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${DISK_NAME}'" || {
    echo "FORGET failed — leaving pool dir in place (fail-closed)"; exit 1; }
LIFECYCLE=$($CLICKHOUSE_CLIENT --query "
    SELECT lifecycle || '(' || lifecycle_reason || ')' FROM system.cas_mounts
    WHERE disk = '${DISK_NAME}'")
[ "${LIFECYCLE}" = "vanished(forgotten)" ] || {
    echo "unexpected lifecycle after FORGET: ${LIFECYCLE}"; exit 1; }

rm -rf "${POOL_DIR:?}"   # safe: FORGET stopped and joined every CAS thread for this disk
