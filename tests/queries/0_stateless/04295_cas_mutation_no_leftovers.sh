#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree, no-replicated-database
# ^ cas is an object-storage metadata type (keep it off the minimal fasttest image);
#   no-parallel because we inspect a known on-disk pool directory from the shell and must not race
#   another test sharing the same path;
#   no-shared-merge-tree: this exercises open-source ReplicatedMergeTree on a cas disk;
#   no-replicated-database: the replica path is hard-coded per the database, not per the replica.

# No-leftovers oracle for MUTATIONS + lightweight DELETE (patch parts) on the content-addressed pool
# (CAS M7), exercised over a `local` object_storage backend so the pool is a plain directory the test
# shell can inspect directly. Mirrors 04290 but adds heavy mutations and a patch-part lightweight
# DELETE before the drop: a mutation supersedes the source part (its uniquely-owned blobs become
# unreachable) and writes a new part; carried-forward columns stay referenced. The GC reclaims a blob
# only through the Keeper part records of a ReplicatedMergeTree table, so the table is replicated. We
# snapshot the blob files the inserts and the mutations wrote, DROP PARTITION (not DROP TABLE: it
# removes the records with the table's Keeper path, so its blobs leak by design), and assert that
# `SYSTEM CAS GC RUN` rounds reclaim every one of them (no mutated-away or patch-part blobs left
# behind), that `FSCK` on the running disk (T13) reads back zero `dangling` objects, and that
# `_pool_meta` survives. `unreachable` is not asserted: DROP's empty covering part has no record and the
# writer's metadata objects are not the GC's to reclaim. Teardown is fail-closed (spec rev.8 §5/§9):
# FORGET the disk, verify `vanished(forgotten)`, then rm.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

POOL_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}_04295_${RANDOM}"

rm -rf "${POOL_DIR:?}"
mkdir -p "${POOL_DIR}"

# The blob files (not their `.meta` sidecars) currently living under blobs/ in the pool, sorted.
list_pool_blobs() {
    find "${POOL_DIR}/ca/blobs" -type f ! -name '*.meta' 2>/dev/null | sort
}

DISK_NAME="ca_04295_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
DISK_DEF="disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${CLICKHOUSE_DATABASE}_04295',
    name = '${DISK_NAME}',
    path = '${POOL_DIR}/',
    cas_gc_enabled = 1,
    cas_gc_interval_sec = 1)"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_cas_mut_leftovers SYNC"

BASELINE=$(list_pool_blobs)

$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_cas_mut_leftovers (a UInt64, v UInt64, s String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_cas_mut_leftovers', 'r1')
PARTITION BY intDiv(a, 1000000) ORDER BY a
SETTINGS disk = ${DISK_DEF}, enable_block_number_column = 1, enable_block_offset_column = 1,
         old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1"

# Several distinct inserts -> several distinct parts/blobs.
for i in 0 1 2 3; do
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO t_cas_mut_leftovers
        SELECT number + ${i} * 100000, (number + ${i} * 100000) * 10, toString(number + ${i} * 100000)
        FROM numbers(100000)"
done

# The blob files seen so far, re-snapshotted after each step: the GC may reclaim a superseded part's
# blobs before the next step, so one snapshot at the end would miss them.
SEEN=$(list_pool_blobs)
if [ -n "$(comm -13 <(printf '%s\n' "${BASELINE}") <(printf '%s\n' "${SEEN}"))" ]; then
    echo "grew_above_baseline 1"
else
    echo "grew_above_baseline 0"
fi

# Heavy mutation (rewrites the v column; a/s carry forward by reference -> shared blobs).
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_cas_mut_leftovers UPDATE v = v + 1 WHERE a % 2 = 0 SETTINGS mutations_sync = 2"
SEEN=$( { printf '%s\n' "${SEEN}"; list_pool_blobs; } | grep . | sort -u )
# Heavy mutation: delete part of the data.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_cas_mut_leftovers DELETE WHERE a % 5 = 0 SETTINGS mutations_sync = 2"
SEEN=$( { printf '%s\n' "${SEEN}"; list_pool_blobs; } | grep . | sort -u )
# Patch part: forced lightweight-update DELETE (throws if unsupported, so success == patch path).
$CLICKHOUSE_CLIENT --query "
    DELETE FROM t_cas_mut_leftovers WHERE a % 7 = 0
    SETTINGS enable_lightweight_update = 1, lightweight_delete_mode = 'lightweight_update_force', lightweight_deletes_sync = 2"
SEEN=$( { printf '%s\n' "${SEEN}"; list_pool_blobs; } | grep . | sort -u )
WRITTEN=$(comm -13 <(printf '%s\n' "${BASELINE}") <(printf '%s\n' "${SEEN}"))

# Self-checking row count: a ranges over [0, 400000); the two deletes drop a%5=0 and a%7=0
# (the UPDATE does not change the row count), so the survivors are exactly a%5!=0 AND a%7!=0.
$CLICKHOUSE_CLIENT --query "
SELECT 'rows_after_mutations_correct',
       count() = (SELECT count() FROM numbers(400000) WHERE number % 5 != 0 AND number % 7 != 0)
FROM t_cas_mut_leftovers"

# Drop the partition: the replica unlists every part (original, mutated, and patch parts -- the cleanup
# thread drops the patch parts once their partition has no active part), and their records become GC fodder.
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_cas_mut_leftovers DROP PARTITION 0"

# Run `SYSTEM CAS GC RUN` rounds until no blob the inserts or the mutations wrote is left. Bounded
# (~60 rounds, half-second spacing), not a fixed sleep: the parts' removal from the replica, the
# record's mark and its removal `old_parts_lifetime` later each take a cleanup or GC round.
LEFT=$(printf '%s\n' "${WRITTEN}" | grep -c .)
for _ in $(seq 1 60); do
    $CLICKHOUSE_CLIENT --query "SYSTEM CAS GC RUN '${DISK_NAME}'" > /dev/null
    LEFT=$(comm -12 <(printf '%s\n' "${WRITTEN}") <(list_pool_blobs) | grep -c .)
    [ "${LEFT}" = "0" ] && break
    sleep 0.5
done
echo "blobs_left ${LEFT}"

# FSCK runs directly on the running disk (T13): no live ref may reference a missing object.
$CLICKHOUSE_CLIENT --query "SYSTEM CAS FSCK '${DISK_NAME}'" --format TSVWithNames \
    | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                  { print "fsck_dangling", $col["dangling"] }'

if [ -f "${POOL_DIR}/ca/_pool_meta" ]; then
    echo "pool_meta_present 1"
else
    echo "pool_meta_present 0"
fi

# The table's remaining blobs (DROP's empty covering part) leak with its Keeper path by design: not asserted.
$CLICKHOUSE_CLIENT --query "DROP TABLE t_cas_mut_leftovers SYNC"

# Fail-closed teardown (spec rev.8 §5/§9): FORGET the disk (force-Vanish, node-local; the table is
# already dropped above), verify it reads exactly `vanished(forgotten)` in the mounts table, and only
# then rm. A failed FORGET or an unexpected lifecycle aborts with the pool dir left in place. FORGET logs
# an operator WARNING; the harness runs the client at --send_logs_level=warning, so that expected warning
# would stream to stderr and be flagged as a failure -- suppress it for this call.
$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal \
    --query "SYSTEM CAS FORGET '${DISK_NAME}'" || {
    echo "FORGET failed — leaving pool dir in place (fail-closed)"; exit 1; }
LIFECYCLE=$($CLICKHOUSE_CLIENT --query "
    SELECT lifecycle || '(' || lifecycle_reason || ')' FROM system.cas_mounts
    WHERE disk = '${DISK_NAME}'")
[ "${LIFECYCLE}" = "vanished(forgotten)" ] || {
    echo "unexpected lifecycle after FORGET: ${LIFECYCLE}"; exit 1; }

rm -rf "${POOL_DIR:?}"   # safe: FORGET stopped and joined every CAS thread for this disk
