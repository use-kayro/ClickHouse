"""TEC-850, step 2 of keeping CAS part metadata in Keeper: the pool's GC is derived from Keeper.

A blob is live while some Keeper record (`<zookeeper_path>/cas_parts/<part>`) references it. The writer's GC
(the only one that deletes) removes the record of a part no replica lists only `old_parts_lifetime` after it
marked it, so a reader query shorter than that keeps reading the blobs of parts DROP/REPLACE PARTITION removed
under it.
"""

import itertools
import shlex
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_shared"
CA_DISK = "disk_cas_shared"
BLOBS_PREFIX = "reader_pin_pool/blobs/"
ROWS = 100
QUERY_SECONDS = 45
# The writer keeps an unlisted part's record (and so its blobs) for its `old_parts_lifetime` after the GC
# marks it: longer than the reader's query here, as production readers' queries stay under the default.
WRITER_OLD_PARTS_LIFETIME = 60
READ_ERRORS = r"NoSuchKey|FILE_DOESNT_EXIST|CANNOT_READ_ALL_DATA"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    for name in ("writer", "reader"):
        cluster.add_instance(
            name,
            main_configs=["configs/storage_conf.xml", "configs/server_root_id_{}.xml".format(name)],
            macros={"replica": name},
            with_rustfs=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["create_if_not_exists"],  # Keeper part records (TEC-849)
            stay_alive=True,
        )
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_until(predicate, timeout, what):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline, "timed out waiting for {}".format(what)
        time.sleep(0.5)


def log_lines(node, pattern):
    out = node.exec_in_container(
        [
            "bash",
            "-c",
            "grep -a -E {} /var/log/clickhouse-server/clickhouse-server.log || true".format(
                shlex.quote(pattern)
            ),
        ]
    )
    return [line for line in out.splitlines() if line.strip()]


def create_table(node, table, reader):
    settings = [
        "storage_policy = '{}'".format(STORAGE_POLICY),
        "min_bytes_for_wide_part = 0",
        "old_parts_lifetime = {}".format(1 if reader else WRITER_OLD_PARTS_LIFETIME),
        "cleanup_delay_period = 1",
        "max_cleanup_delay_period = 1",
    ]
    if reader:
        # As production readers: they never assign merges and never run them, they fetch the writer's result.
        settings += ["always_fetch_merged_part = 1", "replicated_can_become_leader = 0"]
    node.query(
        """
        CREATE TABLE {table} (p UInt8, x UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{{replica}}')
        PARTITION BY p
        ORDER BY x
        SETTINGS {settings}
        """.format(table=table, settings=", ".join(settings))
    )


_inserts = itertools.count()


def insert(node, table, p, rows=ROWS):
    """Inserts rows no other insert of this module shares a blob with; returns the (sum(x), count()) added.

    Distinct `x` alone is not enough: `p.bin` and the marks depend only on `p` and the row count, so two
    inserts into the same partition would dedup to one blob unless their row counts differ too."""
    k = next(_inserts)
    start, rows = k * 1000000, rows + k
    node.query(
        "INSERT INTO {} SELECT {}, number + {} FROM numbers({})".format(table, p, start, rows)
    )
    return rows * start + rows * (rows - 1) // 2, rows


def blob_keys():
    objects = cluster.rustfs_client.list_objects(cluster.rustfs_bucket, BLOBS_PREFIX, recursive=True)
    return {o.object_name for o in objects if not o.object_name.endswith(".meta")}


def partition_parts(node, table, partition):
    return node.query(
        "SELECT name FROM system.parts WHERE table = '{}' AND partition_id = '{}' AND active "
        "ORDER BY name".format(table, partition)
    ).split()


def active_parts(node, table):
    return node.query(
        "SELECT name FROM system.parts WHERE table = '{}' AND active ORDER BY name".format(table)
    ).split()


def records(node, table):
    return node.query(
        "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/{}/cas_parts' "
        "ORDER BY name".format(table)
    ).split()


def sums(node, table):
    out = node.query("SELECT p, sum(x), count() FROM {} GROUP BY p ORDER BY p FORMAT TSV".format(table))
    return {int(p): (int(s), int(c)) for p, s, c in (line.split("\t") for line in out.splitlines())}


def server_now(node):
    return node.query("SELECT toString(now())").strip()


def gc_scalar(node, sql):
    node.query("SYSTEM FLUSH LOGS")
    return int(node.query(sql).strip() or 0)


def led_rounds(since):
    """GC rounds that ran as the deleting member, on either node (`since` is a writer timestamp; the containers
    share the host clock)."""
    return sum(
        gc_scalar(
            node,
            "SELECT count() FROM system.cas_gc_log WHERE disk_name = '{}' AND event_type = 'Finish' "
            "AND outcome = 'Success' AND writer = 1 AND event_time > '{}'".format(CA_DISK, since),
        )
        for node in cluster.instances.values()
    )


def assert_gc_path_lists_only_the_floor(writer, reader, since):
    # The heartbeat floor's one LIST of `gc/server-roots/` is the only listing a round may do: liveness comes
    # from Keeper, never from listing the pool.
    writer.query("SYSTEM FLUSH LOGS")
    over_budget = writer.query(
        """
        SELECT round_id,
               sumIf(ProfileEvents['S3ListObjects'], event_type = 'Finish') AS total,
               sumIf(ProfileEvents['S3ListObjects'], event_type = 'Phase' AND phase = 'heartbeat_floor') AS floor
        FROM system.cas_gc_log
        WHERE disk_name = '{disk}' AND round_id IN (
            SELECT round_id FROM system.cas_gc_log
            WHERE disk_name = '{disk}' AND event_type = 'Finish' AND event_time >= '{since}')
        GROUP BY round_id
        HAVING total != floor
        FORMAT TSV
        """.format(disk=CA_DISK, since=since)
    )
    assert not over_budget.strip(), over_budget
    assert gc_scalar(
        writer,
        "SELECT count() FROM system.cas_gc_log WHERE disk_name = '{}' AND event_type = 'Finish' "
        "AND event_time >= '{}'".format(CA_DISK, since),
    ) > 0
    # Only the writer deletes.
    assert gc_scalar(
        reader,
        "SELECT sum(objects_deleted) FROM system.cas_gc_log WHERE disk_name = '{}' AND event_type = 'Finish' "
        "AND event_time >= '{}'".format(CA_DISK, since),
    ) == 0


@pytest.mark.parametrize("action", ["drop", "replace"])
def test_reader_query_survives_gc(action):
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table = "reader_pin_{}".format(action)
    staging = "reader_pin_{}_staging".format(action)
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    writer.query("DROP TABLE IF EXISTS {} SYNC".format(staging))
    create_table(writer, table, reader=False)
    create_table(reader, table, reader=True)
    since = {node: server_now(node) for node in (writer, reader)}

    expected = {2: insert(writer, table, 2)}
    if action == "replace":
        # Replicated too, so its part has a Keeper record like every live part of the pool.
        create_table(writer, staging, reader=False)
        replacement = insert(writer, staging, 1)
    s0 = blob_keys()

    # Merges stay off until the reader's query holds the three original parts.
    writer.query("SYSTEM STOP MERGES {}".format(table))
    inserted = [insert(writer, table, 1) for _ in range(3)]
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    s1 = blob_keys()
    p1_orig = s1 - s0
    assert p1_orig, "the partition-1 inserts wrote no new blob"
    original_parts = partition_parts(reader, table, 1)
    assert len(original_parts) == 3, original_parts
    query_result = "{}\t{}".format(sum(s for s, _ in inserted), sum(c for _, c in inserted))

    query_id = "reader_pin_{}_{}".format(action, uuid.uuid4().hex)
    query = reader.get_query_request(
        "SELECT sum(x), count() FROM {} WHERE p = 1 AND NOT ignore(sleepEachRow({}))".format(
            table, QUERY_SECONDS / sum(c for _, c in inserted)
        ),
        settings={
            "max_block_size": 1,
            "max_threads": 1,
            "function_sleep_max_microseconds_per_block": 0,
            # Without prefetch each part is opened when the query reaches it, i.e. after the GC rounds below.
            "allow_prefetched_read_pool_for_remote_filesystem": 0,
        },
        query_id=query_id,
        timeout=QUERY_SECONDS * 4,
    )

    def query_running():
        return reader.query(
            "SELECT count() FROM system.processes WHERE query_id = '{}'".format(query_id)
        ).strip() == "1"

    wait_until(query_running, 60, "the reader's query to start")

    writer.query("SYSTEM START MERGES {}".format(table))
    writer.query("OPTIMIZE TABLE {} PARTITION 1 FINAL".format(table))
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    p1_merged = blob_keys() - s1
    assert p1_merged, "the merge wrote no new blob"

    if action == "drop":
        writer.query("ALTER TABLE {} DROP PARTITION 1".format(table))
    else:
        writer.query("ALTER TABLE {} REPLACE PARTITION 1 FROM {}".format(table, staging))
        expected[1] = replacement
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    dropped_at = server_now(writer)

    wait_until(lambda: led_rounds(dropped_at) >= 5, QUERY_SECONDS, "five GC rounds")
    missing = p1_orig - blob_keys()
    assert not missing, "GC deleted {} blobs the reader's query still reads".format(len(missing))
    held = reader.query(
        "SELECT name FROM system.parts WHERE table = '{}' AND NOT active AND refcount > 1 "
        "ORDER BY name".format(table)
    ).split()
    assert set(original_parts) <= set(held), (original_parts, held)
    # The reader no longer lists them (DROP/REPLACE remove part znodes at once); their records, marked by the
    # GC, keep their blobs for the writer's `old_parts_lifetime`.
    kept = records(writer, table)
    assert set(original_parts) <= set(kept), (original_parts, kept)
    assert query_running(), "the query ended before the assertions; raise QUERY_SECONDS"

    assert query.get_answer().strip() == query_result
    errors = log_lines(reader, r"\{{{}\}}.*({})".format(query_id, READ_ERRORS))
    assert not errors, errors[:5]

    # Once the records are due, everything no live part references goes: the old blobs and the old records.
    wait_until(
        lambda: not (p1_orig | p1_merged) & blob_keys(),
        WRITER_OLD_PARTS_LIFETIME + 120,
        "the dropped partition's blobs to be deleted",
    )
    wait_until(
        lambda: records(writer, table) == active_parts(writer, table),
        WRITER_OLD_PARTS_LIFETIME + 120,
        "the records of the dropped parts to be deleted",
    )
    for node in (writer, reader):
        assert sums(node, table) == expected, node.name

    assert_gc_path_lists_only_the_floor(writer, reader, since[writer])
    for node in (writer, reader):
        node.query("DROP TABLE {} SYNC".format(table))
    writer.query("DROP TABLE IF EXISTS {} SYNC".format(staging))


def test_writer_killed_mid_round_leaks_nothing():
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table = "reader_pin_kill"
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    create_table(writer, table, reader=False)
    create_table(reader, table, reader=True)

    expected = {2: insert(writer, table, 2)}
    # One part per insert, so each part's blobs are the delta its insert added.
    writer.query("SYSTEM STOP MERGES {}".format(table))
    part_blobs = {}
    for _ in range(3):
        before_blobs, before_parts = blob_keys(), set(partition_parts(writer, table, 3))
        insert(writer, table, 3)
        (part,) = set(partition_parts(writer, table, 3)) - before_parts
        part_blobs[part] = blob_keys() - before_blobs
        assert part_blobs[part], "the insert of {} wrote no new blob".format(part)
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)

    writer.query("ALTER TABLE {} DROP PARTITION 3".format(table))
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    dropped_at = server_now(writer)
    wait_until(lambda: led_rounds(dropped_at) >= 1, 60, "a GC round after the drop")
    writer.stop_clickhouse(kill=True)

    # The kill lands while the dropped parts' records are marked but not yet due: they persist, and the
    # restarted writer reclaims every blob (its condemnations are redone, the records' marks are in Keeper).
    left = set(records(reader, table))
    assert set(part_blobs) <= left, (part_blobs.keys(), left)
    writer.start_clickhouse()
    all_blobs = set().union(*part_blobs.values())
    wait_until(lambda: not all_blobs & blob_keys(), WRITER_OLD_PARTS_LIFETIME + 180, "the dropped partition's blobs to be deleted")
    wait_until(
        lambda: not set(part_blobs) & set(records(reader, table)),
        60,
        "the records of the dropped parts to be deleted",
    )
    for node in (writer, reader):
        assert sums(node, table) == expected, node.name
        node.query("DROP TABLE {} SYNC".format(table))
