import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_gc_s3"

# Endpoint is http://rustfs1:11121/test/cas_gc_data/, so the pool's content blobs live under this prefix of
# the `test` RustFS bucket (each with a `.meta` sidecar next to it).
BLOBS_PREFIX = "cas_gc_data/blobs/"

# Enough rows / inserts to materialise several distinct blobs in the pool.
NUM_ROWS = 100000
NUM_INSERTS = 8

# The GC runs every second; the dropped parts' records are removed `old_parts_lifetime` (1 s here) after the
# GC marks them, and their blobs right after. How long we give the background GC to do its rounds.
RECLAIM_RETRIES = 60
RECLAIM_SLEEP = 1.0


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        macros={"replica": "node"},
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


def blob_keys():
    objects = cluster.rustfs_client.list_objects(cluster.rustfs_bucket, BLOBS_PREFIX, recursive=True)
    return {o.object_name for o in objects if not o.object_name.endswith(".meta")}


def gc_log_scalar(node, query):
    node.query("SYSTEM FLUSH LOGS")
    return int(node.query(query).strip())


def create_table(node, table):
    node.query(
        """
        CREATE TABLE {table} (
            p UInt8,
            id Int64,
            data String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{{replica}}')
        PARTITION BY p
        ORDER BY id
        SETTINGS storage_policy = '{policy}', old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1
        """.format(
            table=table, policy=STORAGE_POLICY
        )
    )


def assert_reclaimed(node, table, inserted):
    """
    Every blob in `inserted` (written by the inserts, and the merges of their parts, into a partition dropped since)
    goes, and so do the table's records. DROP's empty covering part is a local part with no record: its blob (the one
    every empty file hashes to) is not the GC's to reclaim.
    """
    assert inserted, "the inserts wrote no blob"
    left = inserted & blob_keys()
    for _ in range(RECLAIM_RETRIES):
        if not left:
            break
        time.sleep(RECLAIM_SLEEP)
        left = inserted & blob_keys()
    assert not left, "{} of the {} blobs of the dropped partition were not reclaimed".format(len(left), len(inserted))
    assert node.query(
        "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/{}/cas_parts'".format(table)
    ).strip() == "0"


def test_gc_reclaims_dropped_blobs():
    """
    The background GC reclaims the blobs of a dropped partition, from the parts' Keeper records alone: the
    only listing a round does is the heartbeat floor's `gc/server-roots/`.
    """
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS cas_gc_test SYNC")
    baseline = blob_keys()

    create_table(node, "cas_gc_test")
    since = node.query("SELECT toString(now())").strip()

    for i in range(NUM_INSERTS):
        node.query(
            "INSERT INTO cas_gc_test "
            "SELECT 1, number + {offset}, toString(number + {offset}) "
            "FROM numbers({rows})".format(offset=i * NUM_ROWS, rows=NUM_ROWS)
        )
    assert int(node.query("SELECT count() FROM cas_gc_test")) == NUM_INSERTS * NUM_ROWS
    inserted = blob_keys() - baseline

    node.query("ALTER TABLE cas_gc_test DROP PARTITION 1")
    assert_reclaimed(node, "cas_gc_test", inserted)

    assert gc_log_scalar(
        node,
        "SELECT sum(objects_deleted) FROM system.cas_gc_log WHERE event_type = 'Finish' AND event_time >= '{}'".format(since),
    ) > 0
    # The heartbeat floor's one LIST is the only listing on the GC path.
    node.query("SYSTEM FLUSH LOGS")
    over_budget = node.query(
        """
        SELECT round_id,
               sumIf(ProfileEvents['S3ListObjects'], event_type = 'Finish') AS total,
               sumIf(ProfileEvents['S3ListObjects'], event_type = 'Phase' AND phase = 'heartbeat_floor') AS floor
        FROM system.cas_gc_log
        WHERE round_id IN (SELECT round_id FROM system.cas_gc_log WHERE event_type = 'Finish' AND event_time >= '{}')
        GROUP BY round_id
        HAVING total != floor
        FORMAT TSV
        """.format(since)
    )
    assert not over_budget.strip(), over_budget

    node.query("DROP TABLE cas_gc_test SYNC")


def test_gc_after_drop_of_boot_table():
    """
    A table that existed at boot and is dropped afterwards keeps its name in the database's startup tasks; the GC
    still reads the tables on the disk and reclaims.
    """
    node = cluster.instances["node"]

    create_table(node, "cas_gc_boot")
    node.restart_clickhouse()
    node.query("DROP TABLE cas_gc_boot SYNC")

    create_table(node, "cas_gc_test")
    baseline = blob_keys()
    node.query("INSERT INTO cas_gc_test SELECT 1, number, toString(number) FROM numbers({})".format(NUM_ROWS))
    inserted = blob_keys() - baseline
    node.query("ALTER TABLE cas_gc_test DROP PARTITION 1")
    assert_reclaimed(node, "cas_gc_test", inserted)

    node.query("DROP TABLE cas_gc_test SYNC")
