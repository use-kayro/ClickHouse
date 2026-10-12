import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_gc_s3"

# Endpoint is http://rustfs1:11121/test/cas_gc_data/, so the pool's blobs live under this prefix of the `test`
# RustFS bucket. Since TEC-851 step 3c a blob body is the only object the pool holds (no `.meta` sidecar).
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
    return {o.object_name for o in objects}


def gc_log_scalar(node, query):
    node.query("SYSTEM FLUSH LOGS")
    return int(node.query(query).strip())


def test_gc_reclaims_dropped_blobs():
    """
    The background GC reclaims the blobs of a dropped partition, from the parts' Keeper records alone, and a
    round lists nothing (TEC-851 step 3c: no heartbeat floor, no server roots).
    """
    node = cluster.instances["node"]

    node.query("DROP TABLE IF EXISTS cas_gc_test SYNC")
    baseline = blob_keys()

    node.query(
        """
        CREATE TABLE cas_gc_test (
            p UInt8,
            id Int64,
            data String
        ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cas_gc_test', '{{replica}}')
        PARTITION BY p
        ORDER BY id
        SETTINGS storage_policy = '{}', old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 1
        """.format(
            STORAGE_POLICY
        )
    )
    since = node.query("SELECT toString(now())").strip()

    for i in range(NUM_INSERTS):
        node.query(
            "INSERT INTO cas_gc_test "
            "SELECT 1, number + {offset}, toString(number + {offset}) "
            "FROM numbers({rows})".format(offset=i * NUM_ROWS, rows=NUM_ROWS)
        )
    assert int(node.query("SELECT count() FROM cas_gc_test")) == NUM_INSERTS * NUM_ROWS
    inserted = blob_keys() - baseline
    assert inserted, "the inserts wrote no blob"

    node.query("ALTER TABLE cas_gc_test DROP PARTITION 1")

    # Every blob the inserts (and the merges of their parts) wrote goes.
    left = inserted & blob_keys()
    for _ in range(RECLAIM_RETRIES):
        if not left:
            break
        time.sleep(RECLAIM_SLEEP)
        left = inserted & blob_keys()
    assert not left, "{} of the {} blobs of the dropped partition were not reclaimed".format(len(left), len(inserted))
    assert node.query(
        "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/cas_gc_test/cas_parts'"
    ).strip() == "0"

    assert gc_log_scalar(
        node,
        "SELECT sum(objects_deleted) FROM system.cas_gc_log WHERE event_type = 'Finish' AND event_time >= '{}'".format(since),
    ) > 0
    # No round lists the pool.
    node.query("SYSTEM FLUSH LOGS")
    listed = node.query(
        "SELECT sum(ProfileEvents['S3ListObjects']) FROM system.cas_gc_log "
        "WHERE event_type = 'Finish' AND event_time >= '{}'".format(since)
    ).strip()
    assert listed in ("0", ""), listed

    node.query("DROP TABLE cas_gc_test SYNC")
