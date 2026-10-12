import shlex
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "ref_snaplog"
RO_DISK = "disk_ca_ro"

# Endpoint is http://rustfs1:11121/test/cas_snaplog_data/, so the pool lives under bucket `test`,
# prefix `cas_snaplog_data/`. The snapshot+log ref protocol keeps a table's immutable transaction logs
# and snapshots under cas/ns/stream/, and content blobs (each with a `.meta` sidecar) under blobs/.
POOL = "cas_snaplog_data"
BLOBS_PREFIX = POOL + "/blobs/"
REFS_PREFIX = POOL + "/cas/ns/stream/"

NUM_ROWS = 20000
NUM_INSERTS = 8

# The GC runs every second; the dropped parts' records are removed `old_parts_lifetime` (1 s here) after the
# GC marks them, and their blobs right after. How long we give the background GC to do its rounds.
RECLAIM_RETRIES = 120
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


def _count(prefix):
    return len(
        list(
            cluster.rustfs_client.list_objects(
                cluster.rustfs_bucket, prefix, recursive=True
            )
        )
    )


def _blob_keys():
    objects = cluster.rustfs_client.list_objects(cluster.rustfs_bucket, BLOBS_PREFIX, recursive=True)
    return {o.object_name for o in objects if not o.object_name.endswith(".meta")}


def _disks(node, query):
    # Run a clickhouse-disks command against the read-only CA window over the same pool.
    return node.exec_in_container(
        [
            "bash",
            "-c",
            "/usr/bin/clickhouse disks -C /etc/clickhouse-server/config.xml "
            "--disk {} --save-logs --query {}".format(RO_DISK, shlex.quote(query)),
        ]
    )


def test_ref_snaplog_lifecycle_reclaims_and_fsck_clean():
    node = cluster.instances["node"]

    for t in ("ref_t1", "ref_t1_renamed", "ref_t2"):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(t))

    baseline = _blob_keys()

    # (1) Two tables on the CA/rustfs policy.
    for t in ("ref_t1", "ref_t2"):
        node.query(
            "CREATE TABLE {t} (p UInt8, id Int64, data String) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{t}', '{{replica}}') PARTITION BY p ORDER BY id "
            "SETTINGS storage_policy = '{policy}', old_parts_lifetime = 1, cleanup_delay_period = 1, "
            "max_cleanup_delay_period = 1".format(t=t, policy=STORAGE_POLICY)
        )
    since = node.query("SELECT toString(now())").strip()

    # (2) Several inserts each -> distinct content blobs + one immutable ref-log transaction per insert.
    for t in ("ref_t1", "ref_t2"):
        for i in range(NUM_INSERTS):
            node.query(
                "INSERT INTO {} SELECT 1, number + {off}, toString(number + {off}) "
                "FROM numbers({rows})".format(t, off=i * NUM_ROWS, rows=NUM_ROWS)
            )

    assert int(node.query("SELECT count() FROM ref_t1")) == NUM_INSERTS * NUM_ROWS
    assert int(node.query("SELECT count() FROM ref_t2")) == NUM_INSERTS * NUM_ROWS

    # The snapshot+log ref format is actually in use: immutable ref objects exist under cas/ns/stream/.
    assert _count(REFS_PREFIX) > 0, "expected ref log/snapshot objects under cas/ns/stream/"
    inserted = _blob_keys() - baseline
    assert inserted, "the inserts wrote no blob"

    # Read-only fsck agrees while data is present: no authoritative ref names a missing object.
    live_fsck = _disks(node, "cas-fsck")
    assert "dangling=0" in live_fsck, live_fsck

    # (3) Rename one table: data must survive (its ref namespace and its logs/snapshots are unaffected).
    node.query("RENAME TABLE ref_t1 TO ref_t1_renamed")
    assert (
        int(node.query("SELECT count() FROM ref_t1_renamed")) == NUM_INSERTS * NUM_ROWS
    )

    # (4) Drop the partition of both tables. Not DROP TABLE: that removes the parts' Keeper records with the
    #     table's path, so nothing is left to drive the reclamation.
    node.query("ALTER TABLE ref_t1_renamed DROP PARTITION 1")
    node.query("ALTER TABLE ref_t2 DROP PARTITION 1")

    # (5) THE RECLAMATION: every blob the inserts (and the merges of their parts) wrote goes. DROP's empty
    #     covering part is a local part with no record: its blob is not the GC's to reclaim.
    left = inserted & _blob_keys()
    for _ in range(RECLAIM_RETRIES):
        if not left:
            break
        time.sleep(RECLAIM_SLEEP)
        left = inserted & _blob_keys()
    assert not left, "{} of the {} blobs of the dropped partitions were not reclaimed".format(
        len(left), len(inserted)
    )

    #     Cross-checked against GC's own bookkeeping: a pool that shrank for some other reason must not pass
    #     for a round that reclaimed it.
    node.query("SYSTEM FLUSH LOGS")
    deleted = int(
        node.query(
            "SELECT sum(objects_deleted) FROM system.cas_gc_log "
            "WHERE event_type = 'Finish' AND outcome = 'Success' AND event_time >= '{}'".format(since)
        ).strip()
        or 0
    )
    assert deleted > 0, "the blobs are gone but GC's own bookkeeping reports no deletion"

    # (6) Read-only fsck on the reclaimed pool: no authoritative ref names a deleted blob. `unreachable` is not
    #     asserted: the writer's metadata and the empty covering part's blob stay behind.
    final_fsck = _disks(node, "cas-fsck")
    assert "dangling=0" in final_fsck, final_fsck

    node.query("DROP TABLE ref_t1_renamed SYNC")
    node.query("DROP TABLE ref_t2 SYNC")
