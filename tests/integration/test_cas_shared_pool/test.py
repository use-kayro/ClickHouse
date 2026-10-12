import time

import pytest

from helpers.cas import join_pool_after_writer
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Both servers mount the SAME content-addressed pool (endpoint .../test/shared_pool/) as the two replicas of
# one ReplicatedMergeTree table: node1 writes (merge leader, the only server whose GC deletes), node2 reads.
STORAGE_POLICY = "cas_shared"

# Content blobs of the shared pool inside the `test` RustFS bucket.
BLOBS_PREFIX = "shared_pool/blobs/"

# Deterministic data.
NUM_ROWS = 100000

# The GC runs every second; an unlisted part's record is removed `old_parts_lifetime` (1 s here) after the
# GC marks it, and its blobs right after. Bounded poll on that background process.
RECLAIM_RETRIES = 60
RECLAIM_SLEEP = 1.0  # seconds; total bound ~= 60s


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    # RustFS (not MinIO) backs the pool: the record GC's exact-ETag DELETE needs enforced conditional-DELETE
    # semantics, which MinIO OSS lacks. Both instances reach the shared rustfs1 and load the identical
    # storage_conf.xml, so both mount the SAME shared pool; node2 is a reader (`configs/reader_role.xml`) and
    # mounts it once node1 has minted `_pool_meta`.
    for name in ("node1", "node2"):
        cluster.add_instance(
            name,
            main_configs=([] if name == "node2" else ["configs/storage_conf.xml", "configs/server_root_id_node1.xml"])
            + (["configs/reader_role.xml"] if name == "node2" else []),
            macros={"replica": name},
            with_rustfs=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["create_if_not_exists"],  # Keeper part records (TEC-849)
            stay_alive=True,
        )

    try:
        cluster.start()
        join_pool_after_writer(
            cluster.instances["node2"], cluster.base_dir, ["configs/storage_conf.xml", "configs/server_root_id_node2.xml"]
        )
        yield cluster
    finally:
        cluster.shutdown()


def blob_keys():
    objects = cluster.rustfs_client.list_objects(cluster.rustfs_bucket, BLOBS_PREFIX, recursive=True)
    return {o.object_name for o in objects}


def _gc_bookkeeping(node, writer, since):
    """(successful rounds that ran as the writer or not, objects deleted by any round) of `node`'s CA GC since
    `since`."""
    node.query("SYSTEM FLUSH LOGS")
    if node.query("EXISTS TABLE system.cas_gc_log").strip() != "1":
        return 0, 0   # a server that never ran a round has no rows
    rounds, deleted = node.query(
        "SELECT countIf(outcome = 'Success' AND writer = {}), sum(objects_deleted) FROM system.cas_gc_log "
        "WHERE event_type = 'Finish' AND event_time >= '{}' FORMAT TSV".format(int(writer), since)
    ).split("\t")
    return int(rounds), int(deleted)


def test_two_servers_share_one_pool():
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS t SYNC")

    # (0) The pool's blobs before the table exists.
    baseline = blob_keys()

    # (1) node1 and node2 hold the two replicas of one table on the shared pool.
    create_tpl = (
        "CREATE TABLE {tbl} (p UInt8, id Int64, v UInt64, s String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{tbl}', '{{replica}}') "
        "PARTITION BY p ORDER BY id "
        "SETTINGS storage_policy = '{policy}', old_parts_lifetime = 1, "
        "cleanup_delay_period = 1, max_cleanup_delay_period = 1{extra}"
    )
    for node in (node1, node2):
        node.query(create_tpl.format(tbl="t", policy=STORAGE_POLICY, extra=""))
    since = node1.query("SELECT toString(now())").strip()

    # (2) INSERT on the writer; the reader fetches the same parts. Logical reads must be correct on each.
    node1.query(
        "INSERT INTO t SELECT 1, number, number * 10, toString(number) FROM numbers({})".format(NUM_ROWS)
    )
    node2.query("SYSTEM SYNC REPLICA t", timeout=120)

    expected_sum_id = (NUM_ROWS - 1) * NUM_ROWS // 2
    for node in (node1, node2):
        assert int(node.query("SELECT count() FROM t")) == NUM_ROWS
        assert int(node.query("SELECT sum(id) FROM t")) == expected_sum_id

    written = blob_keys() - baseline
    assert written, "the insert wrote no blob"

    # (3) Heavy mutations / merges: UPDATE (id/s carry forward by reference), DELETE, then OPTIMIZE FINAL.
    node1.query("ALTER TABLE t UPDATE v = v + 1 WHERE id % 2 = 0 SETTINGS mutations_sync = 2")
    node1.query("ALTER TABLE t DELETE WHERE id % 100 = 0 SETTINGS mutations_sync = 2")
    node1.query("OPTIMIZE TABLE t FINAL")
    node2.query("SYSTEM SYNC REPLICA t", timeout=120)
    written |= blob_keys() - baseline

    count_after_mut = int(node1.query("SELECT count() FROM t"))
    sum_after_mut = int(node1.query("SELECT sum(v) FROM t"))
    digest_after_mut = node1.query("SELECT sum(cityHash64(id, v, s)) FROM t").strip()

    # (4) Let the writer's GC run rounds over the parts the mutations and the merge left unlisted while the
    #     live parts share their blobs: no round may reclaim a blob a live part on either replica references.
    mutated_at = node1.query("SELECT toString(now())").strip()
    for _ in range(RECLAIM_RETRIES):
        if _gc_bookkeeping(node1, True, mutated_at)[0] >= 5:
            break
        time.sleep(RECLAIM_SLEEP)
    assert _gc_bookkeeping(node1, True, mutated_at)[0] >= 5, "the writer's GC did not run five rounds"

    for node in (node1, node2):
        assert int(node.query("SELECT count() FROM t")) == count_after_mut
        assert int(node.query("SELECT sum(v) FROM t")) == sum_after_mut
        assert node.query("SELECT sum(cityHash64(id, v, s)) FROM t").strip() == digest_after_mut

    # (5) THE RECLAMATION: every blob the table wrote goes once its partition is dropped.
    node1.query("ALTER TABLE t DROP PARTITION 1")
    node2.query("SYSTEM SYNC REPLICA t", timeout=120)

    left = written & blob_keys()
    for _ in range(RECLAIM_RETRIES):
        if not left:
            break
        time.sleep(RECLAIM_SLEEP)
        left = written & blob_keys()
    assert not left, "{} of the {} blobs of the dropped partition were not reclaimed".format(
        len(left), len(written)
    )
    assert node1.query(
        "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/t/cas_parts'"
    ).strip() == "0"

    rounds, deleted = _gc_bookkeeping(node1, True, since)
    assert rounds > 0, "no GC round led by the writer"
    assert deleted > 0, "the blobs went but the writer's GC reports no deletion"
    # Only the writer runs GC rounds: the reader logs none.
    assert _gc_bookkeeping(node2, False, since) == (0, 0)

    # DROP TABLE removes the records with the table's Keeper path: its blobs are not the GC's to reclaim.
    for node in (node1, node2):
        node.query("DROP TABLE t SYNC")


CRASH_ROWS = 50000


def test_pool_survives_node_crash():
    # Proves the pool survives a hard crash of the writer:
    #   (a) the surviving reader keeps reading and deletes nothing (it runs no GC round);
    #   (b) the hard-killed writer recovers its data on restart (from the Keeper part records);
    #   (c) the restarted writer's GC reclaims every blob of the table once its partition is dropped.
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    for node in (node1, node2):
        node.query("DROP TABLE IF EXISTS crash SYNC")

    # (0) The pool's blobs before the table exists.
    baseline = blob_keys()

    create_tpl = (
        "CREATE TABLE {tbl} (p UInt8, id Int64, v UInt64, s String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{tbl}', '{{replica}}') "
        "PARTITION BY p ORDER BY id "
        "SETTINGS storage_policy = '{policy}', old_parts_lifetime = 1, "
        "cleanup_delay_period = 1, max_cleanup_delay_period = 1{extra}"
    )
    for node in (node1, node2):
        node.query(create_tpl.format(tbl="crash", policy=STORAGE_POLICY, extra=""))

    node1.query(
        "INSERT INTO crash SELECT 1, number, number * 10, toString(number) "
        "FROM numbers({rows})".format(rows=CRASH_ROWS)
    )
    node2.query("SYSTEM SYNC REPLICA crash", timeout=120)

    # Capture the authoritative aggregates BEFORE the crash; recovery must reproduce them exactly.
    n1_count = int(node1.query("SELECT count() FROM crash"))
    n1_sum_id = int(node1.query("SELECT sum(id) FROM crash"))
    n1_digest = node1.query("SELECT sum(cityHash64(id, v, s)) FROM crash").strip()
    assert n1_count == CRASH_ROWS

    written = blob_keys() - baseline
    assert written, "the insert wrote no blob"

    # (1) HARD-KILL node1 (SIGKILL via pkill -9 => simulated crash). node2 stays up.
    node1.stop_clickhouse(kill=True)

    # (2) With the writer down, node2 keeps reading; as a reader it runs no GC round and deletes nothing,
    #     so every blob the writer wrote stays.
    killed_at = node2.query("SELECT toString(now())").strip()
    time.sleep(5)
    assert _gc_bookkeeping(node2, False, killed_at) == (0, 0)

    assert int(node2.query("SELECT count() FROM crash")) == n1_count
    assert node2.query("SELECT sum(cityHash64(id, v, s)) FROM crash").strip() == n1_digest
    missing = written - blob_keys()
    assert not missing, "{} blobs of the table went while the writer was down".format(len(missing))

    # (3) RESTART node1: it re-attaches its active set from the Keeper part records and must re-read its
    #     table with the EXACT pre-crash count/sum/digest.
    node1.start_clickhouse()
    restarted_at = node1.query("SELECT toString(now())").strip()

    assert int(node1.query("SELECT count() FROM crash")) == n1_count
    assert int(node1.query("SELECT sum(id) FROM crash")) == n1_sum_id
    assert node1.query("SELECT sum(cityHash64(id, v, s)) FROM crash").strip() == n1_digest

    # node2 still consistent after node1 rejoined.
    assert int(node2.query("SELECT count() FROM crash")) == n1_count

    # (4) THE RECLAMATION: the crash left nothing that survives the drop of the partition.
    node1.query("ALTER TABLE crash DROP PARTITION 1")
    node2.query("SYSTEM SYNC REPLICA crash", timeout=120)

    left = written & blob_keys()
    for _ in range(RECLAIM_RETRIES):
        if not left:
            break
        time.sleep(RECLAIM_SLEEP)
        left = written & blob_keys()
    assert not left, "{} of the {} blobs of the dropped partition were not reclaimed".format(
        len(left), len(written)
    )
    assert node1.query(
        "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/crash/cas_parts'"
    ).strip() == "0"

    rounds, deleted = _gc_bookkeeping(node1, True, restarted_at)
    assert rounds > 0, "no GC round led by the writer"
    assert deleted > 0, "the blobs went but the writer's GC reports no deletion"
    # Only the writer runs GC rounds.
    assert _gc_bookkeeping(node2, False, restarted_at) == (0, 0)

    # DROP TABLE removes the records with the table's Keeper path: its blobs are not the GC's to reclaim.
    for node in (node1, node2):
        node.query("DROP TABLE crash SYNC")
