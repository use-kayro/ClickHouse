"""TEC-851, step 3c: no CAS metadata in the object store.

A writer and two readers share one content-addressed pool. The only objects any node writes are blob bodies
(`blobs/...`), and only the writer -- the server whose replicas can become merge leader -- writes them. A part's
metadata is its Keeper record (`<zk>/cas_parts/<part>`); every node holds its parts in memory and re-attaches
them from the records on restart, with no LIST. Liveness and dedup come from the records: identical content is
uploaded once; the writer's record GC removes a record no replica lists (fenced by its `is_active`/`host`) and
deletes the blobs nothing references by exact ETag; a part not yet committed to Keeper keeps its blobs.

Object-store writes and deletes are read per key from `system.blob_storage_log`; LIST from `system.events`
(`S3ListObjects` counts every LIST since the server started).
"""

import contextlib
import shlex
import threading
import time

import pytest

from helpers.cas import join_pool_after_writer
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

POLICY = "cas_shared"
NODES = ("writer", "reader1", "reader2")
READERS = ("reader1", "reader2")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    for name in NODES:
        configs = ["configs/logs.xml"]
        if name == "writer":
            configs += ["configs/storage_conf.xml", "configs/server_root_id_writer.xml"]
        else:
            configs.append("configs/reader_role.xml")
        cluster.add_instance(
            name,
            main_configs=configs,
            macros={"replica": name},
            with_rustfs=True,
            with_zookeeper=True,
            keeper_required_feature_flags=["create_if_not_exists"],  # Keeper part records (TEC-849)
            stay_alive=True,
        )
    try:
        cluster.start()
        for name in READERS:
            join_pool_after_writer(
                cluster.instances[name],
                cluster.base_dir,
                ["configs/storage_conf.xml", "configs/server_root_id_{}.xml".format(name)],
            )
        yield cluster
    finally:
        cluster.shutdown()


def node(name):
    return cluster.instances[name]


def wait_until(predicate, timeout, what):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline, "timed out waiting for {}".format(what)
        time.sleep(0.5)


def now(n):
    return n.query("SELECT now64(3)").strip()


def object_writes(n, since):
    """(remote_path, event_type) of every object-store write and delete `n` logged since `since`."""
    n.query("SYSTEM FLUSH LOGS")
    out = n.query(
        "SELECT event_type, remote_path FROM system.blob_storage_log "
        "WHERE event_time_microseconds >= '{}' AND disk_name = 'disk_cas_shared' FORMAT TSV".format(since)
    )
    return [tuple(line.split("\t")) for line in out.splitlines() if line.strip()]


def is_blob(path):
    return "/blobs/" in path and not path.endswith(".meta")


def s3_lists_since_boot(n):
    return int(
        n.query(
            "SELECT sum(value) FROM system.events WHERE event IN ('S3ListObjects', 'DiskS3ListObjects')"
        ).strip()
        or 0
    )


def log_lines(n, pattern):
    out = n.exec_in_container(
        [
            "bash",
            "-c",
            "grep -a -E {} /var/log/clickhouse-server/clickhouse-server.log || true".format(shlex.quote(pattern)),
        ]
    )
    return [line for line in out.splitlines() if line.strip()]


def create(table, extra=""):
    for name in NODES:
        node(name).query(
            """
            CREATE TABLE IF NOT EXISTS {t} (id UInt64, v UInt64, s String, PROJECTION p (SELECT s, sum(v) GROUP BY s))
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{t}', '{{replica}}')
            PARTITION BY id % 2 ORDER BY id
            SETTINGS storage_policy = '{p}', min_bytes_for_wide_part = 0, old_parts_lifetime = 1,
                     cleanup_delay_period = 1, max_cleanup_delay_period = 1 {extra}
            """.format(t=table, p=POLICY, extra=extra)
        )


def sync(table):
    for name in READERS:
        node(name).query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)


def answer(n, table):
    return n.query(
        "SELECT count(), sum(v), groupBitXor(cityHash64(id, v, s)) FROM {}".format(table)
    ).strip()


def records(table):
    zk = cluster.get_kazoo_client("zoo1")
    return sorted(zk.get_children("/clickhouse/tables/{}/cas_parts".format(table)))


def active(n, table):
    return n.query(
        "SELECT name FROM system.parts WHERE table = '{}' AND active ORDER BY name".format(table)
    ).split()


def test_writes_are_blob_bodies_only_and_nothing_lists():
    since = {name: now(node(name)) for name in NODES}
    create("t_data")
    for i in range(8):
        node("writer").query(
            "INSERT INTO t_data SELECT number + {o}, number * 7, toString(number % 97) FROM numbers(20000)".format(
                o=i * 20000
            )
        )
    node("writer").query("OPTIMIZE TABLE t_data FINAL")
    sync("t_data")

    expected = answer(node("writer"), "t_data")
    for name in READERS:
        assert answer(node(name), "t_data") == expected

    # Every part any replica lists has a record; the record set is exactly what the writer holds.
    wait_until(lambda: records("t_data") == active(node("writer"), "t_data"), 60, "records == active parts")

    for name in NODES:
        writes = object_writes(node(name), since[name])
        metadata = [w for w in writes if not is_blob(w[1])]
        assert metadata == [], "{} wrote metadata objects: {}".format(name, metadata[:10])
        assert s3_lists_since_boot(node(name)) == 0, "{} listed the object store".format(name)
    for name in READERS:
        assert [w for w in object_writes(node(name), since[name]) if w[0] != "Delete"] == [], (
            "a reader wrote to the object store"
        )
    assert any(w[0] != "Delete" for w in object_writes(node("writer"), since["writer"])), "the writer wrote no blob"


def test_restart_reattaches_from_records_without_list():
    create("t_data")
    before = {name: answer(node(name), "t_data") for name in NODES}
    for name in NODES:
        node(name).restart_clickhouse()
    for name in NODES:
        wait_until(lambda: answer(node(name), "t_data") == before[name], 120, "{} answers after restart".format(name))
        assert s3_lists_since_boot(node(name)) == 0, "{} listed the object store on restart".format(name)
        attached = log_lines(node(name), r"Attached [0-9]+ of [0-9]+ parts of this replica from their Keeper records")
        assert attached, "{} did not attach from records".format(name)
        last = attached[-1].split("Attached ")[1].split(" ")
        assert last[0] == last[2], "{}: {}".format(name, attached[-1])


def test_identical_content_is_uploaded_once():
    create("t_dedup")
    insert = (
        "INSERT INTO t_dedup SELECT number, number * 3, toString(number % 13) FROM numbers(30000) "
        "SETTINGS insert_deduplicate = 0"
    )
    w = node("writer")
    node("writer").query(insert)
    since = now(w)
    node("writer").query(insert)
    second = [x for x in object_writes(w, since) if x[0] != "Delete"]
    assert second == [], "identical content was uploaded again: {}".format(second[:10])
    sync("t_dedup")
    assert answer(node("reader1"), "t_dedup") == answer(w, "t_dedup")


def test_reader_does_not_build_parts():
    create("t_reader_insert")
    # Even when the reader holds every blob of the new part (identical content, deduplication off): the build
    # is refused before blob presence is consulted, since the writer's GC cannot see a reader's reference.
    row = "INSERT INTO t_reader_insert SETTINGS insert_deduplicate = 0 VALUES (1, 1, 'a')"
    node("writer").query(row)
    sync("t_reader_insert")
    err = node("reader1").query_and_get_error(row)
    assert "only the pool's writer builds parts" in err, err


def test_refusals_of_what_cannot_have_a_record():
    create("t_refuse")
    w = node("writer")
    w.query("INSERT INTO t_refuse VALUES (1, 1, 'a'), (2, 2, 'b')")
    for q in (
        "ALTER TABLE t_refuse FREEZE",
        "ALTER TABLE t_refuse DETACH PARTITION 1",
    ):
        err = w.query_and_get_error(q)
        assert "NOT_IMPLEMENTED" in err or "not supported on a content-addressed disk" in err, (q, err)
    err = w.query_and_get_error(
        "CREATE TABLE t_plain (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '{}'".format(POLICY)
    )
    assert "non-replicated MergeTree table cannot use the content-addressed disk" in err, err
    assert answer(w, "t_refuse").startswith("2\t")


def test_gc_deletes_unreferenced_blobs_and_spares_a_part_under_construction():
    """t_gc_a and t_gc_b hold identical content. A drop of t_gc_a's partition makes the writer's GC remove the
    records of its parts and consider their blobs, while an insert of the same content into t_gc_b is renamed
    (its part held on the writer) but not yet committed to Keeper: those blobs must survive. Blobs only the
    dropped parts referenced are deleted."""
    w = node("writer")
    create("t_gc_a")
    create("t_gc_b", extra=", sleep_before_commit_local_part_in_replicated_table_ms = 15000")
    shared = "SELECT number * 2, number, toString(number % 11) FROM numbers(10000)"
    only_a = "SELECT number * 2 + 1, number, 'only-a' FROM numbers(10000)"
    w.query("INSERT INTO t_gc_a {}".format(shared))      # partition 0
    w.query("INSERT INTO t_gc_a {}".format(only_a))      # partition 1
    sync("t_gc_a")

    since = now(w)
    inserter = threading.Thread(target=lambda: w.query("INSERT INTO t_gc_b {}".format(shared), timeout=120))
    inserter.start()
    time.sleep(2)   # the t_gc_b part is renamed and waits before its Keeper commit
    w.query("ALTER TABLE t_gc_a DROP PARTITION 0")
    w.query("ALTER TABLE t_gc_a DROP PARTITION 1")
    for name in READERS:
        node(name).query("SYSTEM SYNC REPLICA t_gc_a", timeout=120)
    wait_until(lambda: records("t_gc_a") == [], 60, "t_gc_a records removed")
    inserter.join()

    wait_until(lambda: any(e == "Delete" for e, _ in object_writes(w, since)), 60, "a GC delete")
    sync("t_gc_b")
    expected = answer(w, "t_gc_b")
    assert expected.startswith("10000\t")
    for name in READERS:
        node(name).query("SYSTEM DROP FILESYSTEM CACHE")
        assert answer(node(name), "t_gc_b") == expected
    # Readers never delete; every delete is the writer's, of a blob body.
    for name in READERS:
        assert [x for x in object_writes(node(name), since) if x[0] == "Delete"] == []
    assert all(is_blob(p) for e, p in object_writes(w, since) if e == "Delete")
    w.query("SYSTEM FLUSH LOGS")
    assert int(w.query(
        "SELECT sum(objects_deleted) FROM system.cas_gc_log WHERE event_type = 'Finish' AND event_time >= '{}'".format(since[:19])
    ).strip()) > 0


def test_attach_without_keeper_is_refused():
    """A part on a content-addressed disk exists only in its Keeper record: a table attached while Keeper is
    unreachable must not come up empty (its replica would then fetch, and the writer's GC remove, every part).
    The ATTACH is refused, and succeeds with every part once Keeper is back."""
    create("t_nokeeper")
    w = node("writer")
    w.query("INSERT INTO t_nokeeper SELECT number, number, 'k' FROM numbers(1000)")
    expected = answer(w, "t_nokeeper")
    w.query("DETACH TABLE t_nokeeper")
    # Every Keeper node: with one paused, the other two still form a quorum.
    with contextlib.ExitStack() as paused:
        for keeper in ("zoo1", "zoo2", "zoo3"):
            paused.enter_context(cluster.pause_container(keeper, wait_for_paused=False))
        err = w.query_and_get_error("ATTACH TABLE t_nokeeper", timeout=180)
        # Refused at construction: NO_ZOOKEEPER without a session, the Keeper error itself with a dead one.
        assert "does not start without Keeper" in err or "Coordination" in err, err
    assert w.query("EXISTS TABLE t_nokeeper").strip() == "0"

    def attached():
        try:
            w.query("ATTACH TABLE t_nokeeper")
        except Exception:
            pass
        return w.query("EXISTS TABLE t_nokeeper").strip() == "1"

    wait_until(attached, 120, "ATTACH after Keeper is back")
    wait_until(lambda: answer(w, "t_nokeeper") == expected, 120, "t_nokeeper parts back")
