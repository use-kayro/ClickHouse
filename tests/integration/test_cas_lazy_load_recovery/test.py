import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_shared"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node1",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node1.xml"],
        macros={"replica": "node1"},
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


def _create(node):
    # lazy_load_tables=1: the CAS table attaches as a proxy and its real storage is built on first
    # access.
    node.query("CREATE DATABASE IF NOT EXISTS lazy_db ENGINE = Atomic SETTINGS lazy_load_tables = 1")
    node.query(
        "CREATE TABLE IF NOT EXISTS lazy_db.t (k UInt64, v UInt64) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/lazy_t', '{replica}') "
        "ORDER BY k SETTINGS storage_policy = '%s', min_bytes_for_wide_part = 0" % STORAGE_POLICY
    )


def test_lazy_cas_table_build_survives_an_object_store_outage(start_cluster):
    node = cluster.instances["node1"]
    _create(node)
    node.query("INSERT INTO lazy_db.t SELECT number, number FROM numbers(100)")
    assert node.query("SELECT count() FROM lazy_db.t").strip() == "100"

    # Restart so the table re-attaches as a lazy proxy (its real storage is not yet constructed; the
    # disk mounts at startup while S3 is up, the storage is built only on first access below).
    node.restart_clickhouse()

    # TEC-851 step 3c: the first-access build re-attaches the parts from their Keeper records; loading a
    # part still reads its marks (blobs). Started while S3 is unreachable, the build waits on that read and
    # completes once S3 is back, with every part.
    result = {}

    def first_access():
        result["count"] = node.query("SELECT count() FROM lazy_db.t", timeout=300).strip()

    with cluster.pause_container("rustfs1", wait_for_paused=False):
        reader = threading.Thread(target=first_access)
        reader.start()
        deadline = time.time() + 60
        while node.query("SELECT count() FROM system.processes WHERE query LIKE 'SELECT count() FROM lazy_db.t%'").strip() == "0":
            assert time.time() < deadline, "the first access never started"
            time.sleep(0.5)
    reader.join(300)
    assert not reader.is_alive(), "the first access did not finish after S3 came back"
    assert result.get("count") == "100"

    assert node.query("SELECT sum(v) FROM lazy_db.t").strip() == str(sum(range(100)))
