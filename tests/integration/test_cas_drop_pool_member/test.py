import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

# Both servers mount the SAME content-addressed pool over RustFS (not MinIO -- MinIO cannot serve CA
# pools, see memory), distinct server_root_id (node1/node2) -- exactly the shared-pool model test's
# two-node topology (test_cas_shared_pool), just on rustfs instead of minio (the model
# test predates rustfs support; test_cas_ref_snaplog is the rustfs precedent copied here).
STORAGE_POLICY = "cas_dpm"
RO_DISK = "disk_ca_ro"
CA_DISK = "disk_cas_dpm"

SRID1 = "node1"
SRID2 = "node2"

POOL = "cas_dpm_data"
BLOBS_PREFIX = POOL + "/blobs/"

NUM_ROWS = 20000


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
    cluster.add_instance(
        "node2",
        main_configs=["configs/storage_conf.xml", "configs/server_root_id_node2.xml"],
        macros={"replica": "node2"},
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
        list(cluster.rustfs_client.list_objects(cluster.rustfs_bucket, prefix, recursive=True))
    )


def test_drop_dead_pool_member():
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    node1.query("DROP TABLE IF EXISTS t1 SYNC")
    node2.query("DROP TABLE IF EXISTS t2 SYNC")

    blobs_baseline = _count(BLOBS_PREFIX)

    create_tpl = (
        "CREATE TABLE {tbl} (id Int64, s String) "
        "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{tbl}', '{{replica}}') ORDER BY id "
        "SETTINGS storage_policy = '{policy}'"
    )

    # (2) node2 gets its own table with several parts -- this data must NOT survive (node2 is about to
    #     be killed and decommissioned).
    node2.query(create_tpl.format(tbl="t2", policy=STORAGE_POLICY))
    for i in range(4):
        node2.query(
            "INSERT INTO t2 SELECT number + {off}, toString(number + {off}) "
            "FROM numbers({rows})".format(off=i * NUM_ROWS, rows=NUM_ROWS)
        )
    assert int(node2.query("SELECT count() FROM t2")) == 4 * NUM_ROWS

    # (3) node1 gets its own table -- this data MUST survive the whole flow untouched.
    node1.query(create_tpl.format(tbl="t1", policy=STORAGE_POLICY))
    node1.query(
        "INSERT INTO t1 SELECT number, toString(number) FROM numbers({})".format(NUM_ROWS)
    )
    n1_count = int(node1.query("SELECT count() FROM t1"))
    n1_sum = int(node1.query("SELECT sum(id) FROM t1"))
    assert n1_count == NUM_ROWS

    assert _count(BLOBS_PREFIX) > blobs_baseline, "expected content blobs after both nodes' inserts"

    # (4) Hard-kill node2: SIGKILL, no graceful farewell -- node2's mount lease is left to expire
    #     naturally, exactly the scenario decommission exists for.
    node2.stop_clickhouse(kill=True)

    # (5) Wait until node1 observes node2's mount as no longer live (expired once its lease's TTL
    #     elapses with no renewal, since there was no graceful farewell to mark it terminated instead).
    #     min() because node1 sees the pool through TWO disks (the writable disk + the disk_ca_ro
    #     read-only window), so the mounts table carries one row per disk view for the same server_root_id --
    #     aggregate to a single row for the equality assert.
    assert_eq_with_retry(
        node1,
        "SELECT min(state != 'live') FROM system.cas_mounts WHERE server_root_id = '{}'".format(
            SRID2
        ),
        "1",
        retry_count=90,
        sleep_time=1.0,
    )

    # (6) Decommission the dead member from node1. SYSTEM queries do
    #     not accept a FORMAT clause (ParserSystemQuery is not part of ParserQueryWithOutput), so parse
    #     the default TSV row. Column order matches the interpreter's ColumnsDescription:
    #     server_root_id, namespaces_removed, namespaces_already_removed, committed_refs_removed,
    #     precommits_removed, manifest_debris_removed, staging_objects_removed,
    #     mountpoint_objects_removed, slot_removed, warnings.
    #
    #     t2 is still `Live` at this point, so THIS call's normal drop path is what appends its
    #     removal terminal and moves its catalog row to `Removing`; the decommission never deletes a
    #     catalog row, so that row still counts as "owned" and the retirement fence refuses the slot.
    #     The mounts-table poll above and the decommission command judge liveness by DIFFERENT
    #     predicates on purpose: the table renders TTL arithmetic over the last observed mount row,
    #     while the command re-reads the mountpoint object and refuses while the lease could still
    #     be live under its conservative safety margin. The destructive side being stricter is the
    #     fail-close direction, so the table saying "not live" does not guarantee the command is
    #     ready yet -- under sanitizer slowdowns the gap is wide enough to hit. Retry the command
    #     itself through the documented "wait for its lease to lapse" refusal, bounded.
    report_tsv = None
    for _ in range(90):
        try:
            report_tsv = node1.query(
                "SYSTEM CAS DROP POOL MEMBER '{}' FROM DISK '{}'".format(SRID2, CA_DISK)
            ).rstrip("\n")
            break
        except Exception as e:
            if "alive or contended" not in str(e):
                raise
            time.sleep(1.0)
    assert report_tsv is not None, "decommission kept refusing: lease never lapsed within the bound"
    fields = report_tsv.split("\t")
    assert len(fields) == 10, report_tsv
    assert fields[0] == SRID2, report_tsv
    assert int(fields[1]) >= 1, report_tsv  # namespaces_removed: the drop half did its work
    assert int(fields[8]) == 0, report_tsv  # slot_removed: t2's `Removing` row is still owned
    assert "pool member decommission incomplete" in fields[9], report_tsv

    # (7) node1's own data survives the decommission untouched.
    assert int(node1.query("SELECT count() FROM t1")) == n1_count
    assert int(node1.query("SELECT sum(id) FROM t1")) == n1_sum


def test_drop_pool_member_rejected_on_readonly_disk():
    # disk_ca_ro is the fail-close guard's target: an observe-only window over the SAME pool.
    # Decommission is a mutating operation, so it must be rejected on this disk exactly like
    # `createTransaction`/GC round are -- READONLY, not a silent no-op or a crash further down the call chain.
    node1 = cluster.instances["node1"]
    err = node1.query_and_get_error(
        "SYSTEM CAS DROP POOL MEMBER 'whatever' FROM DISK '{}'".format(RO_DISK)
    )
    assert "read-only" in err, err
