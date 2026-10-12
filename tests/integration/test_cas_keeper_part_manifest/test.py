"""TEC-849, step 1 of keeping CAS part metadata in Keeper.

A writer that commits a part on a content-addressed disk writes, in the same multi-op as its part znode, the
part's Keeper record -- one copy per part, at the table level (`<zookeeper_path>/cas_parts/<part>`): the files
whose bytes exist only inside a manifest, plus the pool. A reader that executes the replication log entry attaches the part from that record and
the blobs the writer already put in the shared pool -- nothing is fetched from the writer, nothing is
written to or listed in the object store. On restart the reader re-attaches its parts from its own records.

The S3 counters of an attach are read from `system.part_log` (`DownloadPart` rows carry the fetch's own
ProfileEvents), so the reader's mount-lease renewals in the background do not blur them.
"""

import json
import re
import shlex
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

STORAGE_POLICY = "cas_shared"
CA_DISK = "disk_cas_shared"
NUM_INSERTS = 10
ROWS_PER_INSERT = 5000
S3_WRITE_AND_LIST_EVENTS = (
    "S3PutObject",
    "S3CreateMultipartUpload",
    "S3UploadPart",
    "S3CompleteMultipartUpload",
    "S3CopyObject",
    "S3DeleteObjects",
    "S3ListObjects",
    "DiskS3PutObject",
    "DiskS3ListObjects",
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    for name in ("writer", "reader"):
        cluster.add_instance(
            name,
            main_configs=[
                "configs/storage_conf.xml",
                "configs/server_root_id_{}.xml".format(name),
                "configs/part_log.xml",
            ],
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


def wait_until(predicate, timeout, what):
    deadline = time.time() + timeout
    while not predicate():
        assert time.time() < deadline, "timed out waiting for {}".format(what)
        time.sleep(0.5)


def create_table(node, table, wide, reader, cleanup_period=1):
    settings = [
        "storage_policy = '{}'".format(STORAGE_POLICY),
        "min_bytes_for_wide_part = {}".format(0 if wide else 1 << 30),
        "old_parts_lifetime = 1",
        "cleanup_delay_period = {}".format(cleanup_period),
        "max_cleanup_delay_period = {}".format(cleanup_period),
        "merge_tree_clear_old_temporary_directories_interval_seconds = {}".format(cleanup_period),
    ]
    if reader:
        # As production readers: they never assign merges and never run them, they fetch the writer's result.
        settings += ["always_fetch_merged_part = 1", "replicated_can_become_leader = 0"]
    node.query(
        """
        CREATE TABLE {table} (
            id UInt64,
            v UInt64,
            s String,
            n Nullable(UInt32),
            INDEX idx_v v TYPE minmax GRANULARITY 1,
            PROJECTION by_s (SELECT s, sum(v), count() GROUP BY s)
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}', '{{replica}}')
        PARTITION BY id % 2
        ORDER BY id
        SETTINGS {settings}
        """.format(table=table, settings=", ".join(settings))
    )


def results(node, table):
    """The answers the reader must reproduce exactly: full data hash, aggregates, projection, index."""
    return {
        "count": node.query("SELECT count() FROM {}".format(table)).strip(),
        "data": node.query(
            "SELECT groupBitXor(cityHash64(id, v, s, n)), sum(v), uniqExact(s) FROM {}".format(table)
        ).strip(),
        "projection": node.query(
            "SELECT s, sum(v), count() FROM {} GROUP BY s ORDER BY s LIMIT 20 "
            "SETTINGS optimize_use_projections = 1, force_optimize_projection = 1".format(table)
        ).strip(),
        "index": node.query(
            "SELECT count() FROM {} WHERE v BETWEEN 1000 AND 2000 SETTINGS force_data_skipping_indices = 'idx_v'".format(
                table
            )
        ).strip(),
    }


def active_parts(node, table):
    return node.query(
        "SELECT name FROM system.parts WHERE table = '{}' AND active ORDER BY name".format(table)
    ).split()


def record_lengths(node, table):
    """`dataLength` of every Keeper part record of `table`, keyed by part name."""
    out = node.query(
        "SELECT name, dataLength FROM system.zookeeper WHERE path = '/clickhouse/tables/{}/cas_parts' "
        "ORDER BY name FORMAT TSV".format(table)
    )
    return {name: int(length) for name, length in (line.split("\t") for line in out.splitlines())}


def replica_part_znodes(node, table, replica):
    return node.query(
        "SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/{}/replicas/{}/parts' "
        "ORDER BY name".format(table, replica)
    ).split()


def download_rows(node, table, since):
    node.query("SYSTEM FLUSH LOGS")
    columns = ", ".join(
        "sum(ProfileEvents['{0}']) AS {0}".format(e) for e in S3_WRITE_AND_LIST_EVENTS
    )
    row = node.query(
        "SELECT count() AS parts, {columns}, sum(ProfileEvents['S3GetObject']) AS S3GetObject "
        "FROM system.part_log WHERE table = '{table}' AND event_type = 'DownloadPart' "
        "AND event_time >= toDateTime('{since}') FORMAT JSONEachRow".format(
            columns=columns, table=table, since=since
        )
    )
    return {k: int(v) for k, v in json.loads(row).items()}


def global_events(node, names):
    rows = node.query(
        "SELECT event, value FROM system.events WHERE event IN ({}) FORMAT TSV".format(
            ", ".join("'{}'".format(n) for n in names)
        )
    )
    values = {name: 0 for name in names}
    for line in rows.splitlines():
        event, value = line.split("\t")
        values[event] = int(value)
    return values


def server_now(node):
    return node.query("SELECT toString(now() - 1)").strip()


def writer_created_parts(writer, table):
    """Every part the writer produced for `table` (inserts and merges), from its `system.part_log`."""
    writer.query("SYSTEM FLUSH LOGS")
    return writer.query(
        "SELECT DISTINCT part_name FROM system.part_log WHERE table = '{}' AND error = 0 "
        "AND event_type IN ('NewPart', 'MergeParts') ORDER BY part_name".format(table)
    ).split()


@pytest.mark.parametrize("wide", [True, False], ids=["wide", "compact"])
def test_reader_attaches_parts_from_keeper(wide):
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table = "keeper_parts_{}".format("wide" if wide else "compact")
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    create_table(writer, table, wide, reader=False)
    create_table(reader, table, wide, reader=True)
    since = server_now(reader)

    # N inserts on the writer; the writer merges in the background as it normally does, and a final
    # OPTIMIZE adds one more merge per partition. The reader replicates every part, inserted or merged.
    for i in range(NUM_INSERTS):
        writer.query(
            "INSERT INTO {table} SELECT number, number % 7919, toString(number % 97), "
            "if(number % 5 = 0, NULL, toUInt32(number)) FROM numbers({start}, {rows})".format(
                table=table, start=i * ROWS_PER_INSERT, rows=ROWS_PER_INSERT
            )
        )
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    assert results(reader, table) == results(writer, table)
    writer.query("OPTIMIZE TABLE {} FINAL".format(table))
    for node in (writer, reader):
        node.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)

    active = active_parts(writer, table)
    assert active_parts(reader, table) == active
    assert results(reader, table) == results(writer, table)

    # Every part has its record, one per part name at the table level; the replicas' own part znodes follow
    # their active parts as always.
    created = writer_created_parts(writer, table)
    assert len(created) >= NUM_INSERTS * 2  # two partitions per insert, plus the merges
    records = record_lengths(writer, table)
    assert set(active) <= set(records) and set(records) <= set(created), (sorted(records), created)
    for replica in ("writer", "reader"):
        wait_until(
            lambda: replica_part_znodes(writer, table, replica) == active,
            60,
            "{}'s part znodes to follow its active parts".format(replica),
        )

    # Every part the reader got arrived through its Keeper record: the attach line, never relink, never
    # bytes, and the writer sent nothing.
    downloads = download_rows(reader, table, since)
    attached = log_lines(reader, r"default\.{} .*Attached part .* from its Keeper record".format(table))
    assert downloads["parts"] > 0 and len(attached) == downloads["parts"], (downloads, attached[:3])
    assert not log_lines(reader, r"default\.{} .*Relink of part ".format(table))
    assert not log_lines(reader, r"default\.{} .*Download of part ".format(table))
    assert not log_lines(writer, r"default\.{} .*Sending part ".format(table))

    # And the attaches wrote nothing to and listed nothing in the object store.
    for event in S3_WRITE_AND_LIST_EVENTS:
        assert downloads[event] == 0, (event, downloads)

    # Leave the reader with only the table each later test creates, so their boot counters compare.
    for node in (writer, reader):
        node.query("DROP TABLE {} SYNC".format(table))


def test_reader_restart_reattaches_from_keeper():
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table = "keeper_parts_restart"
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    # The table's periodic cleanup lists its directory (the namespace-files emulation, TEC-845 (C)); pushing
    # it out of the measured window keeps two boots comparable.
    create_table(writer, table, wide=True, reader=False, cleanup_period=3600)
    create_table(reader, table, wide=True, reader=True, cleanup_period=3600)

    list_events = ["S3ListObjects", "DiskS3ListObjects", "CASRootList", "CASOtherList", "CASManifestList", "CASBlobList"]

    def boot_lists():
        reader.restart_clickhouse()
        reader.query("SELECT count() FROM {}".format(table))  # waits for the table to be loaded
        wait_until(
            lambda: reader.query(
                "SELECT is_readonly = 0 AND queue_size = 0 FROM system.replicas WHERE table = '{}'".format(table)
            ).strip() == "1",
            60,
            "the reader's replica to be active with an empty queue",
        )
        return global_events(reader, list_events)

    # Baseline: a reader boot with the table and no parts.
    empty_boot = boot_lists()

    for i in range(NUM_INSERTS):
        writer.query(
            "INSERT INTO {table} SELECT number, number % 7919, toString(number % 97), "
            "if(number % 5 = 0, NULL, toUInt32(number)) FROM numbers({start}, {rows})".format(
                table=table, start=i * ROWS_PER_INSERT, rows=ROWS_PER_INSERT
            )
        )
    writer.query("OPTIMIZE TABLE {} FINAL".format(table))
    for node in (writer, reader):
        node.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    expected = results(writer, table)
    parts = active_parts(writer, table)
    assert results(reader, table) == expected

    full_boot = boot_lists()
    assert active_parts(reader, table) == parts
    assert results(reader, table) == expected
    # Re-attached from the records: every part the reader lists in Keeper had one (outdated parts the
    # cleanup has not removed yet are among them), and nothing was fetched again...
    attached = log_lines(reader, r"default\.{} .*Attached [0-9]+ of [0-9]+ parts of this replica from their Keeper records".format(table))
    assert attached, "no re-attach line"
    held, total = map(int, re.search(r"Attached ([0-9]+) of ([0-9]+) parts", attached[-1]).groups())
    assert held == total >= len(parts), attached[-1]
    reader.query("SYSTEM FLUSH LOGS")
    assert int(
        reader.query(
            "SELECT count() FROM system.part_log WHERE table = '{}' AND event_type = 'DownloadPart' "
            "AND event_time >= now() - uptime()".format(table)
        )
    ) == 0
    # ...and the boot listed exactly what a boot with no parts lists: re-attaching N parts costs no LIST.
    assert full_boot == empty_boot, (empty_boot, full_boot)
    # What a boot does list is the mount probe and the namespace-files emulation of the table directory
    # (TEC-845 (C)); never a manifest, a blob, a ref stream or anything per part.
    boot = reader.query("SELECT formatDateTime(now() - uptime(), '%Y.%m.%d %H:%i:%S')").strip()
    lists = [line for line in log_lines(reader, r"Make S3 request to: .*list-type=2") if line[:19] >= boot]
    prefixes = {re.search(r"prefix=([^&,]*)", line).group(1) for line in lists}
    assert lists and all(p.startswith("keeper_pool%2F_probe%2F") or p.endswith("%2F_files%2F") for p in prefixes), prefixes



def test_writer_boot_backfills_missing_records():
    """Parts committed without a record (as by a build before records existed) get one at the next boot,
    with the bytes a commit would have written. A reader that already has those parts in its own ref table
    keeps serving them from there (a record never shadows it), and a part inserted afterwards reaches the
    reader through its record."""
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table = "keeper_parts_backfill"
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    create_table(writer, table, wide=True, reader=False)
    create_table(reader, table, wide=True, reader=True)

    def insert(start):
        writer.query(
            "INSERT INTO {table} SELECT number, number, toString(number), NULL FROM numbers({start}, 1000)".format(
                table=table, start=start
            )
        )

    # The writer commits as a build without records did. The reader fetches by relink (there is no record)
    # and its fetch commit never creates one: only a replica that produced a part may.
    writer.query("SYSTEM ENABLE FAILPOINT cas_part_record_omit")
    try:
        for i in range(3):
            insert(i * 1000)
        reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    finally:
        writer.query("SYSTEM DISABLE FAILPOINT cas_part_record_omit")
    assert record_lengths(writer, table) == {}
    # As a table created by a build without records: not even the parent node exists. The attach thread
    # creates it before backfilling.
    cluster.get_kazoo_client("zoo1").delete("/clickhouse/tables/{}/cas_parts".format(table))
    legacy = active_parts(writer, table)
    assert active_parts(reader, table) == legacy

    writer.restart_clickhouse()
    writer.query("SELECT count() FROM {}".format(table))
    assert sorted(record_lengths(writer, table)) == legacy
    assert log_lines(
        writer, r"default\.{} .*Backfilled {} Keeper part records \(0 already present\)".format(table, len(legacy))
    )

    # The reader's own ref table holds those parts: at its boot the records attach none of them.
    reader.restart_clickhouse()
    assert results(reader, table) == results(writer, table)
    held = log_lines(reader, r"default\.{} .*Attached 0 of [0-9]+ parts of this replica from their Keeper records".format(table))
    assert held, "the reader's ref-table parts must not be re-attached from their records"

    # A part inserted now reaches the reader through its record.
    insert(3000)
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    assert results(reader, table) == results(writer, table)
    assert log_lines(reader, r"default\.{} .*Attached part .* from its Keeper record".format(table))
    for node in (writer, reader):
        node.query("DROP TABLE {} SYNC".format(table))


def test_replace_partition_creates_records():
    """REPLACE PARTITION FROM (the nightly refresh) commits the cloned parts with their records and still
    reads its own log entry back; the reader gets the parts from the records."""
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table, source = "keeper_parts_replace", "keeper_parts_replace_src"
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    writer.query("DROP TABLE IF EXISTS {} SYNC".format(source))
    create_table(writer, table, wide=True, reader=False)
    create_table(reader, table, wide=True, reader=True)
    writer.query(
        "CREATE TABLE {source} AS {table} ENGINE = MergeTree PARTITION BY id % 2 ORDER BY id "
        "SETTINGS storage_policy = '{policy}', min_bytes_for_wide_part = 0".format(source=source, table=table, policy=STORAGE_POLICY)
    )
    writer.query(
        "INSERT INTO {} SELECT number, number % 7919, toString(number % 97), NULL FROM numbers(4000)".format(source)
    )

    writer.query("ALTER TABLE {} REPLACE PARTITION 0 FROM {} SETTINGS alter_sync = 1".format(table, source))
    writer.query("ALTER TABLE {} REPLACE PARTITION 1 FROM {} SETTINGS alter_sync = 1".format(table, source))
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)

    active = active_parts(writer, table)
    assert active and set(active) <= set(record_lengths(writer, table))
    assert active_parts(reader, table) == active
    assert results(reader, table) == results(writer, table)
    assert log_lines(reader, r"default\.{} .*Attached part .* from its Keeper record".format(table))
    for node in (writer, reader):
        node.query("DROP TABLE {} SYNC".format(table))
    writer.query("DROP TABLE {} SYNC".format(source))


def test_reader_removes_outdated_record_parts():
    """A part the reader attached from its record becomes outdated after the writer's merge, and the
    reader's cleanup removes it the fast way: no refusal, and nothing of it is left, on disk or in memory."""
    writer = cluster.instances["writer"]
    reader = cluster.instances["reader"]
    table = "keeper_parts_removal"
    for node in (writer, reader):
        node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    create_table(writer, table, wide=True, reader=False)
    create_table(reader, table, wide=True, reader=True)
    uuid = reader.query("SELECT uuid FROM system.tables WHERE name = '{}'".format(table)).strip()

    for i in range(3):
        writer.query(
            "INSERT INTO {table} SELECT number, number, toString(number), NULL FROM numbers({start}, 1000)".format(
                table=table, start=i * 1000
            )
        )
    reader.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    from_records = active_parts(reader, table)
    attached = log_lines(reader, r"default\.{} .*Attached part .* from its Keeper record".format(table))
    assert from_records and all(any(" {} ".format(p) in line for line in attached) for p in from_records), (
        from_records,
        attached,
    )

    def part_dirs():
        """The part directories the reader's disk serves for the table."""
        return set(
            reader.query(
                "SELECT DISTINCT splitByChar('/', local_path)[4] FROM system.remote_data_paths "
                "WHERE disk_name = '{}' AND local_path LIKE '%/{}/%'".format(CA_DISK, uuid)
            ).split()
        )

    assert set(from_records) <= part_dirs()

    writer.query("OPTIMIZE TABLE {} FINAL".format(table))
    for node in (writer, reader):
        node.query("SYSTEM SYNC REPLICA {}".format(table), timeout=120)
    active = active_parts(writer, table)
    assert active_parts(reader, table) == active and not set(from_records) & set(active)
    assert results(reader, table) == results(writer, table)

    # Removed from memory (no part in any state but the active ones) and from the disk.
    wait_until(
        lambda: reader.query(
            "SELECT name FROM system.parts WHERE table = '{}' ORDER BY name".format(table)
        ).split() == active,
        60,
        "the reader's cleanup to remove its outdated parts",
    )
    assert part_dirs() == set(active), part_dirs()
    assert not log_lines(reader, r"Cannot quickly remove directory .*{}".format(uuid))
    assert not log_lines(reader, r"{}.*NOT_IMPLEMENTED".format(uuid))
    for node in (writer, reader):
        node.query("DROP TABLE {} SYNC".format(table))
