#!/usr/bin/env python3
"""Rehearsal of the TEC-849/TEC-850 production upgrade of the CAS pool, on the real release images.

Production: a writer and two readers (`reader`, `reader-mdb`) share one CAS pool (metadata_type=cas) and one
Keeper. The upgrade restarts the writer on step 1 (Keeper part records + boot backfill, old GC kept), then the
writer on step 2 (GC derived from the records), then each reader on step 2, one at a time. This script plays that
sequence against compose.yml, on a pool whose state the OLD GC produced, and checks each step. Run one phase at a
time, in order; each appends its observations to results.jsonl:

    ./rehearse.py up          # pool, Keeper, three nodes on RC3
    ./rehearse.py a           # RC3 everywhere: data, merges, DROP PARTITION, old GC rounds
    ./rehearse.py b           # writer -> step 1
    ./rehearse.py c           # writer -> step 2 (reader query loop throughout)
    ./rehearse.py d           # readers -> step 2, one at a time
    ./rehearse.py e           # rollback drill: writer -> step 1, reader -> RC3

Images come from the environment (RC3_IMAGE, STEP1_IMAGE, STEP2_IMAGE). The lab-only timing knobs are in
configs/common.xml (cas_gc_interval_sec=5, old_parts_lifetime=30); production keeps 600 s and the 480 s default.
"""

import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.config import Config

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "state.json")
RESULTS = os.path.join(HERE, "results.jsonl")
PORTS = {"writer": 39123, "reader": 39124, "reader-mdb": 39125}
NODES = list(PORTS)
READERS = ["reader", "reader-mdb"]
IMAGE_VARS = {"writer": "WRITER_IMAGE", "reader": "READER_IMAGE", "reader-mdb": "READER_MDB_IMAGE"}
TABLE = "rehearsal"
ZK_PATH = "/clickhouse/tables/1/" + TABLE
DISK = "shared_s3"
BUCKET, POOL = "pool", "shared_pool/"
GC_INTERVAL = 5
OLD_PARTS_LIFETIME = 30


# ---------------------------------------------------------------- plumbing

def log(*args):
    print(time.strftime("%H:%M:%S"), *args, flush=True)


def record(phase, check, ok, observed):
    row = {"t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "phase": phase, "check": check,
           "ok": ok, "observed": observed}
    with open(RESULTS, "a") as f:
        f.write(json.dumps(row) + "\n")
    log("[{}] {} {}: {}".format(phase, "PASS" if ok else ("INFO" if ok is None else "FAIL"), check,
                                json.dumps(observed)[:600]))


def q(node, sql, timeout=120):
    req = urllib.request.Request("http://127.0.0.1:{}/".format(PORTS[node]), data=sql.encode(), method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def q1(node, sql, timeout=120):
    return q(node, sql, timeout).strip()


def load_state():
    if os.path.exists(STATE):
        return json.load(open(STATE))
    return {}


def save_state(state):
    json.dump(state, open(STATE, "w"), indent=1)


def compose(*args, check=True):
    env = dict(os.environ)
    st = load_state()
    for node, var in IMAGE_VARS.items():
        env[var] = st.get("images", {}).get(node, os.environ.get("RC3_IMAGE", ""))
    return subprocess.run(["docker", "compose", "-p", "rehearsal", "-f", os.path.join(HERE, "compose.yml")] + list(args),
                          env=env, check=check, capture_output=True, text=True)


def image_of(node):
    return load_state().get("images", {}).get(node)


def wait_until(pred, timeout, what, step=1.0):
    deadline = time.time() + timeout
    while True:
        try:
            v = pred()
            if v:
                return v
        except Exception:
            pass
        if time.time() > deadline:
            raise AssertionError("timed out after {} s waiting for {}".format(timeout, what))
        time.sleep(step)


def node_up(node):
    try:
        return q1(node, "SELECT 1", timeout=5) == "1"
    except Exception:
        return False


def server_now(node):
    return q1(node, "SELECT toString(now64(3))")


def log_grep(node, pattern, since_line=0):
    path = os.path.join(HERE, "logs", node, "clickhouse-server.log")
    out = subprocess.run(["grep", "-a", "-n", "-E", pattern, path], capture_output=True, text=True).stdout
    return [l.split(":", 1)[1] for l in out.splitlines() if int(l.split(":", 1)[0]) > since_line]


def log_len(node):
    path = os.path.join(HERE, "logs", node, "clickhouse-server.log")
    if not os.path.exists(path):
        return 0
    return int(subprocess.run(["wc", "-l", path], capture_output=True, text=True).stdout.split()[0])


def restart(node, image):
    """Replaces the node's container with one on `image`, as configure-image.py does on a production host.
    Returns (seconds down, log line count before the restart)."""
    st = load_state()
    st.setdefault("images", {})[node] = image
    save_state(st)
    mark = log_len(node)
    t0 = time.time()
    compose("stop", "-t", "60", node)
    t_stop = time.time()
    compose("up", "-d", "--no-deps", node)
    wait_until(lambda: node_up(node), 300, "{} to answer".format(node))
    wait_until(lambda: q1(node, "SELECT count() FROM system.replicas WHERE table = '{}' AND NOT is_readonly".format(TABLE)) == "1",
               300, "{} replica to be writable/attached".format(node))
    log("{} restarted on {} (stop {:.1f}s, down {:.1f}s)".format(node, image.split("@")[-1][:19], t_stop - t0, time.time() - t0))
    return time.time() - t0, mark


# ---------------------------------------------------------------- pool and node observations

_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:39121", aws_access_key_id="clickhouse",
                           aws_secret_access_key="clickhouse", region_name="us-east-1",
                           config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))
    return _s3


def list_keys(prefix=POOL):
    keys = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def pool_inventory(read_meta=True):
    """Counts of the pool's key classes, the old GC's artefacts included."""
    keys = list_keys()
    rel = [k[len(POOL):] for k in keys]
    inv = {
        "blobs": sum(1 for k in rel if k.startswith("blobs/") and not k.endswith(".meta")),
        "blob_meta": sum(1 for k in rel if k.startswith("blobs/") and k.endswith(".meta")),
        "gc_state": int("gc/state" in rel),
        "fold_seals": sum(1 for k in rel if k.startswith("gc/gen/") and k.endswith("/fold_seal")),
        "gc_gen_other": sum(1 for k in rel if k.startswith("gc/gen/") and not k.endswith("/fold_seal")),
        "ref_logs": sum(1 for k in rel if k.startswith("cas/ns/stream/") and "/_log/" in k),
        "ref_snaps": sum(1 for k in rel if k.startswith("cas/ns/stream/") and "/_snap/" in k),
        "ns_state": sum(1 for k in rel if k.startswith("cas/ns/state/")),
        "manifests": sum(1 for k in rel if k.startswith("cas/manifests/")),
        "server_roots": sorted({k.split("/")[2] for k in rel if k.startswith("gc/server-roots/")}),
        "total": len(rel),
    }
    if read_meta:
        condemned = 0
        for k in keys:
            if k.endswith(".meta") and "/blobs/" in "/" + k[len(POOL):]:
                body = s3().get_object(Bucket=BUCKET, Key=k)["Body"].read()
                condemned += b'"condemned"' in body
        inv["blob_meta_condemned"] = condemned
    return inv


def condemned_metas():
    n = 0
    for k in list_keys(POOL + "blobs/"):
        if k.endswith(".meta"):
            try:
                n += b'"condemned"' in s3().get_object(Bucket=BUCKET, Key=k)["Body"].read()
            except s3().exceptions.NoSuchKey:
                pass
    return n


def garbage_then_condemned(p, phase, what):
    """Merges partition `p` and drops it, then returns as soon as the old GC has condemned blobs: the next restart
    then lands while `.meta = Condemned` markers are outstanding, as a production restart between two 600 s
    rounds does."""
    for _ in range(3):
        insert(p)
    q("writer", "OPTIMIZE TABLE {} PARTITION {} FINAL".format(TABLE, p), timeout=600)
    q("writer", "ALTER TABLE {} DROP PARTITION {}".format(TABLE, p))
    sync()
    n = wait_until(condemned_metas, OLD_PARTS_LIFETIME + 40 * GC_INTERVAL, "old-GC condemned markers", step=1)
    record(phase, "condemned markers outstanding at the restart ({})".format(what), n > 0, {"condemned_meta": n})


def blob_keys():
    return {k for k in list_keys(POOL + "blobs/") if not k.endswith(".meta")}


def events(node, like=("S3%", "DiskS3%", "CAS%")):
    where = " OR ".join("event LIKE '{}'".format(p) for p in like)
    out = q(node, "SELECT event, value FROM system.events WHERE {} FORMAT TSV".format(where))
    return {e: int(v) for e, v in (l.split("\t") for l in out.splitlines())}


def delta(after, before):
    return {k: v - before.get(k, 0) for k, v in sorted(after.items()) if v - before.get(k, 0)}


def S3_PUT_LIST(d):
    return {k: v for k, v in d.items() if k in ("S3PutObject", "S3ListObjects", "DiskS3PutObject", "DiskS3ListObjects",
                                               "S3CreateMultipartUpload", "S3CopyObject", "S3DeleteObjects")}


def gc_rounds(node, since):
    """Finish rows per outcome since `since`. Only columns both GC generations have (step 2 changed the table)."""
    q(node, "SYSTEM FLUSH LOGS")
    cols = set(q(node, "SELECT name FROM system.columns WHERE database = 'system' AND table = 'cas_gc_log'").split())
    extra = ", sum(writer)" if "writer" in cols else ", -1"
    extra += ", sum(objects_condemned)" if "objects_condemned" in cols else ", sum(entries_condemned)"
    out = q(node, """
        SELECT toString(outcome), count(), sum(objects_deleted), sum(objects_absent), max(event_time){}
        FROM system.cas_gc_log
        WHERE disk_name = '{}' AND event_type = 'Finish' AND event_time_microseconds > '{}'
        GROUP BY outcome ORDER BY outcome FORMAT TSV""".format(extra, DISK, since))
    res = {}
    for line in out.splitlines():
        o, n, deleted, absent, last, writer, condemned = line.split("\t")
        res[o] = {"rounds": int(n), "deleted": int(deleted), "absent": int(absent), "condemned": int(condemned), "last": last}
        if int(writer) >= 0:
            res[o]["as_writer"] = int(writer)
    return res


def gc_leaders():
    """Each up node's own view of the old GC lease (system.cas_mounts.is_leader of its own mount)."""
    res = {}
    for n in NODES:
        try:
            res[n] = q1(n, "SELECT toString(is_leader) FROM system.cas_mounts WHERE disk = '{}' AND server_root_id = '{}'".format(DISK, n), timeout=10)
        except Exception as e:
            res[n] = "n/a: " + str(e)[:80]
    return res


def deleted_by(node, since):
    return sum(v["deleted"] for v in gc_rounds(node, since).values())


def keeper_gc_rounds(node, since):
    """Step-2 rounds one by one: the Finish row and the phase metrics of the same round_id."""
    q(node, "SYSTEM FLUSH LOGS")
    out = q(node, """
        SELECT round_id, min(event_time_microseconds) AS t,
               maxIf(writer, event_type = 'Finish'), maxIf(round, event_type = 'Finish'),
               maxIf(objects_condemned, event_type = 'Finish'), maxIf(objects_deleted, event_type = 'Finish'),
               maxIf(objects_absent, event_type = 'Finish'), anyIf(toString(outcome), event_type = 'Finish'),
               toJSONString(maxMap(phase_metrics)), groupUniqArrayIf(phase, event_type = 'Phase')
        FROM system.cas_gc_log
        WHERE disk_name = '{}' AND event_time_microseconds > '{}'
        GROUP BY round_id HAVING countIf(event_type = 'Finish') = 1 ORDER BY t FORMAT TSV""".format(DISK, since))
    rows = []
    for line in out.splitlines():
        rid, t, writer, rnd, cond, deleted, absent, outcome, metrics, phases = line.split("\t")
        rows.append({"t": t[11:19], "writer": int(writer), "round": int(rnd), "outcome": outcome, "condemned": int(cond),
                     "deleted": int(deleted), "absent": int(absent), "metrics": json.loads(metrics), "phases": phases})
    return rows


def active_parts(node):
    return q(node, "SELECT name FROM system.parts WHERE table = '{}' AND active ORDER BY name".format(TABLE)).split()


def records(node="writer"):
    try:
        return q(node, "SELECT name FROM system.zookeeper WHERE path = '{}/cas_parts' ORDER BY name".format(ZK_PATH)).split()
    except urllib.error.HTTPError:
        return None  # the node does not exist yet


def sums(node):
    out = q(node, "SELECT p, count(), sum(x) FROM {} GROUP BY p ORDER BY p FORMAT TSV".format(TABLE))
    return {int(p): (int(c), int(s)) for p, c, s in (l.split("\t") for l in out.splitlines())}


def sync(nodes=READERS):
    for n in nodes:
        q(n, "SYSTEM SYNC REPLICA {}".format(TABLE), timeout=300)


_insert_no = [None]


def insert(p, rows=2000):
    """An insert whose blobs no other insert shares (distinct x and row count)."""
    st = load_state()
    k = st.get("inserts", 0)
    st["inserts"] = k + 1
    save_state(st)
    q("writer", "INSERT INTO {} SELECT {}, number + {} FROM numbers({})".format(TABLE, p, k * 10_000_000, rows + k))


def consistent(phase, what):
    sync()
    ref = sums("writer")
    others = {n: sums(n) for n in READERS if node_up(n)}
    ok = all(v == ref for v in others.values())
    record(phase, "data identical on every node ({})".format(what), ok,
           {"partitions": len(ref), "rows": sum(c for c, _ in ref.values()),
            "mismatch": [n for n, v in others.items() if v != ref]})
    return ref


def reader_window(node, seconds, label):
    """S3 requests a reader makes over `seconds` while it serves queries and syncs (no restart)."""
    before = events(node)
    t_end = time.time() + seconds
    n = 0
    while time.time() < t_end:
        q(node, "SELECT p, count(), sum(x) FROM {} GROUP BY p FORMAT Null".format(TABLE))
        n += 1
        time.sleep(1)
    d = delta(events(node), before)
    return {"seconds": seconds, "queries": n, "label": label, "s3": {k: v for k, v in d.items() if k.startswith(("S3", "DiskS3"))},
            "cas": {k: v for k, v in d.items() if k.startswith("CAS")}}


# ---------------------------------------------------------------- reader query loop

class QueryLoop:
    """Queries a node continuously from a thread; every failure is kept with the node's container state."""

    def __init__(self, node):
        self.node, self.ok, self.failures, self.stop_ev = node, 0, [], threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stop_ev.is_set():
            try:
                q(self.node, "SELECT p, count(), sum(x) FROM {} GROUP BY p ORDER BY p FORMAT Null".format(TABLE), timeout=60)
                self.ok += 1
            except Exception as e:
                self.failures.append((time.time(), type(e).__name__, str(e)[:300]))
            time.sleep(0.2)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop_ev.set()
        self.thread.join()

    def server_errors(self):
        """Failures the server answered (HTTP 5xx with an exception), not connection refusals while it was down."""
        return [f for f in self.failures if f[1] == "HTTPError"]


# ---------------------------------------------------------------- phases

def phase_up():
    rc3 = os.environ["RC3_IMAGE"]
    save_state({"images": {n: rc3 for n in NODES}, "inserts": 0})
    for n in NODES:
        os.makedirs(os.path.join(HERE, "logs", n), exist_ok=True)
        os.chmod(os.path.join(HERE, "logs", n), 0o777)
    compose("up", "-d", "rustfs", "keeper")
    wait_until(lambda: s3().list_buckets() is not None, 120, "RustFS")
    if BUCKET not in [b["Name"] for b in s3().list_buckets()["Buckets"]]:
        s3().create_bucket(Bucket=BUCKET)
    # The writer first: it mounts the pool first and so takes the old GC's lease, as in production.
    for n in NODES:
        compose("up", "-d", "--no-deps", n)
        wait_until(lambda: node_up(n), 300, n)
    for n in NODES:
        log(n, q1(n, "SELECT version()"), image_of(n).split("@")[-1][:19])
    flags = q1("writer", "SELECT arrayStringConcat(enabled_feature_flags, ',') FROM system.zookeeper_connection")
    record("up", "Keeper (prod image 26.2.19.43, default flags) advertises CREATE_IF_NOT_EXISTS",
           "CREATE_IF_NOT_EXISTS" in flags, flags)
    for n in NODES:
        q(n, """CREATE TABLE IF NOT EXISTS {t} (p UInt8, x UInt64)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{t}', '{{replica}}')
                PARTITION BY p ORDER BY x SETTINGS storage_policy = 'shared_s3'""".format(t=TABLE))


def phase_a():
    ph = "a"
    since = {n: server_now(n) for n in NODES}
    # Background merges stay on (as in production): the partitions keep several parts each while the merges
    # leave outdated parts behind for the old GC.
    for p in range(8):
        for _ in range(8):
            insert(p)
    sync()
    q("writer", "SYSTEM FLUSH LOGS")
    created = int(q1("writer", "SELECT count() FROM system.part_log WHERE table = '{}' AND event_type IN ('NewPart', 'MergeParts')".format(TABLE)))
    parts0 = active_parts("writer")
    record(ph, "parts loaded", created >= 64, {"parts_created": created, "active_parts": len(parts0), "partitions": 8})
    for p in (0, 1, 2):
        q("writer", "OPTIMIZE TABLE {} PARTITION {} FINAL".format(TABLE, p), timeout=600)
    q("writer", "ALTER TABLE {} DROP PARTITION 7".format(TABLE))
    sync()
    removed = sorted(set(parts0) - set(active_parts("writer")))
    st = load_state()
    st["removed_before_records"] = removed
    save_state(st)
    # Let the old GC reclaim: the outdated parts go after old_parts_lifetime, then condemn -> graduate -> delete
    # takes at least three folding rounds.
    wait_until(lambda: sum(r.get("deleted", 0) for n in NODES for r in gc_rounds(n, since[n]).values()) > 0,
               OLD_PARTS_LIFETIME + 60 * GC_INTERVAL, "the old GC to delete blobs", step=5)
    # More garbage right before the end, so the bucket also holds blobs condemned but not yet deleted.
    garbage_then_condemned(6, ph, "end of a")
    q("writer", "ALTER TABLE {} DROP PARTITION 5".format(TABLE))
    sync()
    st = load_state()
    st["removed_before_records"] = sorted(set(st["removed_before_records"]) | (set(parts0) - set(active_parts("writer"))))
    save_state(st)
    inv = pool_inventory()
    rounds = {n: gc_rounds(n, since[n]) for n in NODES}
    record(ph, "old GC ran several rounds and deleted", sum(r.get("Success", {}).get("rounds", 0) for r in rounds.values()) >= 5
           and sum(v.get("deleted", 0) for r in rounds.values() for v in r.values()) > 0, rounds)
    record(ph, "bucket holds old-GC artefacts (gc/state, fold seals, Condemned .meta, ref logs)",
           inv["gc_state"] == 1 and inv["fold_seals"] > 0 and inv["blob_meta_condemned"] > 0 and inv["ref_logs"] > 0, inv)
    record(ph, "parts removed (merged away / dropped) before any Keeper record existed", len(st["removed_before_records"]) > 0,
           {"removed_parts": len(st["removed_before_records"]), "records_path": records()})
    consistent(ph, "RC3 everywhere")
    st = load_state()
    st["reader_window_rc3"] = reader_window("reader", 60, "writer RC3, reader RC3")
    save_state(st)
    record(ph, "reader S3 requests over 60 s (baseline, all RC3)", None, st["reader_window_rc3"])


def phase_b():
    ph = "b"
    step1 = os.environ["STEP1_IMAGE"]
    before_active = active_parts("writer")
    record(ph, "records before the step-1 writer boots", records() in (None, []), {"records": records()})
    garbage_then_condemned(4, ph, "writer -> step 1")
    with QueryLoop("reader") as loop:
        down, mark = restart("writer", step1)
        backfill = wait_until(lambda: log_grep("writer", r"Backfilled [0-9]+ Keeper part records", mark), 120,
                              "the backfill line", step=2)
    n = int(backfill[0].split("Backfilled ")[1].split()[0])
    after_active = active_parts("writer")
    recs = set(records())
    record(ph, "backfill count == active parts", n == len(after_active) and set(after_active) <= recs,
           {"backfilled": n, "active_parts": len(after_active), "active_before_restart": len(before_active),
            "active_without_record": sorted(set(after_active) - recs), "line": backfill[0][-120:]})
    record(ph, "reader queries during the writer restart", not loop.server_errors(),
           {"ok": loop.ok, "failures": loop.failures[:5], "writer_down_s": round(down, 1)})
    removed = set(load_state()["removed_before_records"])
    record(ph, "no record for parts removed before step 1", not (removed & recs), {"removed_with_record": sorted(removed & recs)})

    since = {n: server_now(n) for n in NODES}
    before = set(active_parts("writer"))
    for p in (10, 11):
        for _ in range(3):
            insert(p)
    new = set(active_parts("writer")) - before
    recs = set(records())
    record(ph, "new inserts create records", bool(new) and new <= recs, {"new_parts": sorted(new), "without_record": sorted(new - recs)})
    consistent(ph, "step-1 writer, RC3 readers")
    attach = {n: {"relink": len(log_grep(n, r"Relink of part .* finished \(no bytes transferred\)")),
                  "download": len(log_grep(n, r"Download of part .* finished")),
                  "from_record": len(log_grep(n, r"from its Keeper record"))} for n in READERS}
    record(ph, "RC3 readers attach new step-1 parts (by relink, as before)", all(a["relink"] > 0 and a["from_record"] == 0 for a in attach.values()), attach)

    w = reader_window("reader", 60, "writer step-1, reader RC3")
    base = load_state()["reader_window_rc3"]
    record(ph, "reader S3 request pattern unchanged (60 s windows)", set(w["s3"]) <= set(base["s3"]) | {"S3GetObject", "DiskS3GetObject"},
           {"rc3_writer": base["s3"], "step1_writer": w["s3"], "cas_rc3_writer": base["cas"], "cas_step1_writer": w["cas"]})

    # The old GC still reclaims a fresh DROP PARTITION.
    b0 = blob_keys()
    for _ in range(3):
        insert(12)
    p12 = blob_keys() - b0
    sync()
    q("writer", "ALTER TABLE {} DROP PARTITION 12".format(TABLE))
    dropped = time.time()
    wait_until(lambda: not (p12 & blob_keys()), OLD_PARTS_LIFETIME + 60 * GC_INTERVAL, "partition 12 blobs reclaimed", step=3)
    rounds = {n: gc_rounds(n, since[n]) for n in NODES}
    leader = [n for n, r in rounds.items() if r.get("Success", {}).get("deleted", 0) > 0]
    record(ph, "old GC reclaims a fresh DROP PARTITION", True,
           {"blobs": len(p12), "reclaimed_after_s": round(time.time() - dropped), "deleting_nodes": leader, "rounds": rounds})
    record(ph, "old GC lease holder after b", None, gc_leaders())
    record(ph, "pool inventory after b", None, pool_inventory())


def create_side_table(node, table):
    q(node, """CREATE TABLE IF NOT EXISTS {t} (p UInt8, x UInt64)
               ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{t}', '{{replica}}')
               PARTITION BY p ORDER BY x SETTINGS storage_policy = 'shared_s3'""".format(t=table))


def old_gc_activity(nodes, since):
    """Rounds of the S3-listing GC on members still running it: which of them deleted."""
    return {n: gc_rounds(n, since[n]) for n in nodes}


def phase_c():
    ph = "c"
    step2 = os.environ["STEP2_IMAGE"]
    garbage_then_condemned(3, ph, "writer -> step 2")
    inv0 = pool_inventory()
    record(ph, "pool inventory before the step-2 writer", None, inv0)
    since = {n: server_now(n) for n in NODES}
    loops = [QueryLoop(n) for n in READERS]
    for l in loops:
        l.__enter__()
    try:
        down, mark = restart("writer", step2)
        t_boot = server_now("writer")
        # First rounds as the step-2 writer.
        wait_until(lambda: len([r for r in keeper_gc_rounds("writer", t_boot) if r["writer"]]) >= 6,
                   60 * GC_INTERVAL, "six writer rounds", step=3)
        first = keeper_gc_rounds("writer", t_boot)[:10]
        record(ph, "first step-2 writer rounds", None, first)
        record(ph, "old GC lease right after the step-2 writer boots", None, {"leaders": gc_leaders(), "old_gc_rounds": old_gc_activity(READERS, since)})
        unrec = [r for r in first if r["metrics"].get("unrecorded_listed", 0) > 0]
        record(ph, "first rounds: no round deletes while a listed part is unrecorded",
               all(r["deleted"] == 0 for r in unrec), {"rounds_with_unrecorded": len(unrec),
                                                       "unrecorded_listed": [r["metrics"].get("unrecorded_listed") for r in first]})

        # The guard on a real unrecorded part: MOVE PARTITION TO TABLE commits the part without a record (step-1
        # comment at StorageReplicatedMergeTree.cpp:10285); a DROP PARTITION in the same window makes records due.
        create_side_table("writer", TABLE + "_src")
        q("writer", "INSERT INTO {}_src SELECT 20, number + 900000000 FROM numbers(3000)".format(TABLE))
        before = set(active_parts("writer"))
        q("writer", "ALTER TABLE {t}_src MOVE PARTITION 20 TO TABLE {t}".format(t=TABLE))
        moved = sorted(set(active_parts("writer")) - before)
        recs = set(records())
        record(ph, "MOVE PARTITION TO TABLE leaves the moved part without a record", bool(moved) and not (set(moved) & recs),
               {"moved": moved, "recorded": sorted(set(moved) & recs)})
        b0 = blob_keys()
        for _ in range(3):
            insert(13)
        p13 = blob_keys() - b0
        sync()
        t_guard = server_now("writer")
        q("writer", "ALTER TABLE {} DROP PARTITION 13".format(TABLE))
        guard_lines = wait_until(lambda: log_grep("writer", r"listed parts have no Keeper record, so their blobs are unknown; removing nothing", mark),
                                 OLD_PARTS_LIFETIME + 20 * GC_INTERVAL, "the unrecorded-part guard", step=3)
        held = keeper_gc_rounds("writer", t_guard)
        record(ph, "guard: rounds with an unrecorded listed part and due records delete nothing",
               all(r["deleted"] == 0 for r in held if r["metrics"].get("unrecorded_listed", 0)),
               {"line": guard_lines[-1][-170:], "rounds": [(r["t"], r["metrics"].get("unrecorded_listed"), r["metrics"].get("due"), r["deleted"]) for r in held]})
        # A merge records the partition again; once the moved part is unlisted the rounds are normal.
        q("writer", "OPTIMIZE TABLE {} PARTITION 20 FINAL".format(TABLE), timeout=600)
        t_merge = time.time()
        wait_until(lambda: not (p13 & blob_keys()), OLD_PARTS_LIFETIME * 3 + 30 * GC_INTERVAL, "partition 13 reclaimed after the guard", step=3)
        after = keeper_gc_rounds("writer", t_guard)
        record(ph, "after the merge the guarded DROP PARTITION is reclaimed", True,
               {"blobs": len(p13), "reclaimed_s_after_merge": round(time.time() - t_merge),
                "rounds": [(r["t"], r["metrics"].get("unrecorded_listed"), r["metrics"].get("due"), r["metrics"].get("records_removed"), r["deleted"], r["absent"]) for r in after]})

        # A plain DROP PARTITION: reclaimed after the writer's old_parts_lifetime.
        b0 = blob_keys()
        for _ in range(3):
            insert(14)
        p14 = blob_keys() - b0
        sync()
        t_drop_srv = server_now("writer")
        q("writer", "ALTER TABLE {} DROP PARTITION 14".format(TABLE))
        t_drop = time.time()
        wait_until(lambda: not (p14 & blob_keys()), OLD_PARTS_LIFETIME + 30 * GC_INTERVAL, "partition 14 reclaimed", step=1)
        took = time.time() - t_drop
        rounds = keeper_gc_rounds("writer", t_drop_srv)
        old = old_gc_activity(READERS, since)
        record(ph, "DROP PARTITION reclaimed after old_parts_lifetime (lab: {} s)".format(OLD_PARTS_LIFETIME),
               took >= OLD_PARTS_LIFETIME and sum(r["deleted"] for r in rounds) >= len(p14),
               {"blobs": len(p14), "reclaimed_after_s": round(took, 1),
                "rounds": [(r["t"], r["metrics"].get("marked"), r["metrics"].get("due"), r["metrics"].get("records_removed"), r["deleted"], r["absent"]) for r in rounds if r["writer"]],
                "old_gc_on_RC3_readers": old})
        record(ph, "who runs deleting GC rounds while the readers are on RC3 (cas_gc_enabled=1 as production)", None,
               {"leaders": gc_leaders(), "old_gc_rounds_on_readers": old_gc_activity(READERS, since),
                "step2_writer_rounds": {"rounds": len(keeper_gc_rounds("writer", t_boot)),
                                        "deleted": sum(r["deleted"] for r in keeper_gc_rounds("writer", t_boot))}})
        consistent(ph, "step-2 writer, RC3 readers")
    finally:
        for l in loops:
            l.__exit__(None, None, None)
    for l in loops:
        record(ph, "no {} query failed during c".format(l.node), not l.failures, {"ok": l.ok, "failures": l.failures[:5]})
    inv1 = pool_inventory()
    record(ph, "old GC artefacts left alone by step 2 (leak, never loss)", None, {"before": inv0, "after": inv1})
    st = load_state()
    st["since_c"] = since
    save_state(st)


def phase_d():
    ph = "d"
    step2 = os.environ["STEP2_IMAGE"]
    for node in READERS:
        others = [n for n in READERS if n != node]
        since = server_now("writer")
        with QueryLoop(others[0]) as other_loop:
            down, mark = restart(node, step2)
            t_boot = server_now(node)
            attach = log_grep(node, r"Attached [0-9]+ of [0-9]+ parts of this replica from their Keeper records", mark)
            record(ph, "{} boots on step 2".format(node), bool(attach), {"down_s": round(down, 1), "line": attach[-1][-120:] if attach else None})
            # New parts: the reader attaches them from their records.
            mark2 = log_len(node)
            q(node, "SYSTEM FLUSH LOGS")
            t_ins = server_now(node)
            for p in (15, 16):
                for _ in range(3):
                    insert(p)
            q("writer", "OPTIMIZE TABLE {} PARTITION 15 FINAL".format(TABLE), timeout=600)
            sync([node])
            q(node, "SYSTEM FLUSH LOGS")
            fetched = q(node, """
                SELECT count(), sum(ProfileEvents['S3PutObject']), sum(ProfileEvents['S3ListObjects']),
                       sum(ProfileEvents['DiskS3PutObject']), sum(ProfileEvents['DiskS3ListObjects']),
                       sum(ProfileEvents['S3CreateMultipartUpload']), sum(ProfileEvents['S3CopyObject']),
                       sum(ProfileEvents['S3GetObject'])
                FROM system.part_log WHERE table = '{}' AND event_type = 'DownloadPart' AND event_time_microseconds > '{}'
                FORMAT TSV""".format(TABLE, t_ins)).split("\t")
            from_record = log_grep(node, r"from its Keeper record at .*nothing fetched from the replica", mark2)
            n_fetch = int(fetched[0])
            record(ph, "{} attaches new parts from records with 0 PUT / 0 LIST".format(node),
                   n_fetch > 0 and len(from_record) == n_fetch and all(int(v) == 0 for v in fetched[1:7]),
                   {"fetches": n_fetch, "from_record": len(from_record),
                    "S3Put": fetched[1], "S3List": fetched[2], "DiskS3Put": fetched[3], "DiskS3List": fetched[4],
                    "MPU": fetched[5], "Copy": fetched[6], "S3Get": fetched[7]})
            w = reader_window(node, 60, "{} on step 2".format(node))
            record(ph, "{} S3 requests over 60 s idle+queries (mount lease renewals expected)".format(node), None, w)
            rounds = keeper_gc_rounds(node, t_boot)
            record(ph, "{} GC rounds do nothing (writer=0, nothing condemned or deleted)".format(node),
                   bool(rounds) and all(r["writer"] == 0 and r["condemned"] == 0 and r["deleted"] == 0 for r in rounds),
                   {"rounds": len(rounds), "sample": rounds[:2]})
            consistent(ph, "{} on step 2".format(node))
        record(ph, "no {} query failed while {} restarted".format(others[0], node), not other_loop.failures,
               {"ok": other_loop.ok, "failures": other_loop.failures[:5]})
    record(ph, "pool inventory after d", None, pool_inventory())


def phase_e():
    ph = "e"
    step1, rc3 = os.environ["STEP1_IMAGE"], os.environ["RC3_IMAGE"]
    with QueryLoop("reader-mdb") as loop:
        down, mark = restart("writer", step1)
        errors = log_grep("writer", r"<Error>|<Fatal>", mark)
        insert(17)
        sync()
        consistent(ph, "writer back on step 1")
        record(ph, "writer rolls back to step 1: boots, accepts inserts, serves", True,
               {"down_s": round(down, 1), "error_lines": len(errors), "first_errors": [e[-200:] for e in errors[:3]]})
        down, mark = restart("reader", rc3)
        errors = log_grep("reader", r"<Error>|<Fatal>", mark)
        insert(18)
        sync()
        ref = consistent(ph, "reader back on RC3")
        detached = q1("reader", "SELECT count() FROM system.detached_parts WHERE table = '{}'".format(TABLE))
        record(ph, "reader rolls back to RC3: boots, serves", True,
               {"down_s": round(down, 1), "error_lines": len(errors), "first_errors": [e[-200:] for e in errors[:3]],
                "active_parts": len(active_parts("reader")), "writer_active_parts": len(active_parts("writer")),
                "detached_parts": int(detached)})
    record(ph, "no reader-mdb query failed during the rollbacks", not loop.failures, {"ok": loop.ok, "failures": loop.failures[:5]})


def phase_lease():
    """How long a reader on the old GC takes to take over the lease once the writer stops heartbeating."""
    ph = "lease"
    since = {n: server_now(n) for n in READERS}
    leaders0 = gc_leaders()
    compose("stop", "-t", "60", "writer")
    t0 = time.time()
    taken = None
    while time.time() - t0 < 60 * GC_INTERVAL:
        lead = {n: gc_leaders()[n] for n in READERS}
        if "1" in lead.values():
            taken = time.time() - t0
            break
        time.sleep(0.5)
    rounds = {n: gc_rounds(n, since[n]) for n in READERS}
    record(ph, "old-GC lease takeover by an RC3 reader after the writer stops", taken is not None,
           {"before": leaders0, "takeover_s": round(taken, 1) if taken else None, "gc_interval_s": GC_INTERVAL,
            "takeover_in_intervals": round(taken / GC_INTERVAL, 1) if taken else None, "reader_rounds": rounds,
            "after": {n: gc_leaders()[n] for n in READERS}})
    compose("up", "-d", "--no-deps", "writer")
    wait_until(lambda: node_up("writer"), 300, "writer back")
    time.sleep(6 * GC_INTERVAL)
    record(ph, "lease holder once the writer is back (RC3)", None, gc_leaders())


def main():
    {"up": phase_up, "a": phase_a, "lease": phase_lease, "b": phase_b, "c": phase_c, "d": phase_d, "e": phase_e}[sys.argv[1]]()


if __name__ == "__main__":
    main()
