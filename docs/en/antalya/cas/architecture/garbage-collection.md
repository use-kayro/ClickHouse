---
description: 'The CAS garbage collector: blob liveness from Keeper part records, the round, condemnation and exact-token deletion, and observability.'
sidebar_label: 'Garbage collection'
sidebar_position: 8
slug: /antalya/cas/architecture/garbage-collection
title: 'CAS Architecture — Garbage Collection'
doc_type: 'reference'
---

# CAS architecture — garbage collection {#garbage-collection}

`GC` is the only place in `CAS` that deletes a blob body. It runs as a background, paced loop per
disk (`Gc/CasGcScheduler`), and each round is `Cas::KeeperGc::runRound` (`Gc/CasKeeperGc.h`). A
blob of the shared pool is live while some Keeper part record (`<zookeeper_path>/cas_parts/<part>`)
references it. The writer-versus-`GC` race over one blob is covered on the
[blob-protocol page](/antalya/cas/architecture/blob-protocol#writer-gc-race).

## The round {#the-round}

A round runs only on the writer: every table registered on the disk must be written by this server
(a merge leader). Tables register with their content-addressed disks as `ICasKeeperRecordSource` at
startup.

1. Read every source's records, one batched read per table.
2. A record whose part no replica lists is **marked**: re-`set` with its own bytes,
   version-checked, so its mtime says when it was found unlisted. Its blobs that no other record
   references are condemned (see below).
3. A marked record whose part is still unlisted `old_parts_lifetime` after its mark is removed by
   its source in one multi-op that checks that no replica lists the part and that the writer epoch
   (`<replica>/is_active` and `host` versions) holds. A part listed again in between fails the check
   and the cycle restarts.
4. Each condemned blob that no remaining record references is deleted, exact-`ETag`.

A record outlives the removal of its part from the replicas: it is the persistent delete queue.
Nothing is deleted while a listed part has no record (its blobs are unknown); a non-replicated
`MergeTree` table on the disk reports its parts as such, so it blocks deletion instead of losing
its blobs.

## Condemnation and deletion {#condemn-delete}

To condemn a blob, `GC` observes its `ETag` and sets its `.meta` to `Condemned`, so a writer that
adopts the blob from then on re-uploads it (new `ETag`) instead. The delete names the observed
`ETag`: a blob a writer re-uploaded since its condemnation is spared.

A blob is deleted only once every build of this server that existed when it was condemned has
retired (`Pool::minActive` passed the build sequence taken when the marker was durable): a build that
adopted it before the marker either committed — its part has a record, read in step 1 — or was
abandoned. A build that started after the marker saw `Condemned` and re-uploaded.

Condemnations are kept in memory: after a restart a blob is condemned again before it is deleted. A
crash between a record removal and the blob deletes leaks those blobs (`.meta = Condemned`, no
record); it never loses one. Manifest bodies, ref logs, ref snapshots and catalog rows are not
reclaimed by `GC`.

## Observability {#observability}

`system.cas_gc_log` emits `Start`, `Finish` and per-`Phase` rows, correlated by `round_id`.
`system.cas_log` carries the audit trail. `CASGCRetiredCondemned` counts condemned blobs.

`SYSTEM CAS FSCK` / `clickhouse-disks cas-fsck` distinguish `dangling` — referenced but missing,
data loss — from present, unreferenced blobs, which it labels by their `.meta`: `condemned` or
`unreferenced`.

## Operational surface {#operational-surface}

| Command | Effect |
|---|---|
| `SYSTEM CAS GC RUN '<disk>'` | One synchronous round on the contacted node; only the writer makes progress |
| `SYSTEM CAS GC STOP` / `SYSTEM CAS GC START` | Stop or resume future rounds on the same scheduler |

`SYSTEM CAS DROP POOL MEMBER '<server_root_id>' FROM DISK '<disk>'` — permanent removal of a dead
replica, distinct from ordinary `GC` — is covered on the
[mounts-and-leases page](/antalya/cas/architecture/mounts-and-leases#mount-lifecycle).
