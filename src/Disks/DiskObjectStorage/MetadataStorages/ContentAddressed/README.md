# Content-Addressed (CAS) metadata storage

This directory implements the `cas` metadata storage for `DiskObjectStorage`: a content-addressed pool of
blobs over S3-family object storage, for `ReplicatedMergeTree` tables. Every unique payload is stored
**once** in a shared pool under a key derived from its **content hash**; a part is the list of its files
(blob references plus small inline files). Replicas that share one pool share the bytes.

## What lives where (TEC-851 step 3c)

- **The object store holds blob bodies and the pool's identity object `_pool_meta`, nothing else.** Only
  the pool's writer writes them: the server whose replicas can become merge leader
  (`merge_tree.replicated_can_become_leader`; a CAS table may not override it). `_pool_meta` is minted once,
  by the writer, on a brand-new pool; every other open only reads it.
- **A part's persistent copy is its Keeper record** `<zk>/cas_parts/<part>` (`Storages/MergeTree/CasKeeperPartRecord.h`):
  its inline files and its pool, created in the commit multi-op of the replica that produced the part. A part
  that cannot have a record is refused at commit. A replica that fetches the part attaches it from the record
  (no request to the source, none to the object store) and checks the record in its own commit multi-op.
- **Every node holds its parts in memory** (`Parts/PartFolderAccess`: `HeldParts`) and re-attaches them from
  their records when a table starts, with no LIST. A table on a CAS disk does not start without Keeper.
- **Liveness and dedup** come from `Pool/CasBlobRefCount`: one reference per distinct blob of every part this
  server holds, and one per blob a part under construction claimed. A build claims a blob before it decides;
  a blob nothing references is uploaded unconditionally under a fresh envelope, a referenced one is adopted
  with no request.
- **GC** (`Gc/CasKeeperGc`, paced by `Gc/CasGcScheduler`) runs on the writer (`cas_gc_enabled`, every
  `cas_gc_interval_sec`). Each table marks the records no replica lists and, `old_parts_lifetime` later,
  removes them in a multi-op that checks this replica's `is_active`/`host` versions (the only fencing). A blob
  a removed record named that no remaining record names is deleted only if, after its ETag is read, nothing on
  this server references it -- by that exact ETag. The pending candidates live in memory only: a writer restart
  between a record removal and the deletes leaks those blobs, never loses one.
- **Readers** (servers that do not write blobs) refuse every blob upload (`NOT_IMPLEMENTED`): an INSERT, a fetch
  of a part's bytes. They attach parts from records, clone them locally (REPLACE), and fetch merged and mutated
  parts (`always_fetch_merged_part` covers mutations on a CAS disk).
- **Refused on a content-addressed disk**, because nothing would persist the result: a table-level or loose
  file, FREEZE, DETACH, a fetch into `detached/`, a MOVE to or from another disk (`moving/`), and non-replicated
  MergeTree tables.

## Source layout

```
Primitives → Formats → Backend → Pool → Gc → Tools ≈ Parts → facade (top level)
```

A file includes only its own layer and layers to its left. `Tools` and `Parts` are siblings.

- **`Primitives/`** -- the vocabulary: `CasBlobDigest` (`BlobHashAlgo`, `BlobDigest`, `BlobRef`), `CasTypes.h`,
  `CasBlobHashingWriteBuffer` (streaming hash-and-passthrough), `CasXxh3Streamer`, `CasCodecUtil`.
- **`Formats/`** -- what is persisted: `CasBlobEnvelopeFormat` (the blob envelope), `CasPoolMetaFormat`
  (`_pool_meta`), `CasPartManifestFormat` (the in-memory part file list and the Keeper record's encoding),
  `CasLayout` (object keys), and the text-format helpers they share.
- **`Backend/`** -- the request contract: `CasBackend` (transport primitives), `CasObjectStorageBackend`,
  `CasInMemoryBackend`, `CasInstrumentedBackend` (per-key-class `ProfileEvents`), `CasRequests` (the retrying,
  `Etag`-typed layer, budgeted by `Retry` and `CasRequestBudget`).
- **`Pool/`** -- `CasPool` (`_pool_meta` + the request plane, fenced by teardown + `BlobRefCount`),
  `CasPartBlobs` (the blobs of one part under construction: the one upload path), `CasBlobRefCount`,
  `CasBlobUploadPool`, `CasPoolMeta`.
- **`Gc/`** -- `CasKeeperGc` (the round), `CasGcScheduler` (pacing thread, `system.cas_gc_log` rows).
- **`Tools/`** -- `CasInspect` (`clickhouse-disks cas-inspect`: `_pool_meta` or a blob envelope to JSON).
- **`Parts/`** -- `PartPathParser` (the ClickHouse-path classifier), `PartFolderAccess` (`PartRefKey`,
  `PartFolderView`, `HeldParts`).
- **Top level** -- `ContentAddressedMetadataStorage` (the `IMetadataStorage` facade),
  `ContentAddressedTransaction` (the `IMetadataTransaction` and its write buffers), `ContentAddressedExchange`
  (the narrow seam replication and the record GC use).

## Configuration

A CAS disk is an `object_storage` disk with `metadata_type` = `cas`:

```xml
<disks>
    <cas>
        <type>object_storage</type>
        <object_storage_type>s3</object_storage_type>
        <metadata_type>cas</metadata_type>
        <!-- Required: prefix of this server's in-memory namespaces and of its S3 staging subtree. -->
        <cas_server_root_id>replica-1</cas_server_root_id>
        <cas_gc_enabled>1</cas_gc_enabled>
        <cas_gc_interval_sec>60</cas_gc_interval_sec>
    </cas>
</disks>
```

The knob set (scratch path, staging backend, hash algorithm, request attempt budget) is declared in
`ContentAddressedSettings.cpp`, each knob documented at its declaration.

## Operations and observability

- `system.cas_gc_log` -- a Start, a row per phase and a Finish row per GC round on the writer.
- `system.cas_mounts` -- one row per CAS disk of this server: its GC's health.
- `SYSTEM CAS GC RUN | STOP | START` -- one synchronous round; pause or resume the background scheduler.
- `clickhouse-disks cas-inspect <key>` -- decode `_pool_meta` or a blob's envelope.
- `ProfileEvents` -- grep `ProfileEvents.cpp` for `CAS`.

## Testing

- **Unit tests** (`unit_tests_dbms`): every CAS suite name starts with `Cas`/`CAS`.
- **Integration**: `tests/integration/test_cas_*` (`test_cas_no_s3_metadata` is the 3c contract: writer +
  two readers, object writes per key class, no LIST, the refusals, the record GC).
