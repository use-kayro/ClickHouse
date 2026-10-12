# CAS persisted formats — the living registry

Every persisted CAS object is a text file: header line `{"type":"cas_<object>","v":N}`, body
(one JSON object / sorted records / raw payload zone), optional `{"n":…}` trailer, stored raw.
`CasTextFormat.{h,cpp}` is the only code that knows this shape. There are no binary CAS formats and no protobuf dependency.

**Rule:** any change to a persisted format lands in the SAME commit as its row here.

## Bucket map

The object store holds `_pool_meta` and blobs, nothing else (TEC-851 step 3c). A part's persistent
copy is its Keeper record.

| Key | Object | Codec | Writer |
|---|---|---|---|
| `_pool_meta` (under the pool prefix) | pool identity + floors (`pool_id`, `blob_header_len`, `min_reader_generation`, `algos_used` array) | `CasPoolMetaFormat` | the pool's writer, once, on a brand-new pool |
| blob keys (`CasLayout::blobKey`) | blob envelope (`type`, `v`, `tag`, `build`, `time_ms`, `creator`, `op`, `chver`, `ref`) + payload | `CasBlobEnvelopeFormat` | the pool's writer (uploads) |
| Keeper, not S3: `<zookeeper_path>/cas_parts/<part>` (one copy per part) | descriptor `pool`; the part's Inline entries (entry `path`, `place`=`inline`, `size`, then the payload zone); Blob entries are derived from the inline `checksums.txt` | `CasPartManifestFormat` (`encodePartFiles`) | `ReplicatedMergeTree` part commit |

## Codec table

Authoritative per-format traits (type string, family, strictness, caps) live
in `CasFormat.cpp` (`TRAITS`), asserted complete by `gtest_cas_text_format.cpp`.

Key naming follows a deliberate split between metadata written once per object and fields repeated
once per record, not a flat character-count budget:

- metadata written once per object (`namespace`, `writer_epoch`, `blob_header_len`, …) uses
  descriptive names;
- fields repeated once per record (`ref`, `mark`, `op`, `class`, `place`, …) use short, semantic
  words whose meaning is clear in the record rather than the C++ member name verbatim;
- the fixed `cas_blob` descriptor uses its own separately budgeted compact vocabulary (`tag`,
  `build`, `chver`, …), because it must fit before the pool-wide fixed payload offset;
- common framing stays `type`, `v`, and `n`;
- `!` stays the must-understand prefix for critical fields;
- C++ member names obey an asymmetric rule: a member may be fuller than its wire key, never more
  cryptic than it.

Exact full C++ member names everywhere were deliberately rejected — see
`docs/superpowers/specs/2026-08-28-cas-semantic-wire-keys-design.md` ("Rejected alternatives").
Fixed-width `UInt128` identities render as 32-char lowercase hex strings; blob digests render as
algo-width hex (two chars per digest byte), with their algo name (`sha256:ab12…`) wherever a bare
hex would be ambiguous; unbounded u64 = decimal strings; bounded counts/lengths/ms-timestamps =
numbers; units documented here per object as codecs land.

`CasWireVocab.{h,cpp}` owns repeated value fields: `BlobRef` uses `algo`/`digest`.

## Evolution rules (one screen)

- `v` (header line) is the ONLY version field; reader gate: `v > G_BUILD` →
  `UNKNOWN_FORMAT_VERSION`, checked before the body.
- Additive change = new tolerant key, no `v` bump; on MUTABLE objects the field is best-effort
  until the pool floor rises (an old writer's fresh re-encode drops it).
- Breaking change = `v` bump + `changePoints` + write-down-to-floor; the floor raise is what
  fences old builds out (mount gates: `min_reader_generation` forward, pool-meta `v` backward).
- A key prefixed `!` is critical: a reader that does not understand it fails closed.
- Padding zones (blob header pad, part-files banners) are deterministic and verified — no
  unaccounted bytes in any object.

## Generation history {#generation-history}

The format's generation history was reset to a flat `{1, 1}` baseline (`G_BUILD == 1`): CAS is
pre-release, carries no persisted data, and pays no compatibility cost for starting the count over.
Every class's `changePoints` begins at generation 1; a future breaking change appends a real entry to
that class's own array and bumps `G_BUILD`, the same way it always has.
