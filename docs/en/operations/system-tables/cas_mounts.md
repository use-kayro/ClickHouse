---
description: 'System table containing the GC health of every content-addressed (CAS) disk of the node.'
sidebar_label: 'cas_mounts'
sidebar_position: 31
slug: /operations/system-tables/cas_mounts
title: 'system.cas_mounts'
doc_type: 'reference'
---

## Description {#description}

The `system.cas_mounts` table contains one row per content-addressed (CAS) disk configured on the node,
read from this server's process state only (no object-store request). Only the server that writes the
pool's blobs (`merge_tree.replicated_can_become_leader`) runs the pool's garbage collector; on any other
server the GC columns stay `0`.

## Columns {#columns}

- `disk` ([String](/sql-reference/data-types/string)) — Name of the content-addressed disk.
- `is_leader` ([UInt8](/sql-reference/data-types/int-uint)) — `1` if this server's last GC round on this disk ran as the writer.
- `pending_reclaim` ([Int64](/sql-reference/data-types/int-uint)) — Blobs whose last Keeper part record this server's GC removed and that it has not deleted yet (a part it holds still references them).
- `last_success_age_seconds` ([UInt64](/sql-reference/data-types/int-uint)) — Seconds since this server's GC last ran a round as the writer on this disk (`0` if it never did).

## Example {#example}

```sql
SELECT disk, is_leader, pending_reclaim, last_success_age_seconds FROM system.cas_mounts FORMAT Vertical;
```

## See Also {#see-also}

- [`system.cas_gc_log`](/operations/system-tables/cas_gc_log) — per-round GC event log.
- [`SYSTEM CAS GC RUN`](/sql-reference/statements/system#system-cas-gc-run) — run one GC round synchronously.
