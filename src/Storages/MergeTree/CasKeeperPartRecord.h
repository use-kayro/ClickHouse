#pragma once

#include <Core/Types.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
using Disks = std::vector<DiskPtr>;
class IMergeTreeDataPart;

/// The metadata of a part on a content-addressed disk can live in Keeper, as ONE copy per part at the table
/// level: `<zookeeper_path>/cas_parts/<part>` holds the part's Keeper record (`cas_part_files`) -- the
/// Inline entries of the part's manifest (`checksums.txt`, `columns.txt`, `count.txt`, ...) and the pool.
/// The blob-backed files are not repeated: `checksums.txt` names them with their size and CityHash128,
/// which is the `ch128` blob digest. A replica that commits a part it holds in its own ref table creates the
/// node, in the same multi-op as its part znode, if it is absent; a replica that fetches the part attaches
/// it from the node and the shared pool (no request to the source replica, no write or LIST to the object
/// store) and checks, in its own commit multi-op, that the node still exists. On restart a replica
/// re-attaches its parts from the nodes, and creates the missing nodes of its active parts (backfill).
///
/// A node is NOT removed with any replica's part znode: it outlives the writer's removal of the part, so
/// that the pool's GC can later remove it only once no replica lists the part (step 2 of the plan, which is
/// also what removes it at all; until then the nodes of merged-away parts accumulate).
inline constexpr std::string_view CAS_PART_RECORDS_ZNODE = "cas_parts";
String casPartRecordsPath(const String & zookeeper_path);
String casPartRecordPath(const String & zookeeper_path, const String & part_name);

struct MergeTreeDataPartChecksum;
/// The `ch128` blob digest of a checksummed file: its MergeTree `file_hash`.
UInt128 casBlobDigestOf(const MergeTreeDataPartChecksum & checksum);

/// Resolve a disk to the content-addressed exchange facade, or nullptr if the disk is not CA. The cast
/// targets the purpose-built INTERFACE, never the concrete metadata-storage class.
IContentAddressedExchange * tryGetContentAddressedExchange(const DiskPtr & disk);

/// The disks of `disks` a Keeper record can hold a part on: content-addressed and writable.
Disks casRecordDisks(const Disks & disks);

/// The Keeper record of `part`: the Inline entries of its published manifest, provided every other file
/// is exactly what a reader derives from its checksums (`IContentAddressedExchange::encodeKeeperPartRecord`).
/// `nullopt` when the part is not on a content-addressed disk, has no published manifest yet, or fails
/// that check; the part then replicates by the ordinary fetch.
std::optional<String> makeCasPartRecord(const IMergeTreeDataPart & part);

/// What a commit of `part` adds to `ops` for its Keeper record `<zookeeper_path>/cas_parts/<part>`.
/// `copied_from_replica_path` is `nullopt` for the commit that PRODUCES the part (insert, merge, mutation,
/// local REPLACE clone, ATTACH of a local detached part), else the replica the part was copied from.
///  - Produced: the record is created when the part has one (`makeCasPartRecord`), as a create-if-absent
///    (another replica that produced the same part wrote the same immutable bytes). Requires Keeper's
///    `CREATE_IF_NOT_EXISTS` (`NOT_IMPLEMENTED` otherwise).
///  - Copied: never created. The commit checks the record when the part was attached from it or the record
///    exists; else -- the producer could not make one -- it checks that the source replica still lists the
///    part. Either check fails once the pool's GC (step 2) removed the record, which it does only when no
///    replica lists the part, so a copy can never resurrect a record or list a part whose blobs may be gone.
/// Records are immutable and never removed in step 1. Called where the part znode is born, after the part's
/// disk transaction is committed (so its manifest is published; an unpublished part gets no record).
void addCasPartRecordOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_path, const IMergeTreeDataPart & part,
    const std::optional<String> & copied_from_replica_path, Coordination::Requests & ops);

/// Make the disk-relative part directory `part_path` hold the part `record` describes, on the disk of
/// `candidates` whose pool the record names. Returns that disk, or nullptr -- changing nothing -- when no
/// candidate is on the pool, the record is malformed (logged), or the chosen disk's ref table already holds
/// the part.
DiskPtr holdPartFromCasRecord(const Disks & candidates, const String & part_path, std::string_view record);

}
