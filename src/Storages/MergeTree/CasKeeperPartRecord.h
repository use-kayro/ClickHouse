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

/// A part on a content-addressed disk persists as ONE Keeper record per part at the table level:
/// `<zookeeper_path>/cas_parts/<part>` (`cas_part_files`) holds the Inline entries of the part's manifest
/// (`checksums.txt`, `columns.txt`, `count.txt`, ...) and the pool. The blob-backed files are not repeated:
/// `checksums.txt` names them with their size and CityHash128, which is the `ch128` blob digest. The replica
/// that commits a part it produced creates the node, in the same multi-op as its part znode; a replica that
/// fetches the part attaches it from the node and the shared pool (no request to the source replica, no
/// request to the object store) and checks, in its own commit multi-op, that the node still exists. On
/// restart a replica re-attaches its parts from the nodes.
///
/// A node is NOT removed with any replica's part znode: the pool's GC removes it only once no replica lists
/// the part.
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

/// What a commit of `part` adds to `ops` for its Keeper record `<zookeeper_path>/cas_parts/<part>`.
/// `copied_from_replica_path` is `nullopt` for the commit that PRODUCES the part (insert, merge, mutation,
/// REPLACE/ATTACH PARTITION clone), else the replica the part was copied from.
///  - Produced: the record -- the Inline entries of the part's published manifest, every other file being
///    exactly what a reader derives from its checksums (`IContentAddressedExchange::encodeKeeperPartRecord`) --
///    is created as a create-if-absent (another replica that produced the same part wrote the same immutable
///    bytes). A part that cannot have one is refused (`NOT_IMPLEMENTED`): on a content-addressed disk it would
///    not survive a restart. Requires Keeper's `CREATE_IF_NOT_EXISTS` (`NOT_IMPLEMENTED` otherwise).
///  - Copied: never created; the commit checks that the record exists. The check fails once the pool's GC
///    removed the record, which it does only when no replica lists the part, so a copy can never resurrect a
///    record or list a part whose blobs may be gone.
/// Called where the part znode is born, after the part's disk transaction is committed (so its manifest is
/// published).
void addCasPartRecordOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_path, const IMergeTreeDataPart & part,
    const std::optional<String> & copied_from_replica_path, Coordination::Requests & ops);

/// `CasKeeperRecordSourcesProvider` (installed by `registerStorageMergeTree`): the replicated tables in
/// `DatabaseCatalog` with a disk that is `disk`; nullopt when a table on `disk` is not replicated (no records:
/// its blobs are unknown), or some table is not loaded yet or detached (its records are not read).
std::optional<std::vector<std::shared_ptr<ICasKeeperRecordSource>>> casKeeperRecordSourcesOnDisk(const IContentAddressedExchange & disk);

/// The one multi-op that removes the record of `part` (at `<zookeeper_path>/cas_parts/<part>`, at
/// `record_version`): it checks that no replica of `replicas` lists the part and that the writer epoch the GC
/// read -- the versions of `<replica_path>/is_active` and `<replica_path>/host` -- still holds.
Coordination::Requests casRecordRemovalOps(
    const zkutil::ZooKeeper & zookeeper, const String & zookeeper_path, const String & replica_path, const Strings & replicas,
    const String & part, Int32 record_version, Int32 is_active_version, Int32 host_version);

/// The blob-backed files a decoded record references: every file its `checksums.txt` (and each projection's)
/// names that the record does not carry inline. Throws `CORRUPTED_DATA` when it has no `checksums.txt`.
std::vector<CasKeeperBlobFile> casRecordBlobFiles(const CasKeeperPartRecord & decoded);

/// Make the disk-relative part directory `part_path` hold the part `record` describes, on the disk of
/// `candidates` whose pool the record names. Returns that disk, or nullptr -- changing nothing -- when no
/// candidate is on the pool, the record is malformed (logged), or the chosen disk already holds the part.
DiskPtr holdPartFromCasRecord(const Disks & candidates, const String & part_path, std::string_view record);

}
