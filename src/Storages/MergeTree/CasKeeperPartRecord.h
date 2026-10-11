#pragma once

#include <Core/Types.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedExchange.h>
#include <Common/ZooKeeper/ZooKeeper.h>

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
/// files whose bytes exist only inside a manifest (`checksums.txt`, `columns.txt`, `count.txt`, ...) and the
/// pool. The blob-backed files are not repeated: `checksums.txt` names them with their size and CityHash128,
/// which is the `ch128` blob digest. The replica that produces a part creates the node in the same multi-op
/// as its own part znode; a replica that fetches the part attaches it from the node and the shared pool (no
/// request to the source replica, no write or LIST to the object store) and checks, in its own commit
/// multi-op, that the node still exists. On restart a replica re-attaches its parts from the nodes.
///
/// A node is NOT removed with any replica's part znode: it outlives the writer's removal of the part, so
/// that the pool's GC can later remove it only once no replica lists the part (step 2 of the plan, which is
/// also what removes it at all; until then the nodes of merged-away parts accumulate).
inline constexpr std::string_view CAS_PART_RECORDS_ZNODE = "cas_parts";

/// Resolve a disk to the content-addressed exchange facade, or nullptr if the disk is not CA. The cast
/// targets the purpose-built INTERFACE, never the concrete metadata-storage class.
IContentAddressedExchange * tryGetContentAddressedExchange(const DiskPtr & disk);

/// What a commit of `part` adds to `ops` for its Keeper record at `<records_path>/<part>`:
///  - the part is held from its record (it was attached from Keeper), or the record already exists (another
///    replica produced the same part): a check that the node exists;
///  - else, the node is created -- unless the part is not on a content-addressed disk, or a file outside the
///    record is not named by `checksums.txt` (a reader could not derive it); then nothing is added and the
///    part replicates by the ordinary fetch.
/// (Not a create-if-not-exists op: Keeper's `CREATE_IF_NOT_EXISTS` feature can be disabled.)
void addCasPartRecordOps(
    const zkutil::ZooKeeperPtr & zookeeper, const String & records_path, const IMergeTreeDataPart & part, Coordination::Requests & ops);

/// The Keeper record of `part` -- the same bytes a commit writes -- or `nullopt` when the part is not on a
/// content-addressed disk or its record could not be read back (a file outside the record not named by
/// `checksums.txt`).
std::optional<String> makeCasPartRecord(const IMergeTreeDataPart & part);

/// Make the disk-relative part directory `part_path` hold the part `record` describes, on the disk of
/// `candidates` whose pool the record names. Returns that disk, or nullptr when none of them is on the pool
/// or that disk's ref table already holds the part (which then stays as it is). Throws `CORRUPTED_DATA` on
/// a malformed record.
DiskPtr holdPartFromCasRecord(const Disks & candidates, const String & part_path, std::string_view record);

}
