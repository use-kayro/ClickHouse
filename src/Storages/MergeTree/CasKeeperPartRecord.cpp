#include <Storages/MergeTree/CasKeeperPartRecord.h>

#include <Disks/IDisk.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Databases/IDatabase.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>

#include <filesystem>
#include <unordered_set>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char cas_part_record_omit[];
}

String casPartRecordsPath(const String & zookeeper_path)
{
    return fs::path(zookeeper_path) / CAS_PART_RECORDS_ZNODE;
}

String casPartRecordPath(const String & zookeeper_path, const String & part_name)
{
    return fs::path(casPartRecordsPath(zookeeper_path)) / part_name;
}

UInt128 casBlobDigestOf(const MergeTreeDataPartChecksum & checksum)
{
    return (static_cast<UInt128>(checksum.file_hash.high64) << 64) | static_cast<UInt128>(checksum.file_hash.low64);
}

IContentAddressedExchange * tryGetContentAddressedExchange(const DiskPtr & disk)
{
    if (!disk || !disk->isContentAddressed())
        return nullptr;
    return dynamic_cast<IContentAddressedExchange *>(disk->getMetadataStorage().get());
}

Disks casRecordDisks(const Disks & disks)
{
    Disks out;
    for (const auto & disk : disks)
        if (tryGetContentAddressedExchange(disk) && !disk->isReadOnly())
            out.push_back(disk);
    return out;
}

namespace
{

/// The files `checksums` names under `prefix`, with their size and hash; `<proj>.proj` entries name a
/// projection directory, not a file. The one conversion writer and reader share.
void appendChecksummedFiles(const String & prefix, const MergeTreeDataPartChecksums & checksums, std::vector<CasKeeperBlobFile> & out)
{
    for (const auto & [name, checksum] : checksums.files)
        if (!name.ends_with(".proj"))
            out.push_back({prefix + name, checksum.file_size, casBlobDigestOf(checksum)});
}

}

std::optional<String> makeCasPartRecord(const IMergeTreeDataPart & part)
{
    /// Test-only: commit the part as a build without Keeper records did, so the fetch paths that serve
    /// parts without a record (relink, bytes) stay exercised.
    fiu_do_on(FailPoints::cas_part_record_omit, { return std::nullopt; });
    const auto disk = part.storage.getStoragePolicy()->tryGetDiskByName(part.getDataPartStorage().getDiskName());
    const auto * ca = tryGetContentAddressedExchange(disk);
    if (!ca)
        return std::nullopt;
    std::vector<CasKeeperBlobFile> checksummed;
    appendChecksummedFiles("", part.checksums, checksummed);
    for (const auto & [name, projection] : part.getProjectionParts())
        appendChecksummedFiles(name + ".proj/", projection->checksums, checksummed);
    return ca->encodeKeeperPartRecord(part.getDataPartStorage().getRelativePath(), checksummed);
}

void addCasPartRecordOps(
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & zookeeper_path, const IMergeTreeDataPart & part,
    const std::optional<String> & copied_from_replica_path, Coordination::Requests & ops)
{
    if (!part.getDataPartStorage().isContentAddressed())
        return;
    const String path = casPartRecordPath(zookeeper_path, part.name);
    if (!copied_from_replica_path)
    {
        auto record = makeCasPartRecord(part);
        if (!record)
            return;
        if (!zookeeper->isFeatureEnabled(KeeperFeatureFlag::CREATE_IF_NOT_EXISTS))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Keeper server doesn't have the feature flag CREATE_IF_NOT_EXISTS, required by the Keeper part records of "
                "content-addressed parts");
        ops.emplace_back(zkutil::makeCreateRequest(path, *record, zkutil::CreateMode::Persistent, /*ignore_if_exists*/ true));
        return;
    }
    const auto disk = part.storage.getStoragePolicy()->tryGetDiskByName(part.getDataPartStorage().getDiskName());
    const auto * ca = tryGetContentAddressedExchange(disk);
    if (ca && (ca->isHeldFromKeeper(part.getDataPartStorage().getRelativePath()) || zookeeper->exists(path)))
        ops.emplace_back(zkutil::makeCheckRequest(path, -1));
    else
        ops.emplace_back(zkutil::makeCheckRequest(fs::path(*copied_from_replica_path) / "parts" / part.name, -1));
}

std::optional<std::vector<std::shared_ptr<ICasKeeperRecordSource>>> casKeeperRecordSourcesOnDisk(const IContentAddressedExchange & disk)
{
    const auto context = Context::getGlobalContextInstance();
    std::vector<std::shared_ptr<ICasKeeperRecordSource>> sources;
    for (const auto & [database_name, database] : DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false, .with_remote_databases = false}))
    {
        /// `skip_not_loaded=false`: a table still loading or starting up would otherwise be missing from the
        /// snapshot, and its records unread.
        for (auto it = database->getTablesIterator(context, {}, /*skip_not_loaded=*/ false); it->isValid(); it->next())
        {
            const auto table = it->table();
            const auto * data = dynamic_cast<const MergeTreeData *>(table.get());
            if (!data || std::ranges::none_of(data->getDisks(), [&](const DiskPtr & d) { return tryGetContentAddressedExchange(d) == &disk; }))
                continue;
            auto replicated = std::dynamic_pointer_cast<StorageReplicatedMergeTree>(table);
            if (!replicated)
                return std::nullopt;
            sources.push_back(std::move(replicated));
        }
        /// Detached: its parts may still reference blobs, but its records are not read. (A table out of the
        /// catalog for a moment -- a reload -- needs no guard: a blob goes only after rounds of its condemnation
        /// and of its deletion both found no record referencing it.)
        if (database->getDetachedTablesIterator(context, {}, /*skip_not_loaded=*/ true)->isValid())
            return std::nullopt;
    }
    return sources;
}

Coordination::Requests casRecordRemovalOps(
    const zkutil::ZooKeeper & zookeeper, const String & zookeeper_path, const String & replica_path, const Strings & replicas,
    const String & part, Int32 record_version, Int32 is_active_version, Int32 host_version)
{
    Coordination::Requests ops;
    for (const auto & replica : replicas)
        zkutil::addCheckNotExistsRequest(ops, zookeeper, fs::path(zookeeper_path) / "replicas" / replica / "parts" / part);
    ops.emplace_back(zkutil::makeCheckRequest(fs::path(replica_path) / "is_active", is_active_version));
    ops.emplace_back(zkutil::makeCheckRequest(fs::path(replica_path) / "host", host_version));
    ops.emplace_back(zkutil::makeRemoveRequest(fs::path(casPartRecordsPath(zookeeper_path)) / part, record_version));
    return ops;
}

std::vector<CasKeeperBlobFile> casRecordBlobFiles(const CasKeeperPartRecord & decoded)
{
    std::vector<CasKeeperBlobFile> blobs;
    std::unordered_set<std::string_view> inline_paths;
    bool has_checksums = false;
    for (const auto & f : decoded.files)
    {
        inline_paths.insert(f.path);
        if (f.path != "checksums.txt" && !f.path.ends_with("/checksums.txt"))
            continue;
        has_checksums |= f.path == "checksums.txt";
        MergeTreeDataPartChecksums checksums;
        ReadBufferFromString in(f.bytes);
        checksums.read(in);
        appendChecksummedFiles(f.path.substr(0, f.path.size() - std::string_view("checksums.txt").size()), checksums, blobs);
    }
    if (!has_checksums)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "no checksums.txt");
    std::erase_if(blobs, [&](const CasKeeperBlobFile & f) { return inline_paths.contains(f.path); });
    return blobs;
}

DiskPtr holdPartFromCasRecord(const Disks & candidates, const String & part_path, std::string_view record)
{
    for (const auto & disk : candidates)
    {
        auto * ca = tryGetContentAddressedExchange(disk);
        if (!ca)
            continue;

        CasKeeperPartRecord decoded;
        std::vector<CasKeeperBlobFile> blobs;
        try
        {
            decoded = ca->decodeKeeperPartRecord(record);
            if (decoded.pool_uuid != ca->getPoolUUID())
                continue;
            blobs = casRecordBlobFiles(decoded);
        }
        catch (const Exception & e)
        {
            /// A record this build cannot read is no record: the part is fetched from the replica instead.
            LOG_WARNING(getLogger("CasKeeperPartRecord"), "Ignoring the Keeper record of {}: {}", part_path, e.message());
            return nullptr;
        }

        return ca->holdKeeperPart(part_path, decoded.files, blobs) ? disk : nullptr;
    }
    return nullptr;
}

}
