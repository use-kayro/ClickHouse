#include <Storages/MergeTree/CasKeeperPartRecord.h>

#include <Disks/IDisk.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <IO/ReadBufferFromString.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
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
}

namespace FailPoints
{
    extern const char cas_part_record_omit[];
}

String casPartRecordsPath(const String & zookeeper_path)
{
    return fs::path(zookeeper_path) / CAS_PART_RECORDS_ZNODE;
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
            out.push_back({prefix + name, checksum.file_size,
                (static_cast<UInt128>(checksum.file_hash.high64) << 64) | static_cast<UInt128>(checksum.file_hash.low64)});
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
    const ZooKeeperWithFaultInjectionPtr & zookeeper, const String & records_path, const IMergeTreeDataPart & part,
    CasPartRecordCommit how, Coordination::Requests & ops)
{
    const auto disk = part.storage.getStoragePolicy()->tryGetDiskByName(part.getDataPartStorage().getDiskName());
    const auto * ca = tryGetContentAddressedExchange(disk);
    if (!ca)
        return;
    const String path = fs::path(records_path) / part.name;
    /// A part held from its record checks it. So does a part another replica already produced (a merge
    /// every replica runs, a REPLACE a replica clones locally): the record exists, with the same content.
    if (ca->isHeldFromKeeper(part.getDataPartStorage().getRelativePath()) || zookeeper->exists(path))
    {
        ops.emplace_back(zkutil::makeCheckRequest(path, -1));
        return;
    }
    if (how != CasPartRecordCommit::Produced)
        return;
    if (auto record = makeCasPartRecord(part))
        ops.emplace_back(zkutil::makeCreateRequest(path, *record, zkutil::CreateMode::Persistent));
}

DiskPtr holdPartFromCasRecord(const Disks & candidates, const Disks & all_disks, const String & part_path, std::string_view record)
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
        }
        catch (const Exception & e)
        {
            /// A record this build cannot read is no record: the part is fetched from the replica instead.
            LOG_WARNING(getLogger("CasKeeperPartRecord"), "Ignoring the Keeper record of {}: {}", part_path, e.message());
            return nullptr;
        }

        /// A part another disk of the policy has (moved there by TTL or MOVE) stays there.
        for (const auto & other : all_disks)
            if (other != disk && other->existsDirectory(part_path))
                return nullptr;

        return ca->holdKeeperPart(part_path, decoded.files, blobs) ? disk : nullptr;
    }
    return nullptr;
}

}
