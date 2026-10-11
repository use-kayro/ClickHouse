#include <Storages/MergeTree/CasKeeperPartRecord.h>

#include <Disks/IDisk.h>
#include <Disks/IStoragePolicy.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>

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

IContentAddressedExchange * tryGetContentAddressedExchange(const DiskPtr & disk)
{
    if (!disk || !disk->isContentAddressed())
        return nullptr;
    return dynamic_cast<IContentAddressedExchange *>(disk->getMetadataStorage().get());
}

namespace
{

/// Walks one part (or projection part) directory: the record files go to `files`, every other file must
/// be named by this directory's own checksums, or the record could not be read back.
bool collectRecordFiles(
    const IMergeTreeDataPart & part, const String & prefix, const IContentAddressedExchange & ca,
    std::vector<CasKeeperInlineFile> & files)
{
    const auto & storage = part.getDataPartStorage();
    const auto & projections = part.getProjectionParts();
    for (auto it = storage.iterate(); it->isValid(); it->next())
    {
        const String name = it->name();
        if (name.ends_with(".proj"))
        {
            auto proj = projections.find(name.substr(0, name.size() - std::string_view(".proj").size()));
            if (proj == projections.end() || !collectRecordFiles(*proj->second, prefix + name + "/", ca, files))
                return false;
            continue;
        }
        const size_t size = storage.getFileSize(name);
        if (ca.isKeeperRecordFile(prefix + name, size))
        {
            String bytes;
            auto in = storage.readFile(name, getReadSettings(), size);
            readStringUntilEOF(bytes, *in);
            files.push_back({prefix + name, std::move(bytes)});
        }
        else if (!part.checksums.files.contains(name))
            return false;
    }
    return true;
}

/// The blob-backed files of the directory whose `checksums.txt` is `checksums_bytes`: every entry the
/// record does not carry, except the `<proj>.proj` entries, which name a projection directory.
void deriveBlobFiles(
    const String & prefix, const String & checksums_bytes, const std::unordered_set<String> & inline_paths,
    std::vector<CasKeeperBlobFile> & blobs)
{
    MergeTreeDataPartChecksums checksums;
    ReadBufferFromString in(checksums_bytes);
    checksums.read(in);
    for (const auto & [name, checksum] : checksums.files)
    {
        const String path = prefix + name;
        if (name.ends_with(".proj") || inline_paths.contains(path))
            continue;
        blobs.push_back({path, checksum.file_size,
            (static_cast<UInt128>(checksum.file_hash.high64) << 64) | static_cast<UInt128>(checksum.file_hash.low64)});
    }
}

std::optional<String> makeCasPartRecord(const IMergeTreeDataPart & part, const IContentAddressedExchange & ca)
{
    /// Test-only: commit the part the way a build without Keeper records did, so the fetch paths that still
    /// serve such parts (relink, bytes) stay exercised until existing parts are migrated.
    fiu_do_on(FailPoints::cas_part_record_omit, { return std::nullopt; });
    std::vector<CasKeeperInlineFile> files;
    if (!collectRecordFiles(part, "", ca, files))
        return std::nullopt;
    return ca.encodeKeeperPartRecord(files);
}

}

std::optional<String> makeCasPartRecord(const IMergeTreeDataPart & part)
{
    const auto disk = part.storage.getStoragePolicy()->tryGetDiskByName(part.getDataPartStorage().getDiskName());
    const auto * ca = tryGetContentAddressedExchange(disk);
    if (!ca)
        return std::nullopt;
    return makeCasPartRecord(part, *ca);
}

void addCasPartRecordOps(
    const zkutil::ZooKeeperPtr & zookeeper, const String & records_path, const IMergeTreeDataPart & part, Coordination::Requests & ops)
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
    if (auto record = makeCasPartRecord(part, *ca))
        ops.emplace_back(zkutil::makeCreateRequest(path, *record, zkutil::CreateMode::Persistent));
}

DiskPtr holdPartFromCasRecord(const Disks & candidates, const String & part_path, std::string_view record)
{
    for (const auto & disk : candidates)
    {
        auto * ca = tryGetContentAddressedExchange(disk);
        if (!ca)
            continue;
        CasKeeperPartRecord decoded = ca->decodeKeeperPartRecord(record);
        if (decoded.pool_uuid != ca->getPoolUUID())
            continue;

        std::unordered_set<String> inline_paths;
        for (const auto & f : decoded.files)
            inline_paths.insert(f.path);
        if (!inline_paths.contains("checksums.txt"))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Keeper part record for '{}' has no checksums.txt", part_path);

        std::vector<CasKeeperBlobFile> blobs;
        for (const auto & f : decoded.files)
        {
            const std::string_view path = f.path;
            if (path == "checksums.txt")
                deriveBlobFiles("", f.bytes, inline_paths, blobs);
            else if (path.ends_with("/checksums.txt"))
                deriveBlobFiles(String(path.substr(0, path.size() - std::string_view("checksums.txt").size())), f.bytes, inline_paths, blobs);
        }
        return ca->holdKeeperPart(part_path, decoded.files, blobs) ? disk : nullptr;
    }
    return nullptr;
}

}
