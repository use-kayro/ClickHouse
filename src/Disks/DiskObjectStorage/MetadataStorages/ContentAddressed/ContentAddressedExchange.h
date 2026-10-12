#pragma once

#include <base/types.h>
#include <base/extended_types.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

namespace DB
{

class ReadPipeline;
struct ReadSettings;

/// One file of a part as the Keeper part record (`cas_part_files`) carries it: a file whose bytes live
/// only inside a manifest, never as a blob of their own.
struct CasKeeperInlineFile
{
    String path;   /// relative to the part directory, `<proj>.proj/<file>` for a projection file
    String bytes;
};

/// One file named by a part's `checksums.txt` (a projection file as `<proj>.proj/<file>`): its size and
/// the MergeTree per-file CityHash128, which IS the `ch128` digest of the file's blob. No blob key
/// travels in the record; the reader derives every checksummed file the record does not carry.
struct CasKeeperBlobFile
{
    String path;
    UInt64 size = 0;
    UInt128 hash{};   /// `(file_hash.high64 << 64) | file_hash.low64`
};

/// A decoded Keeper part record: the pool its blobs live in and the inline files.
struct CasKeeperPartRecord
{
    String pool_uuid;   /// same rendering as `IContentAddressedExchange::getPoolUUID`
    std::vector<CasKeeperInlineFile> files;
};

/// One Keeper part record as the pool's GC sees it (`ICasKeeperRecordSource`).
struct CasKeeperRecordState
{
    String part;
    String data;                             /// the record bytes, re-`set` unchanged to mark it
    std::vector<CasKeeperBlobFile> blobs;    /// the blob-backed files it references
    bool listed = false;                     /// some replica lists the part in `/replicas/<r>/parts`
    Int32 version = 0;                       /// znode version: 0 until the GC marks the record
    Int64 mtime_ms = 0;                      /// znode mtime: when the GC marked it (or it was created)
};

/// One table's records on one disk's pool, read by its replica on this server.
struct CasKeeperRecordSnapshot
{
    bool writer = false;          /// this replica is the table's writer: the only one whose GC deletes
    std::vector<CasKeeperRecordState> records;
    UInt64 retention_ms = 0;      /// how long an unlisted record is kept after its mark (`old_parts_lifetime`)
    Int32 is_active_version = -1; /// the writer epoch the removals check (`<replica>/is_active`, `<replica>/host`)
    Int32 host_version = -1;
};

class IContentAddressedExchange;

/// A replicated table whose parts live on a content-addressed disk: it owns the Keeper client and its
/// records (`<zookeeper_path>/cas_parts`), the disk owns the pool. The pool's GC reads every such table's
/// records each round: a blob is live while some record references it, and a record goes only after its part
/// has been unlisted by every replica for `retention_ms` since the GC marked it.
class ICasKeeperRecordSource
{
public:
    virtual ~ICasKeeperRecordSource() = default;
    /// Its records on the pool of `disk` (one of its disks).
    virtual CasKeeperRecordSnapshot snapshotCasRecords(const IContentAddressedExchange & disk) = 0;
    /// Re-`set`s each record with its own bytes, checked against its version: the mark. Returns the parts
    /// whose record was marked.
    virtual Strings markCasRecords(const std::vector<const CasKeeperRecordState *> & records) = 0;
    /// Removes each record in one multi-op that checks no replica lists its part and the writer epoch of
    /// `snapshot` still holds. Returns the parts whose record was removed.
    virtual Strings removeCasRecords(
        const CasKeeperRecordSnapshot & snapshot, const std::vector<const CasKeeperRecordState *> & records) = 0;
};

/// Purpose-built seam for replication and the pool's GC: it exposes everything they need from a
/// content-addressed disk and nothing else. `ContentAddressedMetadataStorage` implements it, and callers
/// obtain the interface by casting the disk's `IMetadataStorage` to it rather than depending on the
/// concrete storage class.
class IContentAddressedExchange
{
public:
    virtual ~IContentAddressedExchange() = default;

    /// The pool's stable identity (Cas::PoolMeta::pool_id, hex): a Keeper record names the pool its blobs
    /// live in. Empty before the storage started up.
    virtual const String & getPoolUUID() const = 0;

    /// ==== Keeper part record (`cas_part_files`) ====
    /// A part persists as its Keeper record, one copy per part at the table level: the part's Inline files
    /// plus the pool, and nothing for the blob-backed files, which the reader derives from the inline
    /// `checksums.txt`. A replica materializes the part from it and the shared pool, with no request to
    /// the source replica and none to the object store.

    /// Writer: the record of the part at the disk-relative `part_path`, read from the part's published
    /// manifest -- its Inline entries -- after checking that every other file is exactly what a reader will
    /// derive: each file of `checksummed_files` that is not Inline must be a manifest Blob with
    /// `ref == {ch128, hash}` and `size`, and the manifest has no Blob outside `checksummed_files`.
    /// `nullopt` when the part has no published manifest here, or the check fails; the commit is then
    /// refused.
    virtual std::optional<String> encodeKeeperPartRecord(
        const String & part_path, const std::vector<CasKeeperBlobFile> & checksummed_files) const = 0;

    /// Reader: decodes a record; throws `CORRUPTED_DATA` on a malformed one.
    virtual CasKeeperPartRecord decodeKeeperPartRecord(std::string_view record) const = 0;

    /// Reader: makes the part directory at the disk-relative `part_path` hold exactly `inline_files` and
    /// `blob_files`, in memory and for the lifetime of this disk. No object-store request: reads of its
    /// files are served from the inline bytes and from the shared-pool blobs by digest; a rename or a
    /// removal of the directory re-keys or forgets it in memory. Returns false, changing nothing, when this
    /// disk already holds the part.
    virtual bool holdKeeperPart(const String & part_path,
                                const std::vector<CasKeeperInlineFile> & inline_files,
                                const std::vector<CasKeeperBlobFile> & blob_files) = 0;

    /// ==== `DiskObjectStorage::prepareRead` hooks ====
    /// The two CA-only reads `DiskObjectStorage::prepareRead` needs before it composes the standard
    /// object-storage pipeline. Exposed on this narrow seam (rather than only on the concrete
    /// `ContentAddressedMetadataStorage`) so `prepareRead` casts to the interface instead of coupling
    /// to the concrete storage class.

    /// The CA read entry called by `DiskObjectStorage::prepareRead` before the generic
    /// storage-objects path: serves in-manifest bytes (mutable per-part files, inline entries,
    /// verbatim namespace files) from memory. Returns false when the path is not in-manifest.
    virtual bool prepareInManifestRead(const std::string & path, const ReadSettings & settings, ReadPipeline & pipeline) const = 0;

    /// Translates a blob-backed part file to the physical blob
    /// object plus the payload WINDOW inside it (the CHCA envelope header occupies
    /// [0, payload_offset)). DiskObjectStorage::prepareRead composes the STANDARD object-storage
    /// pipeline over `object` (gather/caches/async prefetch — the same chain plain s3 disks get,
    /// so `MergeTreeReaderStream` right-mark bounds reach the object reader and its range
    /// requests stay drainable) and bounds it with the pipeline's FileView stage.
    /// nullopt = the path is not a blob-backed part file (caller falls through; absent paths
    /// then fail in getStorageObjects exactly as before).
    struct BlobViewPlan
    {
        StoredObject object;        /// physical blob key; logical path; readable extent (envelope + payload)
        size_t payload_offset = 0;  /// view left bound inside the blob
        size_t payload_end = 0;     /// view right bound (payload_offset + payload length)
    };
    /// Resolves a blob-backed path to its physical object and payload window. Returns nullopt for
    /// in-manifest, loose, directory, or otherwise unresolved paths.
    virtual std::optional<BlobViewPlan> getBlobViewPlan(const std::string & path) const = 0;
};

/// The one refusal (`NOT_IMPLEMENTED`) for whatever a content-addressed disk cannot persist: anything but a
/// part a ReplicatedMergeTree table commits -- a table-level file, DETACH, FREEZE, a MOVE to another disk.
/// A part's only persistent copy is its Keeper record. `what` names the refused operation or target.
[[noreturn]] void throwNotPersistedOnContentAddressedDisk(const String & what);

/// The tables on `disk` whose Keeper records its pool's GC reads, or nullopt when some table on it cannot be
/// read this round (not loaded yet, or not replicated: its blobs are unknown). Storages installs it once at
/// startup (it walks `DatabaseCatalog`); until then, and in tools without tables, a disk's GC does nothing.
using CasKeeperRecordSourcesProvider
    = std::optional<std::vector<std::shared_ptr<ICasKeeperRecordSource>>> (*)(const IContentAddressedExchange & disk);
void setCasKeeperRecordSourcesProvider(CasKeeperRecordSourcesProvider provider);
CasKeeperRecordSourcesProvider getCasKeeperRecordSourcesProvider();

}
