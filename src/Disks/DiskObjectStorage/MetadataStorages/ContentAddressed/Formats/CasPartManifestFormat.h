#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <base/types.h>
#include <base/extended_types.h>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace DB::Cas
{

/// The file list of one part (`PartManifest`), held in memory (`Parts/PartFolderAccess`), and the text codec of
/// its Keeper record (`cas_part_files`, below).

/// Where a manifest entry's file bytes live. There are no nested tree objects: a directory is a path
/// prefix, not a placement. `Inline` bytes belong to the manifest's raw payload zone; `Blob` bytes
/// are stored separately under `blobKey`.
enum class EntryPlacement : uint8_t
{
    Inline = 1,   /// bytes embedded in `inline_bytes`
    Blob = 2,     /// bytes stored as a content-addressed blob at `blobKey`
};

/// Canonical wire word for one manifest entry placement.
std::string_view entryPlacementToWireWord(EntryPlacement placement);

/// Its fail-closed inverse: an unknown word is `CORRUPTED_DATA`.
EntryPlacement entryPlacementFromWireWord(std::string_view w);

/// One file entry inside a part manifest. `ref` is meaningful only for `Blob`; `inline_bytes` only
/// for `Inline`. `blob_size` is the raw `Blob` byte count (0 for `Inline` — decode never fills it for
/// an inline entry, since the wire format carries no redundant size for inline bytes). Use `size()`
/// for the entry's logical file size regardless of placement; no consumer should branch on
/// `placement` just to answer "how big is this file". `ref` is the full blob identity: the algorithm
/// travels with the digest, per entry, so a manifest may mix hash algorithms. A bare digest is never
/// the identity.
struct ManifestEntry
{
    String path;
    EntryPlacement placement = EntryPlacement::Inline;
    BlobRef ref{};
    uint64_t blob_size = 0;
    String inline_bytes;
    bool operator==(const ManifestEntry &) const = default;

    /// The single source of truth for this entry's logical file size, independent of where its bytes
    /// live. Decoding an `Inline` entry leaves `blob_size == 0`, because the wire record has no
    /// redundant blob size; a carried-forward inline entry can therefore be non-empty even though
    /// `blob_size` is zero. Consumers that need the logical file size must use this method rather
    /// than inspecting the placement-specific fields themselves.
    uint64_t size() const { return placement == EntryPlacement::Inline ? inline_bytes.size() : blob_size; }
};

/// The files of one part, strictly ascending by `path` (`sortedUniqueEntries`), which permits index-free
/// binary-search and prefix-range lookup. Entries carry their own digest algorithm, so one part may mix them.
struct PartManifest
{
    std::vector<ManifestEntry> entries;
    bool operator==(const PartManifest &) const = default;
};

/// Text codec for `cas_part_files`, a part's Keeper record: one copy per part at the table level,
/// `<zookeeper_path>/cas_parts/<part>` (`Storages/MergeTree/CasKeeperPartRecord.h`). Immutable per part name
/// (a part name is never reused within a table). It carries exactly the part's Inline entries -- the files
/// whose bytes exist nowhere but inside a manifest -- and the pool their blobs live in:
///
///   header line                 {"type":"cas_part_files","v":N}
///   descriptor meta line         {"pool":"<32 hex pool_id>"}
///   one entry-record line each   {"path":path,"place":"inline","size":n}, in canonical path order
///   trailer line                 {"n":entry-count}
///   PAYLOAD ZONE (raw, follows the trailer): for each entry, in path order, a `head -v`-style banner line
///                                `==> "<escaped path>" size=<n> <==\n`, then exactly `n` raw bytes, then `\n`
///                                (`inline_bytes` is arbitrary binary, so it rides outside the JSON lines).
///
/// The Blob entries are not repeated: the inline `checksums.txt` already names every other file with its
/// size and its CityHash128, which is the `ch128` blob digest. `encodePartFiles` throws `LOGICAL_ERROR`
/// for a Blob entry; `decodePartFiles` throws `CORRUPTED_DATA` for one.
struct PartFiles
{
    UInt128 pool_id{};
    std::vector<ManifestEntry> entries;   /// Inline only
};
String encodePartFiles(const PartFiles & part_files);
PartFiles decodePartFiles(std::string_view data);

/// Canonical path order with duplicate-path rejection (`CORRUPTED_DATA`, naming `what`): the order every
/// entry-record codec writes and every part view relies on. The pointers alias `entries`.
std::vector<const ManifestEntry *> sortedUniqueEntries(const std::vector<ManifestEntry> & entries, std::string_view what);

/// Pure ordered-entry lookup primitives over entries strictly ascending by `path`.

/// Binary search. Returns nullptr when absent. The pointer aliases `entries` -- do not outlive it.
const ManifestEntry * findEntry(const std::vector<ManifestEntry> & entries, std::string_view path);

/// The contiguous [first, last) sub-span of entries whose path starts with `dir_prefix` (canonical
/// order makes matches contiguous). Empty prefix = the whole span.
std::pair<const ManifestEntry *, const ManifestEntry *>
entryRange(const std::vector<ManifestEntry> & entries, std::string_view dir_prefix);

}
