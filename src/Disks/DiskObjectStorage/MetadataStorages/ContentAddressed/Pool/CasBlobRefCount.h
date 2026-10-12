#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <base/types.h>

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace DB::Cas
{

/// How many references this server holds to each blob of the pool, in memory. It is the ONLY liveness and
/// dedup state the CAS keeps; nothing of it is written to the object store.
///
/// A reference is held by a part this disk holds in memory (from its Keeper record or written here: one per
/// distinct blob of the part, `HeldParts`) or by a part under construction (one per blob it claimed,
/// `PartBlobs`). The Keeper part records themselves are read by the GC each round (`KeeperGc`): this count is
/// what records cannot show -- what this process holds and builds right now.
///
/// THE INVARIANT that makes a blob delete safe against a concurrent build of the same content (the race
/// the step-2 dedup had: adopt an existing body without re-upload while GC deletes it):
///  - a build claims every blob it will reference BEFORE deciding whether to upload it (`claimForBuild`),
///    under the lock, and uploads it -- unconditionally, under a fresh envelope, so a fresh ETag -- exactly
///    when nothing referenced it at that instant;
///  - GC reads the blob's ETag FIRST, then, under the same lock, checks that nothing references it and no
///    upload of it is in flight (`unreferencedNow`), and only then deletes it, by that exact ETag.
/// Any interleaving is safe: either GC sees the build's claim and skips, or the build claimed after GC's
/// check, saw no reference and uploaded -- after GC's ETag read, so GC's exact delete finds a different ETag
/// (`Mismatch`), or lands first and the upload recreates the body. A build that finds the blob referenced
/// adopts it without any request: something on this server holds it, so it exists and GC skips it.
/// The invariant covers builds of THIS process only; that is why only one server per pool writes blobs
/// (the replica that can become merge leader, TEC-851 decision of 11/10).
class BlobRefCount
{
public:
    /// One more reference to each of `refs`.
    void add(const std::vector<BlobRef> & refs);
    /// One reference fewer to each of `refs` (each previously added or claimed).
    void release(const std::vector<BlobRef> & refs);

    enum class Claim : uint8_t
    {
        Upload,    /// nothing referenced the blob: the caller uploads it, then calls `uploadFinished`
        Present,   /// something on this server references it: adopt, no request
    };
    /// Takes one reference to `ref` for a build and decides. Waits while another build's upload of the same
    /// blob is in flight, so no build adopts a body that is still being written.
    Claim claimForBuild(const BlobRef & ref);
    /// Ends the upload `claimForBuild` asked for. A failed upload drops that claim's reference, so a later
    /// claim (the build's retry or another build) uploads again instead of adopting a body that never landed.
    void uploadFinished(const BlobRef & ref, bool succeeded);

    /// GC side, called AFTER the caller read the blob's ETag: whether nothing references `ref` and no upload
    /// of it is in flight right now. See the class comment for why the order matters.
    bool unreferencedNow(const BlobRef & ref) const;

    struct Stats
    {
        size_t blobs = 0;        /// distinct referenced blobs
        size_t references = 0;
    };
    Stats stats() const;

private:
    struct Entry
    {
        UInt32 count = 0;
        bool uploading = false;
    };

    void releaseLocked(const BlobRef & ref);

    mutable std::mutex mutex;
    std::condition_variable upload_done;
    std::unordered_map<BlobRef, Entry, BlobRefHash> blobs;
};

}
