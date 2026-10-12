#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobRefCount.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>

namespace DB { class ReadBuffer; }

namespace DB::Cas
{

/// Re-readable source for one blob upload. `open` returns a fresh reader over exactly `size` logical bytes
/// and may be called more than once. `server_side_copy_from` names an already-verified complete staged
/// object that may be copied verbatim on this source's first publication only (every copy of the source
/// shares `publication_attempted`); later publications stream the payload under a new envelope.
struct BlobSource
{
    uint64_t size = 0;
    std::function<std::unique_ptr<ReadBuffer>()> open;   /// yields exactly `size` bytes, from the start
    std::optional<String> server_side_copy_from;
    std::shared_ptr<std::atomic<bool>> publication_attempted = std::make_shared<std::atomic<bool>>(false);

    /// Consumes the source's first-publication privilege (the verbatim staged copy).
    bool beginPublication() const { return !publication_attempted->exchange(true, std::memory_order_acq_rel); }

    /// A re-readable source backed by an owned string; for small payloads and tests.
    static BlobSource fromString(String bytes);
};

/// One blob a part under construction uploads. `declared_size` mirrors `source.size`.
struct BlobUploadRequest
{
    BlobRef ref;
    BlobSource source;
    uint64_t declared_size = 0;
};

/// Hashes `payload` with `algo` by the same convention as the streaming blob writer and returns the complete
/// `BlobRef` (the algorithm travels with the digest).
BlobRef poolContentHash(BlobHashAlgo algo, std::string_view payload);

/// Whether a failed blob publication is final (a refused write, a local failure) rather than an ambiguity a
/// fresh-envelope reissue may resolve.
bool isDeterministicBlobPublicationFailure(const std::exception & error);

/// The blobs of ONE part under construction. It is the whole write-side protocol of the content-addressed
/// disk: no manifest, ref, marker or lease object is ever written -- the part's metadata becomes its Keeper
/// record (`CasKeeperPartRecord.h`) when ReplicatedMergeTree commits it.
///
/// Every blob the part will reference is claimed in the pool's `BlobRefCount` (one reference, kept in
/// `claimed`) before anything is decided, and stays claimed until the build is destroyed -- by then the
/// committed part holds its own reference (`HeldParts`). A claim that finds the blob referenced adopts
/// it with no request at all; one that finds it unreferenced uploads it unconditionally under a fresh
/// envelope (no `HEAD`, no freshness marker). See `BlobRefCount` for why this order makes GC's exact-ETag
/// delete safe.
///
/// `may_upload == false` (a server whose replicas cannot become merge leader: a reader) refuses every
/// upload with `NOT_IMPLEMENTED`; only the pool's writer writes blob bodies (TEC-851 decision of 11/10).
class PartBlobs
{
public:
    PartBlobs(PoolPtr store_, String intended_ref_, bool may_upload_);
    ~PartBlobs();

    PartBlobs(const PartBlobs &) = delete;
    PartBlobs & operator=(const PartBlobs &) = delete;

    /// Claims a blob that a part this disk holds already references (a hardlink-style carry-forward).
    void adopt(const BlobRef & ref);

    /// Claims and, where nothing referenced them, uploads `requests` -- one task per distinct blob on the
    /// server-wide upload pool; the first failure is rethrown after every task finished.
    void upload(std::span<const BlobUploadRequest> requests);

private:
    /// Takes this build's reference to `ref` once; `nullopt` when it already holds one.
    std::optional<BlobRefCount::Claim> claim(const BlobRef & ref);
    void uploadFinished(const BlobRef & ref, bool succeeded);
    void uploadOne(const BlobUploadRequest & req);

    PoolPtr store;
    std::shared_ptr<BlobRefCount> refs;
    String intended_ref;
    bool may_upload;
    std::mutex claimed_mutex;
    std::vector<BlobRef> claimed;
};

}
