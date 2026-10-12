#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobRefCount.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

namespace DB::Cas
{

/// Configuration supplied when opening a content-addressed pool.
struct PoolConfig
{
    String pool_prefix;
    UInt128 server_id{};                      /// provenance in blob envelopes (ServerUUID)
    uint64_t blob_header_len = 256;           /// creation-time only; the pool is authoritative on reopen
    /// The NODE-LOCAL algo this server writes new content with (`Pool::writeAlgo`). `PoolMeta::createOrValidate`
    /// accepts it with no write when it is already in the pool's `algos_used`; otherwise it is admitted only
    /// with `blob_hash_allow_new`, or refused (BAD_ARGUMENTS).
    BlobHashAlgo blob_hash_algo = BlobHashAlgo::CityHash128;
    bool blob_hash_allow_new = false;
    /// Whether this open may create a brand-new pool's `_pool_meta`: only the server that writes blobs.
    bool mint_pool_meta = false;
    /// The CAS retry controller's budget (cas-s3-timeout-retry-control).
    CasRequestBudget cas_request_budget{};
    /// The requests' clock and inter-attempt sleep (CLOCK_BOOTTIME ms and a real sleep woken by
    /// `beginTeardown` when empty); injected by tests.
    std::function<uint64_t()> boot_ms_fn = {};
    std::function<void(uint64_t)> retry_sleep_fn = {};
};

class Pool;
using PoolPtr = std::shared_ptr<Pool>;

/// A blob entry's ranged read: the payload starts after the pool's fixed envelope header.
struct BlobLocation
{
    String key;
    uint64_t offset = 0;
    uint64_t length = 0;
};

/// The pool as this server sees it after TEC-851 step 3c: its identity object `_pool_meta` (read, or minted
/// once on a brand-new pool by the server that writes blobs), the blob store `blobs/`, and the in-memory
/// `BlobRefCount`. Nothing else of the pool is read or written: a part's metadata is its Keeper record.
///
/// Every request goes through ONE plane (`openRequests`) whose fence is the pool's teardown flag: once
/// `beginTeardown` ran, a GC round or an upload in flight is refused at its next request (or its next retry
/// sleep) instead of running to completion under a disk that is being torn down.
class Pool
{
public:
    /// Validates (or, with `mint_pool_meta`, creates) `_pool_meta`. Throws when it is absent and may not be
    /// minted, or does not admit `blob_hash_algo`.
    static PoolPtr open(BackendPtr backend, PoolConfig config);

    /// Idempotent; refuses every request admitted afterwards and wakes the plane's retry sleeps.
    void beginTeardown() noexcept;
    bool teardownBegun() const noexcept;

    const PoolConfig & poolConfig() const { return config; }
    const PoolMeta & poolMeta() const { return meta; }
    const Layout & layout() const { return pool_layout; }
    CasRequests & openRequests() const { return requests; }
    const std::shared_ptr<BlobRefCount> & blobRefs() const { return blob_refs; }
    BlobHashAlgo writeAlgo() const { return config.blob_hash_algo; }

    /// Where a Blob entry's payload is read from. `BAD_ARGUMENTS` for an Inline entry.
    BlobLocation locate(const ManifestEntry & entry) const;

private:
    Pool(BackendPtr backend_, PoolConfig config_, PoolMeta meta_);

    BackendPtr backend;
    PoolConfig config;
    PoolMeta meta;
    Layout pool_layout;

    std::atomic<bool> stopping{false};
    std::mutex stopping_mutex;
    std::condition_variable stopping_cv;

    /// `mutable`: issuing a request is not a change to the pool.
    mutable CasRequests requests;
    const std::shared_ptr<BlobRefCount> blob_refs = std::make_shared<BlobRefCount>();
};

}
