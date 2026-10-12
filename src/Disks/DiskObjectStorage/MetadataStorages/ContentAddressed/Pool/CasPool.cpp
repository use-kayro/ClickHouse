#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>
#include <Common/Exception.h>

#include <chrono>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::Cas
{

PoolPtr Pool::open(BackendPtr backend, PoolConfig config)
{
    /// Every pool request flows through the per-key-class ProfileEvents chokepoint.
    backend = std::make_shared<InstrumentedBackend>(std::move(backend));
    CasRequests bootstrap(backend, Fence::open(), config.boot_ms_fn);
    CasOperation op = bootstrap.admit();
    PoolMeta meta = PoolMeta::createOrValidate(op, Layout(config.pool_prefix), config.blob_header_len, config.blob_hash_algo,
        config.blob_hash_allow_new, config.mint_pool_meta);
    return PoolPtr(new Pool(std::move(backend), std::move(config), std::move(meta)));
}

Pool::Pool(BackendPtr backend_, PoolConfig config_, PoolMeta meta_)
    : backend(std::move(backend_))
    , config(std::move(config_))
    , meta(std::move(meta_))
    , pool_layout(config.pool_prefix)
    , requests(backend, Fence{
          [] { return uint64_t{0}; },
          [this](uint64_t, uint64_t) { return teardownBegun() ? Fence::Admit::LostOrRearmed : Fence::Admit::Ok; },
          [](uint64_t) {}},
          config.boot_ms_fn,
          config.retry_sleep_fn ? config.retry_sleep_fn : std::function<void(uint64_t)>([this](uint64_t ms)
          {
              std::unique_lock lock(stopping_mutex);
              stopping_cv.wait_for(lock, std::chrono::milliseconds(ms), [this] { return teardownBegun(); });
          }))
{
}

void Pool::beginTeardown() noexcept
{
    {
        std::lock_guard lock(stopping_mutex);
        stopping.store(true, std::memory_order_release);
    }
    stopping_cv.notify_all();
}

bool Pool::teardownBegun() const noexcept
{
    return stopping.load(std::memory_order_acquire);
}

BlobLocation Pool::locate(const ManifestEntry & entry) const
{
    if (entry.placement != EntryPlacement::Blob)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "entry '{}' is inline: it has no blob location", entry.path);
    return BlobLocation{.key = pool_layout.blobKey(entry.ref), .offset = meta.blob_header_len, .length = entry.blob_size};
}

}
