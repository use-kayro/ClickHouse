#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobRefCount.h>

namespace DB::Cas
{

void BlobRefCount::releaseLocked(const BlobRef & ref)
{
    auto it = blobs.find(ref);
    if (it == blobs.end())
        return;
    if (--it->second.count == 0 && !it->second.uploading)
        blobs.erase(it);
}

void BlobRefCount::add(const std::vector<BlobRef> & refs)
{
    std::lock_guard lock(mutex);
    for (const auto & ref : refs)
        ++blobs[ref].count;
}

void BlobRefCount::release(const std::vector<BlobRef> & refs)
{
    std::lock_guard lock(mutex);
    for (const auto & ref : refs)
        releaseLocked(ref);
}

BlobRefCount::Claim BlobRefCount::claimForBuild(const BlobRef & ref)
{
    std::unique_lock lock(mutex);
    upload_done.wait(lock, [&] { auto it = blobs.find(ref); return it == blobs.end() || !it->second.uploading; });
    auto & entry = blobs[ref];
    if (entry.count++ > 0)
        return Claim::Present;
    entry.uploading = true;
    return Claim::Upload;
}

void BlobRefCount::uploadFinished(const BlobRef & ref, bool succeeded)
{
    {
        std::lock_guard lock(mutex);
        auto it = blobs.find(ref);
        if (it != blobs.end())
        {
            it->second.uploading = false;
            if (!succeeded)
                --it->second.count;
            if (it->second.count == 0)
                blobs.erase(it);
        }
    }
    upload_done.notify_all();
}

bool BlobRefCount::unreferencedNow(const BlobRef & ref) const
{
    std::lock_guard lock(mutex);
    return !blobs.contains(ref);
}

BlobRefCount::Stats BlobRefCount::stats() const
{
    std::lock_guard lock(mutex);
    Stats stats{.blobs = blobs.size(), .references = 0};
    for (const auto & [_, entry] : blobs)
        stats.references += entry.count;
    return stats;
}

}
