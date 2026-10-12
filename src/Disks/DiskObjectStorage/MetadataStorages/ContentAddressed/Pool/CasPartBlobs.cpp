#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartBlobs.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <Poco/Exception.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/config_version.h>
#include <Common/thread_local_rng.h>
#include <Common/threadPoolCallbackRunner.h>
#include <base/scope_guard.h>

#include <algorithm>
#include <chrono>
#include <map>

namespace ProfileEvents
{
    extern const Event CASBlobBodyPutAvoided;
    extern const Event CASBlobUploadFanoutBatches;
    extern const Event CASBlobUploadFanoutTasks;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int POCO_EXCEPTION;
    extern const int S3_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int TIMEOUT_EXCEEDED;
}
}

namespace DB::Cas
{

BlobSource BlobSource::fromString(String bytes)
{
    BlobSource source;
    source.size = bytes.size();
    source.open = [b = std::move(bytes)]() -> std::unique_ptr<ReadBuffer> { return std::make_unique<ReadBufferFromOwnString>(b); };
    return source;
}

BlobRef poolContentHash(BlobHashAlgo algo, std::string_view payload)
{
    /// The streaming writer's convention (chunked for `CityHash128`): a one-shot hash would diverge for
    /// payloads larger than one hash block.
    return BlobRef{algo, codecFor(algo).fromHex(blobHashHexOneShot(algo, payload))};
}

bool isDeterministicBlobPublicationFailure(const std::exception & error)
{
    /// A refused write, EXCEPT the class a fresh credential fixes: the engine refreshes once before it hands
    /// the failure back, so the next physical attempt signs with what the refresh installed.
    if (isDefinitelyRefusedWrite(error) && !isRefreshableCredentialError(error))
        return true;

    if (const auto * db_error = dynamic_cast<const Exception *>(&error))
    {
        const int code = db_error->code();
        return code != ErrorCodes::NETWORK_ERROR
            && code != ErrorCodes::S3_ERROR
            && code != ErrorCodes::POCO_EXCEPTION
            && code != ErrorCodes::SOCKET_TIMEOUT
            && code != ErrorCodes::CANNOT_READ_FROM_SOCKET
            && code != ErrorCodes::TIMEOUT_EXCEEDED;
    }

    /// Native Poco transport exceptions are outcome-ambiguous; other standard exceptions (allocation, source
    /// callbacks) are deterministic local failures.
    return dynamic_cast<const Poco::Exception *>(&error) == nullptr;
}

PartBlobs::PartBlobs(PoolPtr store_, String intended_ref_)
    : store(std::move(store_))
    , refs(store->blobRefs())
    , intended_ref(std::move(intended_ref_))
{
}

PartBlobs::~PartBlobs()
{
    refs->release(claimed);
}

std::optional<BlobRefCount::Claim> PartBlobs::claim(const BlobRef & ref)
{
    {
        std::lock_guard lock(claimed_mutex);
        if (std::find(claimed.begin(), claimed.end(), ref) != claimed.end())
            return std::nullopt;
        claimed.push_back(ref);
    }
    return refs->claimForBuild(ref);
}

void PartBlobs::uploadFinished(const BlobRef & ref, bool succeeded)
{
    if (!succeeded)
    {
        std::lock_guard lock(claimed_mutex);
        std::erase(claimed, ref);
    }
    refs->uploadFinished(ref, succeeded);
}

void PartBlobs::adopt(const BlobRef & ref)
{
    if (claim(ref) == BlobRefCount::Claim::Upload)
    {
        /// The source part no longer holds the blob (removed concurrently): there are no bytes to upload.
        uploadFinished(ref, /*succeeded=*/false);
        throwCasWriteRetryLater(fmt::format(
            "the blob {} of '{}' is no longer referenced on this server; retry the operation", blobIdOf(ref), intended_ref));
    }
    ProfileEvents::increment(ProfileEvents::CASBlobBodyPutAvoided);
}

void PartBlobs::uploadOne(const BlobUploadRequest & req)
{
    if (claim(req.ref) != BlobRefCount::Claim::Upload)
    {
        ProfileEvents::increment(ProfileEvents::CASBlobBodyPutAvoided);
        return;
    }

    bool succeeded = false;
    SCOPE_EXIT({ uploadFinished(req.ref, succeeded); });

    CasOperation op = store->openRequests().admit();
    const Retry policy = op.freeze(Retry::standard());
    const String key = store->layout().blobKey(req.ref);
    const PoolMeta & meta = store->poolMeta();
    const BlobSource & source = req.source;

    /// Every physical publication carries its own envelope (a fresh `incarnation_tag`), so a reissue never
    /// republishes an ETag GC may have read: the retry count is the one the former write path used.
    constexpr int max_publication_attempts = 8;
    for (int attempt = 0; attempt < max_publication_attempts; ++attempt)
    {
        if (attempt > 0)
            op.pause(Retry::backoff(static_cast<uint32_t>(attempt)));
        if (!source.open)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PartBlobs: streaming source for {} is not re-readable", key);
        EnvelopeHeader header;
        header.kind = ObjectKind::Blob;
        header.incarnation_tag = (static_cast<UInt128>(thread_local_rng()) << 64) | thread_local_rng();
        header.provenance = Provenance{
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count()),
            store->poolConfig().server_id, VERSION_INTEGER, ProvenanceOp::Insert};
        header.intended_ref = intended_ref;
        StreamingBlobPublication publication{
            source.size, encodeEnvelopeHeader(header, static_cast<uint32_t>(meta.blob_header_len)), source.open};
        try
        {
            op.publish(BlobPublishRequest{key, std::move(publication)}, policy.asSingleAttempt());
            succeeded = true;
            return;
        }
        catch (const std::exception & error)
        {
            if (isDeterministicBlobPublicationFailure(error) || attempt + 1 == max_publication_attempts)
                throw;
        }
    }
}

void PartBlobs::upload(std::span<const BlobUploadRequest> reqs)
{
    std::map<BlobRef, const BlobUploadRequest *> grouped;
    for (const auto & req : reqs)
    {
        if (req.declared_size != req.source.size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "PartBlobs: request for {} declares size {} but its source is sized {}",
                blobIdOf(req.ref), req.declared_size, req.source.size);
        grouped.try_emplace(req.ref, &req);
    }
    if (grouped.empty())
        return;
    ProfileEvents::increment(ProfileEvents::CASBlobUploadFanoutBatches);

    using RunnerTask = ThreadPoolCallbackRunnerLocal<void>::Task;
    ThreadPoolCallbackRunnerLocal<void> runner(blobUploadPool(), ThreadName::UNKNOWN);
    std::vector<std::shared_ptr<RunnerTask>> handles;
    handles.reserve(grouped.size());
    SCOPE_EXIT_SAFE({ ThreadPoolCallbackRunnerLocal<void>::waitForAllToFinish(handles); });
    for (const auto & [ref, req] : grouped)
    {
        handles.emplace_back(runner.enqueueAndGiveOwnership([this, req] { uploadOne(*req); }));
        ProfileEvents::increment(ProfileEvents::CASBlobUploadFanoutTasks);
    }
    ThreadPoolCallbackRunnerLocal<void>::waitForAllToFinishAndRethrowFirstError(handles);
}

}
