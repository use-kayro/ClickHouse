#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequests.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Gc/CasGc.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasBlobUploadPool.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

#include <base/hex.h>
#include <city.h>

#include <gtest/gtest.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
/// For `ChunkFaultBackend`'s `DefiniteFailure` mode, which needs a real S3-classified error, and for
/// the ambiguity it raises otherwise.
#include <IO/S3Common.h>
#include <Poco/Exception.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

/// Per-TU extern declarations for the `ContentAddressedSetting` entries this header's helpers use --
/// the established pattern for `BaseSettings`-derived classes in this codebase (see e.g.
/// `RegisterDiskCache.cpp`'s `namespace FileCacheSetting` block): the entries are DEFINED once in
/// `ContentAddressedSettings.cpp`, and each consumer TU declares only the ones it references.
namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsString server_root_id;
    extern const ContentAddressedSettingsString scratch_path;
}

/// Same per-TU pattern for the error codes this header's fault backends raise (`ChunkFaultBackend`'s
/// non-S3 build of the `Definite` mode).
namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace DB::Cas::tests
{

/// A `CasRequests` over an always-open fence, for a fixture that has a backend but no mounted pool.
/// Every operation admitted from it holds a reference to it, so it must be named and outlive them.
inline DB::Cas::CasRequests openRequestsForTest(DB::Cas::BackendPtr backend)
{
    return DB::Cas::CasRequests(std::move(backend), DB::Cas::Fence::open());
}

/// The same, for a fixture holding only a reference. The aliasing `shared_ptr` owns nothing, so the
/// caller keeps the backend alive for as long as the returned object and its operations live.
inline DB::Cas::CasRequests openRequestsForTest(DB::Cas::Backend & backend)
{
    return openRequestsForTest(DB::Cas::BackendPtr(std::shared_ptr<void>(), &backend));
}

/// An open-fence operation together with the `CasRequests` it refers to, for a fixture that holds a
/// backend and needs to call a production entry point taking a `CasOperation &`. Neither copyable nor
/// movable: the operation points at the member beside it.
class OperationForTest
{
public:
    explicit OperationForTest(DB::Cas::BackendPtr backend)
        : requests(std::move(backend), DB::Cas::Fence::open()), operation(requests.admit())
    {
    }

    /// For a fixture holding only a reference: the aliasing `shared_ptr` owns nothing, so the caller
    /// keeps the backend alive for as long as this object.
    explicit OperationForTest(DB::Cas::Backend & backend)
        : OperationForTest(DB::Cas::BackendPtr(std::shared_ptr<void>(), &backend))
    {
    }

    OperationForTest(const OperationForTest &) = delete;
    OperationForTest & operator=(const OperationForTest &) = delete;

    DB::Cas::CasOperation & operator*() { return operation; }

private:
    DB::Cas::CasRequests requests;
    DB::Cas::CasOperation operation;
};

/// Bring up the server-wide blob upload pool (stage-1 §1) if it is not already up, so any test that
/// drives a `ContentAddressedTransaction` commit -- whose `uploadPendingBlobs` fans out on this pool --
/// finds it initialized. ROBUST (init-if-not-initialized, NOT `call_once`): the raw-lifecycle suite in
/// `gtest_cas_blob_upload_pool.cpp` deliberately shuts the pool down, so a `call_once` helper would fail
/// to bring it back for a later test. A global test-event listener (`gtest_cas_blob_upload_pool_env.cpp`)
/// calls this before every test, which is what makes it robust to test ordering.
inline void ensureBlobUploadPoolForTest(size_t size = 8)
{
    if (!DB::Cas::blobUploadPoolInitializedForTest())
        DB::Cas::initializeBlobUploadPool(size);
}


/// Minimal `ContentAddressedSettings` for a direct-construction gtest fixture: sets only
/// `server_root_id` and `scratch_path` (the two values every positional-ctor call site used to pass
/// explicitly) and validates, so the cached `blobHashAlgo` is populated from its (default) string
/// setting exactly as the disk-factory path would populate it. Callers that need a non-default setting apply
/// the override via `settings[ContentAddressedSetting::x] = value;` and re-run `settings.validate()`
/// themselves before constructing.
inline DB::ContentAddressedSettings makeSettingsForTest(const std::string & server_root_id, const std::filesystem::path & scratch_path)
{
    DB::ContentAddressedSettings settings;
    settings[DB::ContentAddressedSetting::server_root_id] = server_root_id;
    settings[DB::ContentAddressedSetting::scratch_path] = scratch_path.string();
    settings.validate();
    return settings;
}

/// A clock that only ever moves when something sleeps on it, plus the record of every sleep it
/// served. Injected into `CasRequests` so a policy's whole 90-second deadline is exercised in a test
/// that takes no wall-clock time, and so the schedule itself -- how many pauses, how long -- becomes
/// an assertion rather than a wait. `now` is atomic and `sleeps` is mutex-guarded because a
/// decommission session's background mount-lease renewer reads this same clock (via
/// `PoolConfig::boot_ms_fn`) from its own thread while the caller's thread drives it forward through
/// `sleepFn` -- relaxed ordering is enough since nothing here needs a happens-before relationship
/// beyond the value eventually becoming visible; direct field reads from a single thread after the
/// clock stops moving (the common case in this file's other users) are unaffected.
struct FakeClock
{
    std::atomic<uint64_t> now{1'000'000};
    std::vector<uint64_t> sleeps;

    std::function<uint64_t()> nowFn() { return [this] { return now.load(std::memory_order_relaxed); }; }
    std::function<void(uint64_t)> sleepFn()
    {
        return [this](uint64_t ms)
        {
            {
                std::lock_guard lock(sleeps_mutex);
                sleeps.push_back(ms);
            }
            now.fetch_add(ms, std::memory_order_relaxed);
        };
    }

private:
    std::mutex sleeps_mutex;
};

/// Run `fn`, expect a DB::Exception with EXACTLY `expected_code` (CORRUPTED_DATA-vs-NOT_IMPLEMENTED
/// is part of the fail-closed contract: an unknown future format must be NOT_IMPLEMENTED, never
/// misreported as corruption).
template <typename F>
void expectThrowsCode(int expected_code, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
    }
}

/// Asserts the object is present before comparing its body: an absent key would otherwise dereference
/// an empty optional and take the whole binary down instead of failing this one case.
inline void expectBytes(DB::Cas::Backend & backend, const String & key, const String & expected)
{
    OperationForTest op(backend);
    const auto got = (*op).read(key, DB::Cas::Retry::standard());
    ASSERT_TRUE(got.has_value()) << "object '" << key << "' is absent";
    EXPECT_EQ(got->bytes, expected);
}

inline void expectBytes(const DB::Cas::BackendPtr & backend, const String & key, const String & expected)
{
    expectBytes(*backend, key, expected);
}

/// Build a `LocalObjectStorage` rooted at a fresh, unique temporary directory (one per call).
///
/// Used by the unit tests that exercise the `Cas::Backend` seam against a real on-disk object storage
/// (the `EmulatedSingleProcess` adapter mode and the capability probe). For `LocalObjectStorage` the
/// object key IS the local path verbatim, so the unique root keeps every test instance isolated even
/// under the parallel gtest runner.
inline DB::ObjectStoragePtr makeLocalObjectStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_unit_" + unique)).string();

    /// A silently missing root would surface later as a bewildering storage-layer error (e.g. a
    /// failed bootstrap LIST), so a setup failure must be loud and named.
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    if (ec)
        throw std::runtime_error("cannot clear test object storage root " + root + ": " + ec.message());
    std::filesystem::create_directories(root, ec);
    if (ec)
        throw std::runtime_error("cannot create test object storage root " + root + ": " + ec.message());

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<DB::LocalObjectStorage>(std::move(settings));
}

/// Anchor a key under an object storage's own root, for the `Mode::Native` tests.
///
/// `Mode::Native` uses a key VERBATIM as the physical `LocalObjectStorage` path — no root-prefix
/// mapping the way `EmulatedSingleProcess`'s `emuPath` does (`LocalObjectStorage::writeObject`/
/// `readObject` pass `object.remote_path` straight through). A bare relative key like `"some/key"`
/// would therefore resolve relative to the TEST PROCESS's working directory rather than the
/// backend's own unique temp root, leaking a real file on disk that outlives the run and that a
/// later run then observes as pre-existing state. Worse for an assertion of ABSENCE: it answers
/// "absent" for a reason that has nothing to do with the property under test.
inline String nativeKeyUnder(const DB::ObjectStoragePtr & storage, const String & suffix)
{
    String root = storage->getCommonKeyPrefix();
    while (!root.empty() && root.back() == '/')
        root.pop_back();
    return root + "/" + suffix;
}

/// CityHash128 of bytes, composed into the canonical lowercase-hex id.
inline String hexOf(const String & bytes)
{
    return getHexUIntLowercase(CityHash_v1_0_2::CityHash128(bytes.data(), bytes.size()));
}

/// The content id of `bytes` as a UInt128 — definitionally consistent with `idOf` (parses the same hex).
inline DB::UInt128 u128Of(const String & bytes)
{
    return DB::Cas::hexToU128(hexOf(bytes));
}

/// The content id of `bytes` as a `BlobRef` (CityHash128 — every test pool's default write algo).
inline DB::Cas::BlobRef idOf(const String & bytes)
{
    return DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(bytes))};
}

/// Write a Blob object: a fixed-length (blob_header_len) envelope followed by the raw payload, keyed
/// by content. Mirrors what PartWriteTxn::putBlob will emit (Task 11).
inline DB::Cas::BlobRef writeBlobRaw(
    DB::Cas::Backend & backend, const DB::Cas::Layout & layout, const String & payload,
    uint64_t blob_header_len, [[maybe_unused]] const DB::UInt128 & domain_id)
{
    const DB::Cas::BlobRef id = idOf(payload);

    /// v3 envelope: domain_id/hash_algo dropped (identity is the content key); the `domain_id` param
    /// is kept for call-site compatibility but no longer stamped.
    DB::Cas::EnvelopeHeader header;
    header.kind = DB::Cas::ObjectKind::Blob;
    header.incarnation_tag = DB::UInt128(0x1234);
    header.build_id = DB::UInt128(0x5678);

    const String head = DB::Cas::encodeEnvelopeHeader(header, static_cast<uint32_t>(blob_header_len));
    OperationForTest op(backend);
    (*op).create(layout.blobKey(id), head + payload, DB::Cas::Retry::standard());
    return id;
}

/// A blob ManifestEntry referencing `hash` at `path` (size 1, the GC fold counts edges, not bytes).
inline DB::Cas::ManifestEntry blobEntryFor(const String & path, const DB::UInt128 & hash, uint64_t size = 1)
{
    DB::Cas::ManifestEntry e;
    e.path = path;
    e.placement = DB::Cas::EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hash)};
    e.blob_size = size;
    return e;
}

/// ---- GC-core (Phase 1d) test helpers over the part-manifest model ----

/// Open a Pool over `backend`.
/// Templated on the backend's own pointer type (an `InMemoryBackend`, one of its many test subclasses,
/// or a decorator that is not itself an `InMemoryBackend`, e.g. `ThrottlingBackend`) rather than fixed
/// to `InMemoryBackend`/`BackendPtr`: a fixed pair of non-template overloads is genuinely AMBIGUOUS for
/// a `shared_ptr<Derived>` argument, since "derived-to-`InMemoryBackend`" and "derived-to-`Backend`" are
/// equally-ranked conversions with no tiebreaker; template argument deduction has none of that problem.
template <typename BackendT>
DB::Cas::PoolPtr openPoolForTest(std::shared_ptr<BackendT> backend)
{
    return DB::Cas::Pool::open(std::move(backend),
        DB::Cas::PoolConfig{.pool_prefix = "p", .writer = true});
}

/// Serves an inner stream in windows of at most `chunk` bytes, and records the largest window it ever
/// handed out. `InMemoryBackend` materializes the whole object behind its stream, so without this a
/// consumer receives every byte as one contiguous window -- it can hold the object entire and still
/// look like a streaming reader, and nothing at the seam can tell the two apart.
///
/// What arming this proves is what the consumer then DOES: a reader that assumed one contiguous window
/// fails against a chunked source, so the test's own success is the evidence. The recorded window is
/// the bound it succeeded under, not a measurement of the reader's resident memory -- a consumer that
/// copies every window into a buffer of its own is invisible here, as it is to any `ReadBuffer`.
class ChunkedStreamForTest : public DB::ReadBuffer
{
public:
    ChunkedStreamForTest(std::unique_ptr<DB::ReadBuffer> inner_, size_t chunk,
                         std::shared_ptr<std::atomic<uint64_t>> largest_)
        : DB::ReadBuffer(nullptr, 0), inner(std::move(inner_)), storage(chunk), largest(std::move(largest_))
    {
    }

private:
    bool nextImpl() override
    {
        const size_t got = inner->read(storage.data(), storage.size());
        if (got == 0)
            return false;
        BufferBase::set(storage.data(), got, 0);
        uint64_t seen = largest->load();
        while (seen < got && !largest->compare_exchange_weak(seen, got))
        {
        }
        return true;
    }

    std::unique_ptr<DB::ReadBuffer> inner;
    std::vector<char> storage;
    std::shared_ptr<std::atomic<uint64_t>> largest;
};

/// Counts every request per key, for the op-count assertions (Pillar B / A1 tests).
class CountingBackend : public DB::Cas::InMemoryBackend
{
public:
    /// ---- The counters live on the transport primitives, so a request is counted once ----
    ///
    /// Whichever surface a caller used, its request passes through one of the primitives below: a
    /// legacy verb reaches them through its forwarder, and a `CasOperation` speaks them directly.
    /// Counting here is therefore counting requests rather than callers.
    ///
    /// Each counter ticks BEFORE the request is served, so an injected failure still counts as a
    /// request issued. A blob publication is not counted: it reaches the store through `publish`,
    /// which no counter below observes.
    std::optional<DB::Cas::Backend::Raw> read(const String & key, DB::Cas::TransportAccess & access) override
    {
        tick(get_counts, get_total, key);
        return InMemoryBackend::read(key, access);
    }

    std::optional<DB::Cas::Backend::RawMeta> head(const String & key, DB::Cas::TransportAccess & access) override
    {
        tick(head_counts, head_total, key);
        return InMemoryBackend::head(key, access);
    }

    DB::Cas::Backend::RawListPage list(const String & prefix, const String & cursor, size_t limit,
                                       DB::Cas::TransportAccess & access) override
    {
        tick(list_counts, list_total, prefix);
        return InMemoryBackend::list(prefix, cursor, limit, access);
    }

    /// Create-shaped and replace-shaped writes are counted apart as well as together: whether a write
    /// carried a precondition is the only thing about it the transport can still see, and the
    /// namespace-file request-profile goldens read the create path off exactly that.
    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        {
            std::lock_guard lock(count_mutex);
            ++write_counts[key];
            ++write_total;
            if (expected_value)
            {
                ++put_overwrite_counts[key];
                ++put_overwrite_total;
            }
            else
            {
                ++put_counts[key];
                ++put_total;
            }
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }

    /// Every ATTEMPTED delete is counted, whatever the backend answers. The destructive gate's tests
    /// assert that a suppressed round issues NONE, and an attempt that came back `Gone` is still an
    /// attempt -- counting only successful ones would let a gate that leaks deletes over already-absent
    /// keys read as green.
    DB::Cas::Backend::RawRemoval remove(const String & key, const String & expected_value,
                                        DB::Cas::TransportAccess & access) override
    {
        tick(delete_counts, delete_total, key);
        return InMemoryBackend::remove(key, expected_value, access);
    }

    /// A blob publication reaches the store through `publish`, not `write` -- a "zero backend requests"
    /// assertion built only from the primitives above would miss one landing.
    void publish(const DB::Cas::BlobPublishRequest & request, DB::Cas::TransportAccess & access) override
    {
        tick(publish_counts, publish_total, request.destination_key);
        InMemoryBackend::publish(request, access);
    }

    std::unique_ptr<DB::ReadBuffer> stream(const String & key, DB::Cas::TransportAccess & access) override
    {
        tick(get_stream_counts, get_stream_total, key);
        std::unique_ptr<DB::ReadBuffer> opened = InMemoryBackend::stream(key, access);
        const size_t chunk = stream_chunk.load();
        if (!opened || chunk == 0)
            return opened;
        auto chunked = std::make_unique<ChunkedStreamForTest>(std::move(opened), chunk, largestChunkSlot(key));
        /// Hand back a buffer whose FIRST window is already loaded, as a network-backed store does:
        /// `ObjectStorageBackend::stream` forces that GET so the open's own attempt is what pays for
        /// it. A fixture that returned an empty buffer would let a consumer which drops the preloaded
        /// window -- and so silently loses the head of every streamed body -- pass its tests.
        chunked->nextIfAtEnd();
        return chunked;
    }

    /// Serve every stream opened from now on in windows of at most `bytes`, as a network-backed store
    /// does. Zero (the default) hands the consumer the whole object at once, which is what this
    /// backend's own materialization makes of any stream. A mode rather than a count: `resetCounts`
    /// leaves it alone.
    void setStreamChunkForTest(size_t bytes) { stream_chunk.store(bytes); }

    /// The largest contiguous window any consumer of `key`'s stream was handed. Zero when the key was
    /// never streamed.
    uint64_t largestStreamChunk(const String & key) const
    {
        std::lock_guard lock(count_mutex);
        const auto it = largest_stream_chunk.find(key);
        return it == largest_stream_chunk.end() ? 0 : it->second->load();
    }

    uint64_t getCount(const String & key) const { return lookup(get_counts, key); }
    uint64_t headCount(const String & key) const { return lookup(head_counts, key); }
    uint64_t listCount(const String & prefix) const { return lookup(list_counts, prefix); }
    uint64_t writeCount(const String & key) const { return lookup(write_counts, key); }
    uint64_t putCount(const String & key) const { return lookup(put_counts, key); }
    uint64_t putOverwriteCount(const String & key) const { return lookup(put_overwrite_counts, key); }
    uint64_t deleteCount(const String & key) const { return lookup(delete_counts, key); }
    uint64_t getStreamCount(const String & key) const { return lookup(get_stream_counts, key); }
    uint64_t publishCount(const String & key) const { return lookup(publish_counts, key); }

    uint64_t getTotal() const { std::lock_guard lock(count_mutex); return get_total; }
    uint64_t headTotal() const { std::lock_guard lock(count_mutex); return head_total; }
    uint64_t listTotal() const { std::lock_guard lock(count_mutex); return list_total; }
    uint64_t writeTotal() const { std::lock_guard lock(count_mutex); return write_total; }
    uint64_t putTotal() const { std::lock_guard lock(count_mutex); return put_total; }
    uint64_t putOverwriteTotal() const { std::lock_guard lock(count_mutex); return put_overwrite_total; }
    uint64_t deleteTotal() const { std::lock_guard lock(count_mutex); return delete_total; }
    uint64_t getStreamTotal() const { std::lock_guard lock(count_mutex); return get_stream_total; }
    uint64_t publishTotal() const { std::lock_guard lock(count_mutex); return publish_total; }

    /// Attempted deletes against any key whose path CONTAINS `substr` — the per-site assertion the
    /// destructive-gate tests make ("the generation prune deleted nothing", "the sweep deleted nothing").
    uint64_t deleteCountForKeysContaining(const String & substr) const
    {
        return sumForKeysContaining({&delete_counts}, substr);
    }

    /// The total number of read + stream + create-shaped write requests against any key whose path
    /// CONTAINS `substr` (T0 idle-round gate: zero run I/O touches every `.../blob_target/...` key).
    uint64_t ioCountForKeysContaining(const String & substr) const
    {
        return sumForKeysContaining({&get_counts, &get_stream_counts, &put_counts}, substr);
    }

    /// Every key this backend was ever asked to delete, in sorted order — so a failing zero-delete
    /// assertion names the sites that leaked instead of just reporting a count.
    std::vector<String> deletedKeys() const
    {
        std::lock_guard lock(count_mutex);
        std::vector<String> keys;
        keys.reserve(delete_counts.size());
        for (const auto & [key, n] : delete_counts)
            keys.push_back(key);
        return keys;
    }

    /// Every key any counted operation was issued against, plus every LIST prefix, sorted and
    /// de-duplicated. A request-profile gate asserts the SET, not only the totals, so a new request the
    /// profile does not allow names its own key in the failure instead of moving an anonymous counter.
    std::vector<String> touchedKeys() const
    {
        std::lock_guard lock(count_mutex);
        std::vector<String> keys;
        for (const std::map<String, uint64_t> * m :
             {&get_counts, &head_counts, &list_counts, &write_counts, &delete_counts, &get_stream_counts})
            for (const auto & [key, n] : *m)
                keys.push_back(key);
        std::sort(keys.begin(), keys.end());
        keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
        return keys;
    }

    void resetCounts()
    {
        std::lock_guard lock(count_mutex);
        get_counts.clear();
        head_counts.clear();
        list_counts.clear();
        write_counts.clear();
        put_counts.clear();
        put_overwrite_counts.clear();
        delete_counts.clear();
        get_stream_counts.clear();
        publish_counts.clear();
        largest_stream_chunk.clear();
        get_total = head_total = list_total = write_total = put_total = put_overwrite_total
            = delete_total = get_stream_total = publish_total = 0;
    }

private:
    std::shared_ptr<std::atomic<uint64_t>> largestChunkSlot(const String & key)
    {
        std::lock_guard lock(count_mutex);
        auto & slot = largest_stream_chunk[key];
        if (!slot)
            slot = std::make_shared<std::atomic<uint64_t>>(0);
        return slot;
    }

    void tick(std::map<String, uint64_t> & per_key, uint64_t & total, const String & key)
    {
        std::lock_guard lock(count_mutex);
        ++per_key[key];
        ++total;
    }

    uint64_t lookup(const std::map<String, uint64_t> & m, const String & key) const
    {
        std::lock_guard lock(count_mutex);
        const auto it = m.find(key);
        return it == m.end() ? 0 : it->second;
    }

    uint64_t sumForKeysContaining(std::initializer_list<const std::map<String, uint64_t> *> maps,
                                  const String & substr) const
    {
        std::lock_guard lock(count_mutex);
        uint64_t total = 0;
        for (const std::map<String, uint64_t> * m : maps)
            for (const auto & [key, n] : *m)
                if (key.find(substr) != String::npos)
                    total += n;
        return total;
    }

    mutable std::mutex count_mutex;
    /// Held by `shared_ptr` so a stream outliving the map entry's rehash still records into its own slot.
    std::map<String, std::shared_ptr<std::atomic<uint64_t>>> largest_stream_chunk;
    std::atomic<size_t> stream_chunk{0};
    std::map<String, uint64_t> get_counts;
    std::map<String, uint64_t> head_counts;
    std::map<String, uint64_t> list_counts;
    std::map<String, uint64_t> write_counts;
    std::map<String, uint64_t> put_counts;
    std::map<String, uint64_t> put_overwrite_counts;
    std::map<String, uint64_t> delete_counts;
    std::map<String, uint64_t> get_stream_counts;
    std::map<String, uint64_t> publish_counts;
    uint64_t get_total = 0;
    uint64_t head_total = 0;
    uint64_t list_total = 0;
    uint64_t write_total = 0;
    uint64_t put_total = 0;
    uint64_t put_overwrite_total = 0;
    uint64_t delete_total = 0;
    uint64_t get_stream_total = 0;
    uint64_t publish_total = 0;
};

/// Expect a DB::Exception with EXACTLY `expected_code` AND a message containing `expected_substring`.
/// Needed wherever several distinct branches share one code: the code alone does not identify which
/// one ran, so a test that silently takes the wrong branch would still pass.
template <typename F>
void expectThrowsCodeWithMessage(int expected_code, const String & expected_substring, F && fn)
{
    try
    {
        fn();
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), expected_code);
        EXPECT_NE(e.message().find(expected_substring), String::npos)
            << "wrong branch: " << e.message();
    }
}
}

/// The mount and GC suites call these two unqualified, under `using namespace DB::Cas;`. Argument-
/// dependent lookup does not reach `DB::Cas::tests` from a `shared_ptr<InMemoryBackend>`, so the names
/// are re-exported here rather than moved: the qualified `DB::Cas::tests::` spelling the rest of the
/// tree uses keeps working, and there is still one definition.
namespace DB::Cas
{
using tests::OperationForTest;
using tests::openRequestsForTest;
}
