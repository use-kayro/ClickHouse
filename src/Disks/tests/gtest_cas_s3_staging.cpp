#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasObjectStorageBackend.h>
#include <IO/WriteSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskCommitTransactionOptions.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Common/SipHash.h>

#include <Poco/AutoPtr.h>
#include <Poco/Exception.h>
#include <Poco/Util/XMLConfiguration.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <mutex>
#include <sstream>
#include <string>

#include "config.h"
#if USE_AWS_S3
#include <IO/S3Common.h>
#include <aws/s3/S3Errors.h>
#endif

/// `staging_backend` defaults to `local`; explicit `s3` selection requires native-copy capability
/// on writable mounts.

namespace DB::ContentAddressedSetting
{
    extern const ContentAddressedSettingsString staging_backend;
}

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

/// ---- Small raw-fixture request-engine wrappers shared by the tests below ----

/// Unconditional create of a fresh key (the fixture's own setup, never a real conflict).
void createAt(DB::Cas::Backend & backend, const String & key, const String & bytes)
{
    DB::Cas::tests::OperationForTest op(backend);
    EXPECT_TRUE(std::holds_alternative<DB::Cas::Committed>((*op).create(key, bytes, DB::Cas::Retry::once())));
}

/// Build a `Poco::Util::XMLConfiguration` with `inner_xml` nested under a `<disk>` element (mirrors
/// the shape a real CAS disk config has under `storage_configuration.disks.<name>`, so
/// `config_prefix = "disk"` reads exactly like the disk factory's `config_prefix`).
Poco::AutoPtr<Poco::Util::XMLConfiguration> configWithDiskSection(const std::string & inner_xml)
{
    std::istringstream xml_stream( // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        "<clickhouse><disk>" + inner_xml + "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(xml_stream);
}

/// A local test store whose copy-mode capability is configurable. Its ordinary `copyObject`
/// implementation remains the real local implementation; only the advertised transport capability
/// differs so mount selection can be tested independently of a live S3 service.
class FakeNativeCopyObjectStorage final : public DB::LocalObjectStorage
{
public:
    FakeNativeCopyObjectStorage(DB::LocalObjectStorageSettings settings_, bool native_only_copy_supported_)
        : DB::LocalObjectStorage(std::move(settings_))
        , native_only_copy_supported(native_only_copy_supported_)
    {
    }

    bool supportsCopyMode(DB::ObjectStorageCopyMode mode) const override
    {
        return mode == DB::ObjectStorageCopyMode::Default
            || (mode == DB::ObjectStorageCopyMode::NativeOnly && native_only_copy_supported);
    }

private:
    const bool native_only_copy_supported;
};

std::shared_ptr<FakeNativeCopyObjectStorage> makeFakeNativeCopyStorage(bool native_only_copy_supported)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_s3_staging_native_copy_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FakeNativeCopyObjectStorage>(std::move(settings), native_only_copy_supported);
}

/// A fake object-store sink for Task 4 of the S3-native staging plan (`DB::Cas::CaContentWriteBuffer`'s
/// S3-staging constructor): an in-memory `WriteBufferFromFileBase` that records every byte written to
/// it, plus whether `cancelImpl`/`finalizeImpl` ran. This is enough to prove the S3-staging mode
/// streams to the SINK (not to a local temp file) while hashing, without needing a real object storage
/// — the end-to-end wiring (`writeFile` choosing this mode, the promote path) lands in later tasks.
class FakeStagingSink : public DB::WriteBufferFromFileBase
{
public:
    explicit FakeStagingSink(std::string key_)
        : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0), key(std::move(key_))
    {
    }

    void sync() override {}
    std::string getFileName() const override { return key; }

    const std::string & writtenBytes() const { return written; }
    bool wasCancelled() const { return cancelled; }
    bool wasFinalizedForTest() const { return did_finalize; }

protected:
    void nextImpl() override
    {
        if (!offset())
            return;
        written.append(working_buffer.begin(), offset());
    }

    void finalizeImpl() override
    {
        next();
        did_finalize = true;
    }

    void cancelImpl() noexcept override
    {
        cancelled = true;
    }

private:
    std::string key;
    std::string written;
    bool cancelled = false;
    bool did_finalize = false;
};

}

TEST(CASS3Staging, ParsesS3BackendFromConfig)
{
    auto config = configWithDiskSection("<staging_backend>s3</staging_backend>");

    EXPECT_EQ(DB::ContentAddressedMetadataStorage::parseStagingBackend(*config, "disk"), DB::Cas::StagingBackend::S3);
}

TEST(CASS3Staging, DefaultConfigParsesToLocalBackend)
{
    /// No `staging_backend` key at all — the OFF BY DEFAULT arm.
    auto config = configWithDiskSection("<scratch_path>/tmp/whatever</scratch_path>");

    EXPECT_EQ(DB::ContentAddressedMetadataStorage::parseStagingBackend(*config, "disk"), DB::Cas::StagingBackend::Local);
}

TEST(CASS3Staging, UnknownBackendValueThrows)
{
    auto config = configWithDiskSection("<staging_backend>nfs</staging_backend>");
    EXPECT_THROW(DB::ContentAddressedMetadataStorage::parseStagingBackend(*config, "disk"), DB::Exception);
}

TEST(CASS3Staging, DefaultConstructedStorageReportsLocal)
{
    /// Constructed with no staging-related args at all (mirrors the existing gtest call sites, e.g.
    /// gtest_ca_wiring.cpp, which stop at `context_`): the accessors must reflect the same
    /// byte-for-byte-current-behavior defaults the config parser produces above.
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "test", std::filesystem::temp_directory_path() / "cas_s3_staging_default_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);

    EXPECT_EQ(storage->stagingBackend(), DB::Cas::StagingBackend::Local);
}

TEST(CASS3Staging, DefaultObjectStorageRejectsNativeOnlyCopyMode)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();

    EXPECT_TRUE(storage->supportsCopyMode(DB::ObjectStorageCopyMode::Default));
    EXPECT_FALSE(storage->supportsCopyMode(DB::ObjectStorageCopyMode::NativeOnly));
}

/// Task 4 of the S3-native staging plan: `CaContentWriteBuffer`'s S3-staging constructor streams
/// directly to an already-opened object-store sink while hashing, instead of spilling to a local temp
/// file (see the constructor's doc comment in ContentAddressedWriteBuffers.h). These two tests
/// exercise the buffer directly over a `FakeStagingSink` — no real object storage, disk, or
/// `ContentAddressedTransaction` needed; `writeFile` choosing this mode is exercised together with the
/// promote path in later tasks (S3 mode is off by default and not enabled by any existing test).

TEST(CASS3Staging, ContentWriteBufferS3ModeStreamsToSinkAndFinalizes)
{
    const std::string staging_key = "staging/mount1/abc123.tmp";
    auto * sink_ptr = new FakeStagingSink(staging_key);
    std::unique_ptr<DB::WriteBufferFromFileBase> sink(sink_ptr);

    std::string got_hash_hex;
    size_t got_size = 0;
    std::string got_key;
    int on_finalized_calls = 0;

    /// S3-native staging fix 2026-07-11: the S3 constructor takes a fixed-length envelope header that is
    /// written to the sink FIRST, UNHASHED and excluded from the reported size. A distinctive 256-byte
    /// filler stands in for the real CABL header here (this test exercises the buffer mechanics, not the
    /// envelope encoder).
    const std::string envelope_header(256, 'H');

    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        std::move(sink),
        staging_key,
        envelope_header,
        DB::Cas::BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string & hash_hex, size_t size, const std::string & key)
        {
            ++on_finalized_calls;
            got_hash_hex = hash_hex;
            got_size = size;
            got_key = key;
        });

    /// Write in two chunks (exercises more than one nextImpl flush) and finalize.
    const std::string payload_part1(4000, 'x');
    const std::string payload_part2(1234, 'y');
    buf->write(payload_part1.data(), payload_part1.size());
    buf->write(payload_part2.data(), payload_part2.size());
    buf->finalize();

    const std::string payload = payload_part1 + payload_part2;

    /// (a) the sink received the ENVELOPE HEADER FIRST, then EXACTLY the payload bytes — the staging
    /// object holds `[header][payload]` so the promote can stay a verbatim server-side copy.
    EXPECT_EQ(sink_ptr->writtenBytes(), envelope_header + payload);
    EXPECT_TRUE(sink_ptr->wasFinalizedForTest());
    EXPECT_FALSE(sink_ptr->wasCancelled());

    /// (b) on_finalized fired exactly once with the correct cityHash128 hex, size, and staging key.
    /// The pool-wide content hash is the STREAMING `HashingWriteBuffer` convention (chunked
    /// cityHash128, block = 2048 B), which diverges from a one-shot `CityHash_v1_0_2::CityHash128`
    /// call for a payload spanning more than one block (see `gtest_cas_part_write.cpp`'s
    /// `CopyForwardMultiBlockPayloadVerifies`, which documents and exercises the same divergence).
    /// This payload (5234 bytes) spans multiple 2048-byte blocks, so the expected hash must be
    /// recomputed with the SAME streaming convention via `HashingReadBuffer`, not a one-shot call.
    DB::ReadBufferFromMemory expected_in(payload.data(), payload.size());
    DB::HashingReadBuffer expected_hashing(expected_in);
    expected_hashing.ignoreAll();
    const std::string expected_hash_hex = getHexUIntLowercase(expected_hashing.getHash());
    EXPECT_EQ(on_finalized_calls, 1);
    EXPECT_EQ(got_hash_hex, expected_hash_hex);
    EXPECT_EQ(got_size, payload.size());
    EXPECT_EQ(got_key, staging_key);
    EXPECT_EQ(buf->getFileName(), staging_key);
}

TEST(CASS3Staging, ContentWriteBufferS3ModeCancelCancelsSinkAndSkipsFinalize)
{
    const std::string staging_key = "staging/mount1/cancelled.tmp";
    auto * sink_ptr = new FakeStagingSink(staging_key);
    std::unique_ptr<DB::WriteBufferFromFileBase> sink(sink_ptr);

    bool on_finalized_called = false;

    auto buf = std::make_unique<DB::Cas::CaContentWriteBuffer>(
        std::move(sink),
        staging_key,
        /*envelope_header=*/std::string(256, 'H'),
        DB::Cas::BlobHashAlgo::CityHash128,
        /*buf_size=*/8192,
        /*use_adaptive_buffer_size=*/false,
        /*adaptive_buffer_initial_size=*/0,
        [&](const std::string &, size_t, const std::string &)
        {
            on_finalized_called = true;
        });

    const std::string payload = "some bytes that must never be promoted";
    buf->write(payload.data(), payload.size());
    buf->cancel();

    /// (c) cancel() before finalize cancels the sink and on_finalized is NEVER called — no partial
    /// finalize (no promote-worthy hash/size is ever handed to the transaction for cancelled bytes).
    EXPECT_TRUE(sink_ptr->wasCancelled());
    EXPECT_FALSE(sink_ptr->wasFinalizedForTest());
    EXPECT_FALSE(on_finalized_called);

    /// The buffer's destructor calls cancel() again (defensive backstop) — already-cancelled, so this
    /// must stay a no-op: still no on_finalized call, and no attempt to fs::remove a remote key.
    buf.reset();
    EXPECT_FALSE(on_finalized_called);
}

/// The ordinary staged cases below pin the same mandatory-`HEAD` selection used by the ETag-faithful
/// regressions: native verbatim copy only after a first absent observation, no publication for a live
/// body, and retagged streaming for `Condemned`.

/// ===========================================================================================
/// Task 6 of the S3-native staging plan: staging cleanup after commit, read-your-writes over an S3
/// pending blob, and the mount-lease-scoped sweeper (`CASStagingSweeper.h`).
///
/// The wiring-level tests below drive the real metadata storage and transaction over a local test
/// store that advertises native copy. Its real `getType` stays `Local`, so the CAS core uses
/// `EmulatedSingleProcess`; these cases stop before a native-mode staged publication is required.

namespace
{

/// Construct a `ContentAddressedMetadataStorage` with `staging_backend=s3` over `object_storage`,
/// mirroring `DefaultConstructedStorageReportsLocal`'s settings defaults for every
/// field this test suite does not care about — only `server_root_id` (the mount identity that names
/// the staging prefix) and `staging_backend` differ.
std::shared_ptr<DB::ContentAddressedMetadataStorage> makeS3StagingMetadataStorageForTest(
    const DB::ObjectStoragePtr & object_storage, const std::string & server_root_id)
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto scratch = std::filesystem::temp_directory_path()
        / ("cas_s3_staging_wiring_" + server_root_id + "_" + unique);
    auto settings = DB::Cas::tests::makeSettingsForTest(server_root_id, scratch);
    settings[DB::ContentAddressedSetting::staging_backend] = "s3";
    settings.validate();
    return std::make_shared<DB::ContentAddressedMetadataStorage>(
        object_storage, "pool", "srv1", /*disk_name_=*/"", /*context_=*/getContext().context, settings);
}

/// Mirrors gtest_ca_wiring.cpp's helper of the same shape.
void writeThroughS3Transaction(DB::ContentAddressedTransaction & tx, const std::string & path, const std::string & bytes)
{
    auto buf = tx.writeFile(path, 65536, DB::WriteMode::Rewrite, {});
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
}

}

TEST(CASS3Staging, WritableS3StagingRequiresNativeOnlyCopy)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountNative");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    writeThroughS3Transaction(
        ca_tx,
        "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin",
        "native-staging");

    DB::RelativePathsWithMetadata staged;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged, /*max_keys=*/0);
    EXPECT_EQ(staged.size(), 1u);
}

TEST(CASS3Staging, UnsupportedNativeOnlyCopyDoesNotFallBackToLocal)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/false);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountUnsupported");

    DB::Cas::tests::expectThrowsCodeWithMessage(
        DB::ErrorCodes::NOT_IMPLEMENTED,
        "cas_staging_backend=s3",
        [&]
        {
            metadata_storage->startup();
        });
}

/// (a) A successful commit removes the S3 staging object of a pending blob it staged. Uses the B189
/// orphan shape (the pending blob's entry is unlinked before commit) so `publishStaging` never calls
/// `putBlob` for it — only `cleanupPendingTempFiles`'s Task 6 branch ever touches this staging object,
/// which is exactly the seam this test targets.
TEST(CASS3Staging, SuccessfulCommitRemovesOrphanedS3StagingObject)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountA");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);

    /// orphan.bin forces the S3-staging blob path (a ".bin" suffix always stays a blob, per
    /// `partFileMustStayBlob`); it is unlinked below before commit.
    writeThroughS3Transaction(ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin", std::string(300, 'x'));
    /// checksums.txt is small and NOT blob-forcing: an INLINE entry that gives the part's PartWriteTxn a real
    /// (non-orphaned) manifest entry, so `publishStaging` takes its normal path (not the early-return
    /// mutable-only/no-PartWriteTxn branch).
    writeThroughS3Transaction(ca_tx, "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/checksums.txt", "sums");

    DB::RelativePathsWithMetadata staged_before;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged_before, /*max_keys=*/0);
    ASSERT_EQ(staged_before.size(), 1u) << "exactly orphan.bin's S3 staging object should exist pre-commit";

    tx->unlinkFile("a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/orphan.bin", false, false);

    tx->commit(DB::NoCommitOptions{});

    DB::RelativePathsWithMetadata staged_after;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged_after, /*max_keys=*/0);
    EXPECT_TRUE(staged_after.empty())
        << "cleanupPendingTempFiles must remove the orphaned S3 staging object after a successful commit";
}

/// (b) Read-your-writes over an S3 pending blob (before commit) returns the staged bytes from the S3
/// staging object, not a local temp file.
TEST(CASS3Staging, ReadYourWritesReturnsStagedBytesFromS3StagingObject)
{
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountB");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);

    const std::string path = "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin";
    const std::string payload(5000, 'z');
    writeThroughS3Transaction(ca_tx, path, payload);

    auto read_buf = tx->tryReadFileInFlight(path, DB::ReadSettings{}, {});
    ASSERT_NE(read_buf, nullptr);
    std::string got;
    DB::readStringUntilEOF(got, *read_buf);
    EXPECT_EQ(got, payload);
}

/// (c) `sweepOwnMountStaging` removes only objects under the given mount prefix and leaves a DIFFERENT
/// mount's staging objects untouched (the lease-fence — `CASStagingSweeper.h`).
TEST(CASStagingSweeper, RemovesOnlyObjectsUnderGivenMountPrefix)
{
    auto storage = DB::Cas::tests::makeLocalObjectStorageForTest();
    const std::string root = storage->getCommonKeyPrefix();

    auto put = [&](const std::string & key, const std::string & bytes)
    {
        auto buf = storage->writeObject(DB::StoredObject(key), DB::WriteMode::Rewrite);
        buf->write(bytes.data(), bytes.size());
        buf->finalize();
    };

    put(root + "/p/staging/mountA/one.tmp", "a1");
    put(root + "/p/staging/mountA/two.tmp", "a2");
    put(root + "/p/staging/mountB/three.tmp", "b1");   /// a DIFFERENT mount's staging — must survive

    DB::Cas::sweepOwnMountStaging(*storage, root + "/p/staging/mountA/");

    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/mountA/one.tmp")));
    EXPECT_FALSE(storage->exists(DB::StoredObject(root + "/p/staging/mountA/two.tmp")));
    EXPECT_TRUE(storage->exists(DB::StoredObject(root + "/p/staging/mountB/three.tmp")));
}

/// (d) GC's blob-discovery LISTs ONLY `Layout::blobsPrefix()` (`<pool>/blobs/`) — a top-level prefix
/// strictly disjoint from the S3-staging area (`<pool>/staging/<mount_id>/`), so a staging object can
/// never be listed, HEAD'd, or condemned as an orphan blob (`CasFsck.cpp`).
/// This is a prefix-separation assertion (the GC fold itself is not unit-testable in isolation from a
/// full round — see `gtest_cas_gc_fold.cpp` for that machinery); it pins the invariant a refactor that
/// nested `staging/` under `blobs/` (or vice versa) would violate.
TEST(CASS3Staging, GcBlobDiscoveryPrefixExcludesStagingObjects)
{
    /// The REAL staging prefix, from the accessor every writer actually mints staging keys through
    /// (`ContentAddressedMetadataStorage::stagingKeyPrefix`) -- not a hand-copied literal that a
    /// staging-side rename would leave silently stale.
    auto object_storage = makeFakeNativeCopyStorage(/*native_only_copy_supported=*/true);
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountA");
    metadata_storage->startup();
    const std::string physical_root = object_storage->getCommonKeyPrefix();
    const std::string full_staging_prefix = metadata_storage->stagingKeyPrefix();
    ASSERT_TRUE(full_staging_prefix.starts_with(physical_root))
        << full_staging_prefix << " vs root " << physical_root;
    /// Strip the physical object-storage root (and the '/' `physicalKey` joins it to the pool key
    /// with): `Layout` (below) is root-agnostic, and comparing a physically-rooted key against a bare
    /// `Layout` key would pass for the wrong reason (both simply fail to share the unrelated root, not
    /// because the pool-relative prefixes are disjoint).
    std::string staging_prefix = full_staging_prefix.substr(physical_root.size());
    if (!staging_prefix.empty() && staging_prefix.front() == '/')
        staging_prefix.erase(0, 1);
    staging_prefix += "/";
    const std::string staging_key = staging_prefix + "aaa.tmp";

    const DB::Cas::Layout layout(metadata_storage->poolForTest()->poolConfig().pool_prefix);
    const std::string blobs_prefix = layout.blobsPrefix();

    EXPECT_EQ(staging_prefix, "pool/staging/mountA/") << "sanity: the accessor's own shape";
    EXPECT_EQ(blobs_prefix, "pool/blobs/");
    EXPECT_FALSE(staging_prefix.starts_with(blobs_prefix));
    EXPECT_FALSE(blobs_prefix.starts_with(staging_prefix));
    EXPECT_FALSE(staging_key.starts_with(blobs_prefix));
}

#if USE_AWS_S3

namespace
{

/// A `LocalObjectStorage` that reports the GCS generation dialect
/// (`conditionalOpsUseGenerationTokens() == true`) and a non-`Local` `getType()`, so
/// `ContentAddressedMetadataStorage::openPoolView` builds its backend in `Mode::Native` with
/// `native_token_type == Dialect::Generation`. The fake also advertises native copy so generation
/// token mode can exercise explicit S3 staging without endpoint/provider heuristics.
///
/// Holds every object entirely in memory, keyed by the BARE CAS key exactly as `Backend` hands it to
/// `object_storage` (e.g. `"pool/_probe/<hex>/token"`). Native mode never asks `object_storage` to
/// resolve that key against anything (`ContentAddressedMetadataStorage::physicalKey` is a documented
/// no-op for Native, since a real S3 client resolves a bucket-relative key against its own bucket/prefix
/// configuration internally) -- so this fake never needs a notion of "resolve a key to a location" at
/// all, unlike a real filesystem-backed object storage would. That sidesteps the class of bug a
/// resolve-then-strip round trip through the real `LocalObjectStorage` file/list implementation is prone
/// to (a key is a key, with no round trip to get wrong), and it never touches the real filesystem, so it
/// cannot leak files into the test process's working directory either.
///
/// The Native backend requires REAL conditional-write enforcement: the precondition is evaluated when a write completes,
/// signaled by an `S3Exception` carrying the canonical `PreconditionFailed` name (see
/// `ObjectStorageBackend::finalizeConditionalWrite`). `writeObject` therefore buffers bytes in memory and
/// defers both the precondition check and the commit to `finalize`, mirroring how a real object store
/// only commits -- and only then can reject -- on PUT completion.
class FakeGenerationObjectStorage final : public DB::LocalObjectStorage
{
public:
    using DB::LocalObjectStorage::LocalObjectStorage;

    DB::ObjectStorageType getType() const override { return DB::ObjectStorageType::S3; }
    bool conditionalOpsUseGenerationTokens() const override { return true; }
    std::optional<bool> isBucketVersioningEnabled() const override { return false; }
    bool supportsRetryProfile(DB::ObjectStorageRetryProfile) const override { return true; }
    bool supportsCopyMode(DB::ObjectStorageCopyMode mode) const override
    {
        return mode == DB::ObjectStorageCopyMode::Default || mode == DB::ObjectStorageCopyMode::NativeOnly;
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject(
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> /*attributes*/,
        size_t /*buf_size*/,
        const DB::WriteSettings & write_settings) override
    {
        if (mode != DB::WriteMode::Rewrite)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "FakeGenerationObjectStorage only supports Rewrite");
        return std::make_unique<ConditionalWriteBuffer>(
            *this, object.remote_path, write_settings.object_storage_write_if_none_match,
            write_settings.object_storage_write_if_match);
    }

    bool exists(const DB::StoredObject & object) const override
    {
        std::lock_guard lock(mutex);
        return objects.contains(object.remote_path);
    }

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject(
        const DB::StoredObject & object,
        const DB::ReadSettings & /*read_settings*/,
        std::optional<size_t> /*read_hint*/,
        bool /*use_external_buffer*/,
        bool /*restrict_seek*/) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
            /// `RESOURCE_NOT_FOUND`, not a plain `DB::Exception`: the Native backend reads absence ONLY from
            /// an `S3Exception` carrying `NO_SUCH_KEY`/`RESOURCE_NOT_FOUND` (`isObjectNotFound`). A bodyless
            /// HEAD on a real absent S3 key throws exactly this code.
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        /// Copies the bytes, matching a real remote read (no shared ownership with the stored entry, so
        /// a later overwrite of this key cannot mutate bytes a caller is still reading).
        return std::make_unique<DB::ReadBufferFromOwnMemoryFile>(object.remote_path, it->second.bytes);
    }

    DB::ObjectMetadata getObjectMetadata(const std::string & path, bool with_tags) const override
    {
        auto metadata = tryGetObjectMetadata(path, with_tags);
        if (!metadata)
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        return *metadata;
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool /*with_tags*/) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(path);
        if (it == objects.end())
            return std::nullopt;
        DB::ObjectMetadata metadata;
        metadata.size_bytes = it->second.bytes.size();
        metadata.etag = std::to_string(it->second.generation);
        return metadata;
    }

    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(const std::string & path, bool with_tags) const override
    {
        return tryGetObjectMetadata(path, with_tags);
    }

    /// This fake advertises every retry profile, and an in-memory store has no retry behaviour to
    /// vary, so the profile-aware overloads simply forward. A storage that claimed the capability
    /// without implementing them would refuse every control-plane request of a writable mount.
    std::optional<DB::ObjectMetadata> tryGetObjectMetadataWithNativeToken(
        const std::string & path, bool with_tags, const DB::ObjectStorageControlRequest &) const override
    {
        return tryGetObjectMetadata(path, with_tags);
    }

    DB::ObjectStorageIteratorPtr iterate(
        const std::string & path_prefix, size_t max_keys, bool with_tags, const std::optional<std::string> & start_after,
        const DB::ObjectStorageControlRequest &) const override
    {
        return DB::LocalObjectStorage::iterate(path_prefix, max_keys, with_tags, start_after);
    }

    DB::ConditionalRemoveResult removeObjectIfTokenMatches(
        const DB::StoredObject & object, const std::string & etag, const DB::ObjectStorageControlRequest &) override
    {
        return removeObjectIfTokenMatches(object, etag);
    }
    using DB::LocalObjectStorage::removeObjectIfTokenMatches;

    /// A real S3 GET answers with the object's incarnation, which is what the backend reads its
    /// bytes AND its generation from in one request. Quoted, the way the SDK's ETag field carries a
    /// generation across the HTTP boundary.
    DB::SmallObjectDataWithMetadata readSmallObjectAndGetObjectMetadata(
        const DB::StoredObject & object, const DB::ReadSettings &, size_t, std::optional<size_t>) const override
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
            throw DB::S3Exception("FakeGenerationObjectStorage: object does not exist",
                                   Aws::S3::S3Errors::RESOURCE_NOT_FOUND);
        DB::SmallObjectDataWithMetadata result;
        result.data = it->second.bytes;
        result.metadata.size_bytes = it->second.bytes.size();
        result.metadata.etag = "\"" + std::to_string(it->second.generation) + "\"";
        return result;
    }

    void removeObjectIfExists(const DB::StoredObject & object) override
    {
        std::lock_guard lock(mutex);
        objects.erase(object.remote_path);
    }

    void removeObjectsIfExist(const DB::StoredObjects & objects_to_remove) override
    {
        std::lock_guard lock(mutex);
        for (const auto & object : objects_to_remove)
            objects.erase(object.remote_path);
    }

    /// `path` is a bare CAS-relative prefix (e.g. `"pool/"` or `"pool/_probe/<hex>"`) -- exactly what
    /// `Backend::list`'s Native-mode path passes through unchanged (it applies no prefix stripping of
    /// its own there) and expects back on every listed key. A plain string-prefix scan over the
    /// in-memory keys already IS that key space, so there is no separate "physical" representation to
    /// resolve to or strip back off.
    void listObjects(const std::string & path, DB::RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        std::lock_guard lock(mutex);
        for (const auto & [key, entry] : objects)
        {
            if (!key.starts_with(path))
                continue;
            DB::ObjectMetadata metadata;
            metadata.size_bytes = entry.bytes.size();
            metadata.etag = std::to_string(entry.generation);
            children.push_back(std::make_shared<DB::RelativePathWithMetadata>(key, std::move(metadata)));
            if (max_keys != 0 && children.size() >= max_keys)
                break;
        }
    }

    DB::ConditionalRemoveResult removeObjectIfTokenMatches(const DB::StoredObject & object, const std::string & etag) override
    {
        std::lock_guard lock(mutex);
        DB::ConditionalRemoveResult result;
        auto it = objects.find(object.remote_path);
        if (it == objects.end())
        {
            result.outcome = DB::ConditionalRemoveOutcome::NotFound;
            return result;
        }
        if (std::to_string(it->second.generation) != etag)
        {
            result.outcome = DB::ConditionalRemoveOutcome::TokenMismatch;
            return result;
        }
        objects.erase(it);
        result.outcome = DB::ConditionalRemoveOutcome::Removed;
        return result;
    }

    /// Checks the write-once/exact-token precondition against the current generation and, on success,
    /// stores `bytes` and mints the next generation. Throws an `S3Exception` naming `PreconditionFailed`
    /// on a lost condition -- the one signal `finalizeConditionalWrite` classifies as
    /// `ConditionalWriteOutcome::PreconditionLost` rather than an ordinary failure.
    /// Returns the generation it minted, the way a real store returns it in the write response: the
    /// backend attributes the write to that generation and nothing reads it back.
    uint64_t commitConditionalWrite(const std::string & key, const std::string & bytes,
                                 const std::string & if_none_match, const std::string & if_match)
    {
        std::lock_guard lock(mutex);
        auto it = objects.find(key);
        const bool exists_now = it != objects.end();
        if (!if_none_match.empty() && exists_now)
            throw DB::S3Exception("FakeGenerationObjectStorage: if-none-match precondition failed",
                                   Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");
        if (!if_match.empty() && (!exists_now || std::to_string(it->second.generation) != if_match))
            throw DB::S3Exception("FakeGenerationObjectStorage: if-match precondition failed",
                                   Aws::S3::S3Errors::UNKNOWN, "PreconditionFailed");

        const uint64_t generation = next_generation++;
        objects[key] = Entry{bytes, generation};
        return generation;
    }

private:
    struct Entry
    {
        std::string bytes;
        uint64_t generation;
    };

    /// Buffers the whole body in memory (like `FakeStagingSink` above) so the entry is committed exactly
    /// once, at `finalize`, and only after the precondition has been checked.
    class ConditionalWriteBuffer final : public DB::WriteBufferFromFileBase
    {
    public:
        ConditionalWriteBuffer(FakeGenerationObjectStorage & storage_, std::string key_,
                                std::string if_none_match_, std::string if_match_)
            : DB::WriteBufferFromFileBase(/*buf_size=*/8192, nullptr, 0)
            , storage(storage_), key(std::move(key_))
            , if_none_match(std::move(if_none_match_)), if_match(std::move(if_match_))
        {
        }

        void sync() override {}
        std::string getFileName() const override { return key; }

        /// The write response's own incarnation, quoted the way the SDK's ETag field carries a GCS
        /// generation across the HTTP boundary -- the backend is what strips that transport syntax.
        std::optional<std::string> getResultObjectETag() const override { return committed_generation; }

    protected:
        void nextImpl() override
        {
            if (!offset())
                return;
            buffered.append(working_buffer.begin(), offset());
        }

        void finalizeImpl() override
        {
            next();
            committed_generation = "\"" + std::to_string(storage.commitConditionalWrite(key, buffered, if_none_match, if_match)) + "\"";
        }

    private:
        FakeGenerationObjectStorage & storage;
        std::string key;
        std::string if_none_match;
        std::string if_match;
        std::string buffered;
        std::optional<std::string> committed_generation;
    };

    mutable std::mutex mutex;
    std::map<std::string, Entry> objects;
    uint64_t next_generation = 1;
};

std::shared_ptr<FakeGenerationObjectStorage> makeFakeGenerationObjectStorageForTest()
{
    static std::atomic<uint64_t> counter{0};
    const auto unique = std::to_string(::getpid()) + "_" + std::to_string(counter.fetch_add(1));
    const auto root = (std::filesystem::temp_directory_path() / ("cas_s3_staging_generation_" + unique)).string();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    std::filesystem::create_directories(root, ec);

    DB::LocalObjectStorageSettings settings("test", root, /*read_only_=*/false);
    return std::make_shared<FakeGenerationObjectStorage>(std::move(settings));
}

}

/// A store whose iterator does not page (the fallback `IObjectStorage::iterate` lists `max_keys` keys
/// once and ends) must still let a page-sized list report that more keys follow. A page that ended
/// exactly at the limit with an empty cursor would read as the end of the prefix, and the startup
/// residual check would then take a prefix of debris plus residue for an empty one.
TEST(CASS3Staging, ListPageOverANonPagingStoreStillReportsMoreKeys)
{
    auto object_storage = makeFakeGenerationObjectStorageForTest();
    auto backend = std::make_shared<DB::Cas::ObjectStorageBackend>(object_storage, DB::Cas::ObjectStorageBackend::Mode::Native);
    for (int i = 0; i < 40; ++i)
        createAt(*backend, fmt::format("p/list/{:03}", i), "x");

    DB::Cas::tests::OperationForTest op(*backend);
    const DB::Cas::ListPage first = (*op).list("p/list/", "", 32, DB::Cas::Retry::once());
    EXPECT_EQ(first.keys.size(), 32u);
    ASSERT_FALSE(first.next_cursor.empty()) << "a full page over a non-paging store must still say there is more";

    const DB::Cas::ListPage rest = (*op).list("p/list/", first.next_cursor, 32, DB::Cas::Retry::once());
    EXPECT_EQ(rest.keys.size(), 8u);
    EXPECT_TRUE(rest.next_cursor.empty());
}

TEST(CASS3Staging, GenerationBackendMayUseNativeOnlyCopy)
{
    auto object_storage = makeFakeGenerationObjectStorageForTest();
    auto metadata_storage = makeS3StagingMetadataStorageForTest(object_storage, "mountGenerationNative");
    metadata_storage->startup();

    auto tx = metadata_storage->createTransaction();
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(*tx);
    writeThroughS3Transaction(
        ca_tx,
        "a11/a11a11a1-1111-4111-8111-111111111111/all_1_1_0/data.bin",
        "generation-native-staging");

    DB::RelativePathsWithMetadata staged;
    object_storage->listObjects(metadata_storage->stagingKeyPrefix(), staged, /*max_keys=*/0);
    EXPECT_EQ(staged.size(), 1u);
}

#endif
