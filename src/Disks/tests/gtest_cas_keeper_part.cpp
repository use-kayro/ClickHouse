#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Common/ProfileEvents.h>
#include <Common/Exception.h>
#include <algorithm>
#include <filesystem>

/// A part whose metadata is held from Keeper (the replica's `cas_part_files` record, TEC-849): it is
/// served, renamed and removed in memory, with no write and no LIST to the object store, and a part the
/// ref table already holds is never shadowed by one.

namespace ProfileEvents
{
    extern const Event CASBlobPut;
    extern const Event CASBlobList;
    extern const Event CASManifestPut;
    extern const Event CASManifestOverwrite;
    extern const Event CASManifestCompareSwap;
    extern const Event CASManifestList;
    extern const Event CASRootPut;
    extern const Event CASRootOverwrite;
    extern const Event CASRootCompareSwap;
    extern const Event CASRootList;
    extern const Event CASOtherPut;
    extern const Event CASOtherOverwrite;
    extern const Event CASOtherCompareSwap;
    extern const Event CASOtherList;
}

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

constexpr auto SOURCE_TABLE = "a11/a11a11a1-1111-4111-8111-111111111111";
constexpr auto READER_TABLE = "b22/b22b22b2-2222-4222-8222-222222222222";

std::shared_ptr<DB::ContentAddressedMetadataStorage> openStorage()
{
    auto settings = DB::Cas::tests::makeSettingsForTest(
        "srv1", std::filesystem::temp_directory_path() / "cas_keeper_part_scratch");
    auto storage = std::make_shared<DB::ContentAddressedMetadataStorage>(
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", nullptr, settings);
    storage->startup();
    return storage;
}

void writeThroughTransaction(DB::IMetadataTransaction & tx, const String & path, const String & bytes)
{
    auto & ca_tx = dynamic_cast<DB::ContentAddressedTransaction &>(tx);
    auto buf = ca_tx.writeFile(path, 65536, DB::WriteMode::Rewrite, {});
    buf->write(bytes.data(), bytes.size());
    buf->finalize();
}

/// Object-store writes and LISTs against blobs, manifests, ref state and namespace files. The mount lease
/// (Server) and GC classes are excluded: their background renewals are not caused by the operations under
/// test.
uint64_t objectStoreWritesAndLists()
{
    uint64_t total = 0;
    for (auto event : {ProfileEvents::CASBlobPut, ProfileEvents::CASBlobList,
                       ProfileEvents::CASManifestPut, ProfileEvents::CASManifestOverwrite, ProfileEvents::CASManifestCompareSwap,
                       ProfileEvents::CASManifestList,
                       ProfileEvents::CASRootPut, ProfileEvents::CASRootOverwrite, ProfileEvents::CASRootCompareSwap,
                       ProfileEvents::CASRootList,
                       ProfileEvents::CASOtherPut, ProfileEvents::CASOtherOverwrite, ProfileEvents::CASOtherCompareSwap,
                       ProfileEvents::CASOtherList})
        total += ProfileEvents::global_counters[event].load(std::memory_order_relaxed);
    return total;
}

UInt128 contentHash(const String & bytes)
{
    return DB::Cas::poolContentHash(DB::Cas::BlobHashAlgo::CityHash128, bytes).digest.toU128();
}

}

/// The MergeTree per-file checksum IS the `ch128` blob digest: the reader derives blob keys from
/// `checksums.txt` with exactly this conversion (`CasKeeperPartRecord.cpp`), across hashing blocks.
TEST(CASKeeperPart, MergeTreeFileHashIsTheBlobDigest)
{
    for (size_t size : {0UL, 1UL, 100UL, 2048UL, 5000UL, 1UL << 20})
    {
        String bytes(size, '\0');
        for (size_t i = 0; i < size; ++i)
            bytes[i] = static_cast<char>((i * 131 + 7) & 0xff);
        DB::WriteBufferFromOwnString sink;
        DB::HashingWriteBuffer hashing(sink);
        hashing.write(bytes.data(), bytes.size());
        hashing.finalize();
        const DB::MergeTreeDataPartChecksum checksum(hashing.count(), hashing.getHash());
        const UInt128 from_checksum
            = (static_cast<UInt128>(checksum.file_hash.high64) << 64) | static_cast<UInt128>(checksum.file_hash.low64);
        EXPECT_EQ(from_checksum, contentHash(bytes)) << "size " << size;
        EXPECT_EQ(checksum.file_size, size);
    }
}

TEST(CASKeeperPart, HeldPartIsServedRenamedAndRemovedWithoutObjectStoreWritesOrLists)
{
    auto storage = openStorage();

    /// The source replica's part, published the ordinary way: its `data.bin` blob is in the pool.
    const String source_part = String(SOURCE_TABLE) + "/all_1_1_0";
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, source_part + "/data.bin", "content-A");
        writeThroughTransaction(*tx, source_part + "/count.txt", "3");
        tx->commit(DB::NoCommitOptions{});
    }
    const auto source_plan = storage->getBlobViewPlan(source_part + "/data.bin");
    ASSERT_TRUE(source_plan.has_value());

    /// The reader table's namespace exists before any part arrives (CREATE TABLE writes its files).
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, String(READER_TABLE) + "/format_version.txt", "1\n");
        tx->commit(DB::NoCommitOptions{});
    }
    (void)storage->listDirectory(READER_TABLE);   /// warm the reader namespace

    const uint64_t before = objectStoreWritesAndLists();

    const String tmp_part = String(READER_TABLE) + "/tmp-fetch_all_1_1_0";
    const String part = String(READER_TABLE) + "/all_1_1_0";
    EXPECT_TRUE(storage->holdKeeperPart(
        tmp_part,
        {{"count.txt", "3"}, {"columns.txt", "columns format version: 1\n1 columns:\n`x` UInt64\n"}, {"p.proj/count.txt", "3"}},
        {{"data.bin", 9, contentHash("content-A")}, {"p.proj/data.bin", 9, contentHash("content-A")}}));

    EXPECT_TRUE(storage->existsDirectory(tmp_part));
    EXPECT_TRUE(storage->isHeldFromKeeper(tmp_part));
    EXPECT_FALSE(storage->isHeldFromKeeper(source_part));
    EXPECT_TRUE(storage->existsFile(tmp_part + "/data.bin"));
    EXPECT_EQ(storage->getFileSize(tmp_part + "/data.bin"), 9u);
    EXPECT_EQ(storage->tryGetInManifestBytes(tmp_part + "/count.txt"), std::optional<String>("3"));
    EXPECT_TRUE(storage->existsDirectory(tmp_part + "/p.proj"));
    /// A file inside a part is answered by the part's view (TEC-845 route), not by a LIST.
    EXPECT_FALSE(storage->existsDirectory(tmp_part + "/data.bin"));

    /// The derived blob entry addresses the very object the source replica published.
    const auto held_plan = storage->getBlobViewPlan(tmp_part + "/data.bin");
    ASSERT_TRUE(held_plan.has_value());
    EXPECT_EQ(held_plan->object.remote_path, source_plan->object.remote_path);
    EXPECT_EQ(held_plan->payload_offset, source_plan->payload_offset);
    EXPECT_EQ(held_plan->payload_end, source_plan->payload_end);

    auto names = storage->listDirectory(tmp_part);
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names, (std::vector<std::string>{"columns.txt", "count.txt", "data.bin", "p.proj"}));

    /// tmp-fetch -> final, as `renameTempPartAndReplace` does.
    {
        auto tx = storage->createTransaction();
        tx->moveDirectory(tmp_part, part);
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(tmp_part));
    EXPECT_TRUE(storage->existsDirectory(part));
    EXPECT_TRUE(storage->isHeldFromKeeper(part + "/"));
    EXPECT_EQ(storage->getFileSize(part + "/p.proj/data.bin"), 9u);

    {
        auto tx = storage->createTransaction();
        tx->removeRecursive(part, {});
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(part));

    EXPECT_EQ(objectStoreWritesAndLists(), before);
}

TEST(CASKeeperPart, WriteIntoHeldPartIsRefused)
{
    auto storage = openStorage();
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, String(READER_TABLE) + "/format_version.txt", "1\n");
        tx->commit(DB::NoCommitOptions{});
    }
    const String part = String(READER_TABLE) + "/all_3_3_0";
    EXPECT_TRUE(storage->holdKeeperPart(part, {{"count.txt", "3"}}, {}));

    /// A held part has no ref in the pool to repoint and nothing would persist the change: refused,
    /// never silently lost, and the held content is unchanged.
    auto tx = storage->createTransaction();
    try
    {
        writeThroughTransaction(*tx, part + "/count.txt", "4");
        tx->commit(DB::NoCommitOptions{});
        FAIL() << "a write into a Keeper-held part must be refused";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::NOT_IMPLEMENTED) << e.message();
    }
    EXPECT_EQ(storage->tryGetInManifestBytes(part + "/count.txt"), std::optional<String>("3"));
}

TEST(CASKeeperPart, RefTablePartIsNeverShadowed)
{
    auto storage = openStorage();
    const String part = String(SOURCE_TABLE) + "/all_2_2_0";
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, part + "/data.bin", "content-B");
        writeThroughTransaction(*tx, part + "/count.txt", "5");
        tx->commit(DB::NoCommitOptions{});
    }

    /// A record for a part this server already has in its ref table leaves the ref table in charge.
    EXPECT_FALSE(storage->holdKeeperPart(part, {{"count.txt", "999"}}, {{"data.bin", 9, contentHash("content-B")}}));
    EXPECT_EQ(storage->tryGetInManifestBytes(part + "/count.txt"), std::optional<String>("5"));

    /// And removing it drops the ref-table ref, not a phantom held copy.
    {
        auto tx = storage->createTransaction();
        tx->removeRecursive(part, {});
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(part));
}

TEST(CASKeeperPart, RecordFilesFollowThePlacementRule)
{
    auto storage = openStorage();
    EXPECT_TRUE(storage->isKeeperRecordFile("checksums.txt", 4096));
    EXPECT_TRUE(storage->isKeeperRecordFile("p.proj/columns.txt", 100));
    EXPECT_FALSE(storage->isKeeperRecordFile("data.bin", 10));
    EXPECT_FALSE(storage->isKeeperRecordFile("statistics.packed", 10));
    EXPECT_FALSE(storage->isKeeperRecordFile("primary.cidx", 2 * 1024 * 1024));   /// above INLINE_CAP: a blob

    const auto record = storage->encodeKeeperPartRecord({{"checksums.txt", "x"}, {"count.txt", "3"}});
    ASSERT_TRUE(record.has_value());
    const auto decoded = storage->decodeKeeperPartRecord(*record);
    EXPECT_EQ(decoded.pool_uuid, storage->getPoolUUID());
    ASSERT_EQ(decoded.files.size(), 2u);
    EXPECT_EQ(decoded.files[0].path, "checksums.txt");
    EXPECT_EQ(decoded.files[1].bytes, "3");
}
