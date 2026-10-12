#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedTransaction.h>
#include <Disks/tests/cas_test_helpers.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/CasKeeperPartRecord.h>
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
        DB::Cas::tests::makeLocalObjectStorageForTest(), "pool", "srv1", "", getContext().context, settings);
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

/// Object-store writes and LISTs: blob bodies and every non-blob key (`Other`).
uint64_t objectStoreWritesAndLists()
{
    uint64_t total = 0;
    for (auto event : {ProfileEvents::CASBlobPut, ProfileEvents::CASBlobList,
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
        EXPECT_EQ(DB::casBlobDigestOf(checksum), contentHash(bytes)) << "size " << size;
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


    const uint64_t before = objectStoreWritesAndLists();

    const String tmp_part = String(READER_TABLE) + "/tmp-fetch_all_1_1_0";
    const String part = String(READER_TABLE) + "/all_1_1_0";
    EXPECT_TRUE(storage->holdKeeperPart(
        tmp_part,
        {{"count.txt", "3"}, {"columns.txt", "columns format version: 1\n1 columns:\n`x` UInt64\n"}, {"p.proj/count.txt", "3"}},
        {{"data.bin", 9, contentHash("content-A")}, {"p.proj/data.bin", 9, contentHash("content-A")}}));

    EXPECT_TRUE(storage->existsDirectory(tmp_part));
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
    EXPECT_EQ(storage->getFileSize(part + "/p.proj/data.bin"), 9u);

    {
        auto tx = storage->createTransaction();
        tx->removeRecursive(part, {});
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(part));

    EXPECT_EQ(objectStoreWritesAndLists(), before);
}

/// The MergeTree fast removal of an outdated part (`DataPartStorageOnDiskBase::clearDirectory`): one
/// transaction unlinks the part's files, the next removes its directory. On a held part it edits and then
/// forgets what is held, in memory, with no object-store request and no refusal.
TEST(CASKeeperPart, FastRemovalOfHeldPart)
{
    auto storage = openStorage();
    const String part = String(READER_TABLE) + "/delete_tmp_all_5_5_0";
    EXPECT_TRUE(storage->holdKeeperPart(
        part, {{"count.txt", "3"}, {"checksums.txt", "c"}}, {{"data.bin", 9, contentHash("content-A")}}));
    const uint64_t before = objectStoreWritesAndLists();

    {
        auto tx = storage->createTransaction();
        tx->unlinkFile(part + "/count.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->unlinkFile(part + "/data.bin", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->unlinkFile(part + "/txn_version.txt", /*if_exists=*/true, /*should_remove_objects=*/true);
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_EQ(storage->listDirectory(part), (std::vector<std::string>{"checksums.txt"}));
    {
        auto tx = storage->createTransaction();
        tx->unlinkFile(part + "/checksums.txt", /*if_exists=*/false, /*should_remove_objects=*/true);
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_TRUE(storage->isDirectoryEmpty(part));
    {
        auto tx = storage->createTransaction();
        tx->removeDirectory(part);
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(part));
    EXPECT_EQ(objectStoreWritesAndLists(), before);
}

TEST(CASKeeperPart, HeldPartIsNeverReplacedByARecord)
{
    auto storage = openStorage();
    const String part = String(SOURCE_TABLE) + "/all_2_2_0";
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, part + "/data.bin", "content-B");
        writeThroughTransaction(*tx, part + "/count.txt", "5");
        tx->commit(DB::NoCommitOptions{});
    }

    /// A record for a part this disk already holds changes nothing.
    EXPECT_FALSE(storage->holdKeeperPart(part, {{"count.txt", "999"}}, {{"data.bin", 9, contentHash("content-B")}}));
    EXPECT_EQ(storage->tryGetInManifestBytes(part + "/count.txt"), std::optional<String>("5"));

    /// And removing it forgets it.
    {
        auto tx = storage->createTransaction();
        tx->removeRecursive(part, {});
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(part));
}

/// The record is the part's manifest, not a re-application of the placement rule: a part whose manifest
/// carries `statistics.packed` inline (an older part, a hardlink clone of one) gets a record WITH it, and a
/// manifest whose blob is not what the checksums name gets no record at all.
TEST(CASKeeperPart, RecordIsTheManifestInlineEntries)
{
    auto storage = openStorage();
    const String source_part = String(SOURCE_TABLE) + "/all_4_4_0";
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, source_part + "/data.bin", "content-C");
        writeThroughTransaction(*tx, source_part + "/count.txt", "7");
        tx->commit(DB::NoCommitOptions{});
    }
    const auto ns = storage->liveNamespace("a11a11a1-1111-4111-8111-111111111111");
    auto entries = storage->partAccess()->getView({ns, "all_4_4_0"})->manifest()->entries;

    /// A manifest as an older build wrote it: `statistics.packed` Inline.
    DB::Cas::ManifestEntry stats;
    stats.path = "statistics.packed";
    stats.placement = DB::Cas::EntryPlacement::Inline;
    stats.inline_bytes = "stats-bytes";
    entries.push_back(stats);
    std::sort(entries.begin(), entries.end(), [](const auto & a, const auto & b) { return a.path < b.path; });
    storage->partAccess()->install({ns, "all_5_5_0"}, entries);
    const String legacy_part = String(SOURCE_TABLE) + "/all_5_5_0";

    const std::vector<DB::CasKeeperBlobFile> checksummed{
        {"count.txt", 1, contentHash("7")},
        {"data.bin", 9, contentHash("content-C")},
        {"statistics.packed", 11, contentHash("stats-bytes")}};
    const auto record = storage->encodeKeeperPartRecord(legacy_part, checksummed);
    ASSERT_TRUE(record.has_value());
    const auto decoded = storage->decodeKeeperPartRecord(*record);
    EXPECT_EQ(decoded.pool_uuid, storage->getPoolUUID());
    std::vector<String> paths;
    for (const auto & f : decoded.files)
        paths.push_back(f.path);
    EXPECT_EQ(paths, (std::vector<String>{"count.txt", "statistics.packed"}));
    EXPECT_EQ(decoded.files[1].bytes, "stats-bytes");

    /// A blob the checksums name differently, a checksummed file the manifest lacks, and a manifest blob
    /// the checksums do not name: no record (the commit is refused).
    auto wrong_hash = checksummed;
    wrong_hash[1].hash = contentHash("other");
    EXPECT_FALSE(storage->encodeKeeperPartRecord(legacy_part, wrong_hash).has_value());
    auto extra = checksummed;
    extra.push_back({"missing.bin", 3, contentHash("abc")});
    EXPECT_FALSE(storage->encodeKeeperPartRecord(legacy_part, extra).has_value());
    EXPECT_FALSE(storage->encodeKeeperPartRecord(legacy_part, {{"count.txt", 1, contentHash("7")}}).has_value());

    /// No published manifest: no record.
    EXPECT_FALSE(storage->encodeKeeperPartRecord(String(SOURCE_TABLE) + "/all_9_9_0", checksummed).has_value());
}

/// TEC-851 step 3c: a written part costs blob bodies only -- no manifest, ref, marker or namespace object --
/// and identical content in a second part is adopted without a request. Nothing persists in the pool: a
/// second disk over the same pool holds nothing until a record is attached.
TEST(CASKeeperPart, WritesAreBlobBodiesOnlyAndIdenticalContentIsAdopted)
{
    auto storage = openStorage();
    const auto metadata_writes = []
    {
        uint64_t total = 0;
        for (auto event : {ProfileEvents::CASOtherPut, ProfileEvents::CASOtherOverwrite, ProfileEvents::CASOtherCompareSwap,
                           ProfileEvents::CASOtherList,
                           ProfileEvents::CASBlobList})
            total += ProfileEvents::global_counters[event].load(std::memory_order_relaxed);
        return total;
    };
    const auto blob_puts = [] { return ProfileEvents::global_counters[ProfileEvents::CASBlobPut].load(std::memory_order_relaxed); };

    const uint64_t meta_before = metadata_writes();
    const uint64_t puts_before = blob_puts();
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, String(SOURCE_TABLE) + "/all_6_6_0/data.bin", "content-D");
        writeThroughTransaction(*tx, String(SOURCE_TABLE) + "/all_6_6_0/count.txt", "1");
        tx->commit(DB::NoCommitOptions{});
    }
    const uint64_t first_puts = blob_puts() - puts_before;
    EXPECT_GE(first_puts, 1u);
    {
        auto tx = storage->createTransaction();
        writeThroughTransaction(*tx, String(SOURCE_TABLE) + "/all_7_7_0/data.bin", "content-D");
        writeThroughTransaction(*tx, String(SOURCE_TABLE) + "/all_7_7_0/count.txt", "1");
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_EQ(blob_puts() - puts_before, first_puts);   /// adopted: held by all_6_6_0
    EXPECT_EQ(metadata_writes(), meta_before);

    auto other = openStorage();
    EXPECT_FALSE(other->existsDirectory(String(SOURCE_TABLE) + "/all_6_6_0"));
}

namespace
{

void writePart(DB::ContentAddressedMetadataStorage & storage, const String & part, const String & payload)
{
    auto tx = storage.createTransaction();
    writeThroughTransaction(*tx, part + "/data.bin", payload);
    writeThroughTransaction(*tx, part + "/count.txt", "1");
    writeThroughTransaction(*tx, part + "/p.proj/data.bin", payload + "-proj");
    writeThroughTransaction(*tx, part + "/p.proj/count.txt", "1");
    tx->commit(DB::NoCommitOptions{});
}

std::vector<std::string> sorted(std::vector<std::string> v)
{
    std::sort(v.begin(), v.end());
    return v;
}

int errorCode(const std::function<void()> & fn)
{
    try
    {
        fn();
    }
    catch (const DB::Exception & e)
    {
        return e.code();
    }
    return 0;
}

}

/// TEC-851 step 3c: the read surface over the in-memory part store.
TEST(CASKeeperPart, WrittenPartIsListedAndReadFromMemory)
{
    auto storage = openStorage();
    const String table = SOURCE_TABLE;
    writePart(*storage, table + "/all_8_8_0", "payload-E");

    EXPECT_TRUE(storage->existsDirectory(table));
    EXPECT_EQ(sorted(storage->listDirectory(table)), (std::vector<std::string>{"all_8_8_0"}));
    EXPECT_EQ(sorted(storage->listDirectory(table + "/all_8_8_0")), (std::vector<std::string>{"count.txt", "data.bin", "p.proj"}));
    EXPECT_EQ(sorted(storage->listDirectory(table + "/all_8_8_0/p.proj")), (std::vector<std::string>{"count.txt", "data.bin"}));
    EXPECT_TRUE(storage->existsDirectory(table + "/all_8_8_0/p.proj"));
    EXPECT_FALSE(storage->existsDirectory(table + "/all_8_8_0/data.bin"));
    EXPECT_TRUE(storage->existsFile(table + "/all_8_8_0/data.bin"));
    EXPECT_EQ(storage->getFileSize(table + "/all_8_8_0/data.bin"), 9u);
    EXPECT_TRUE(storage->getBlobViewPlan(table + "/all_8_8_0/p.proj/data.bin").has_value());
}

/// RENAME TABLE moves parts in memory; a part rename and the table removal forget in memory.
TEST(CASKeeperPart, RenameAndRemovalAreInMemory)
{
    auto storage = openStorage();
    const String table = SOURCE_TABLE;
    const String other = READER_TABLE;
    writePart(*storage, table + "/all_9_9_0", "payload-F");
    {
        auto tx = storage->createTransaction();
        tx->moveDirectory(table + "/all_9_9_0", table + "/delete_tmp_all_9_9_0");
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(table + "/all_9_9_0"));
    EXPECT_TRUE(storage->existsDirectory(table + "/delete_tmp_all_9_9_0"));
    {
        auto tx = storage->createTransaction();
        tx->moveDirectory(table, other);
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(table));
    EXPECT_EQ(sorted(storage->listDirectory(other)), (std::vector<std::string>{"delete_tmp_all_9_9_0"}));
    {
        auto tx = storage->createTransaction();
        tx->removeRecursive(other, {});
        tx->commit(DB::NoCommitOptions{});
    }
    EXPECT_FALSE(storage->existsDirectory(other));
}

/// What could never get a Keeper record is refused at the operation: a table-level or loose file, DETACH
/// (detached/), a MOVE between disks (moving/) and FREEZE (shadow/).
TEST(CASKeeperPart, TargetsWithoutARecordAreRefused)
{
    auto storage = openStorage();
    const String table = SOURCE_TABLE;
    writePart(*storage, table + "/all_10_10_0", "payload-G");
    const auto refused = [&](const std::function<void(DB::IMetadataTransaction &)> & op)
    {
        return errorCode([&] { auto tx = storage->createTransaction(); op(*tx); tx->commit(DB::NoCommitOptions{}); });
    };
    EXPECT_EQ(refused([&](auto & tx) { tx.moveDirectory(table + "/all_10_10_0", table + "/detached/all_10_10_0"); }),
              DB::ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_EQ(refused([&](auto & tx) { writeThroughTransaction(tx, table + "/moving/all_10_10_0/count.txt", "1"); }),
              DB::ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_EQ(refused([&](auto & tx) { writeThroughTransaction(tx, table + "/format_version.txt", "1\n"); }),
              DB::ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_EQ(refused([&](auto & tx) { writeThroughTransaction(tx, "clickhouse_access_check_x", "test"); }),
              DB::ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_EQ(refused([&](auto & tx) { tx.createHardLink(table + "/all_10_10_0/count.txt", "shadow/1/" + table + "/all_10_10_0/count.txt"); }),
              DB::ErrorCodes::NOT_IMPLEMENTED);
    EXPECT_TRUE(storage->existsDirectory(table + "/all_10_10_0"));
}

