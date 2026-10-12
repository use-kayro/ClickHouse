#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <utility>
#include <vector>

#include <magic_enum.hpp>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// NOT `Disks/tests/cas_test_helpers.h`'s `DB::Cas::tests::expectThrowsCode`: pulling in that header
/// drags along a large chunk of the CAS backend/store machinery this file has no other need for, so it
/// stays clear of `cas_test_helpers.h` entirely and inlines its own copy of the same tiny assertion
/// instead.
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

}

TEST(CASPartManifestFormat, FindEntryBinarySearch)
{
    std::vector<ManifestEntry> entries;
    for (const char * p : {"a.txt", "b/inner.txt", "b/z.txt", "c.txt"})
    {
        ManifestEntry e;
        e.path = p;
        e.placement = EntryPlacement::Inline;
        e.inline_bytes = "v";
        entries.push_back(e);
    }
    EXPECT_NE(findEntry(entries, "a.txt"), nullptr);
    EXPECT_EQ(findEntry(entries, "a.txt")->path, "a.txt");
    EXPECT_NE(findEntry(entries, "c.txt"), nullptr);          /// last element
    EXPECT_EQ(findEntry(entries, "b"), nullptr);              /// prefix of a path, not a path
    EXPECT_EQ(findEntry(entries, "zzz"), nullptr);            /// past the end
    EXPECT_EQ(findEntry({}, "a"), nullptr);                   /// empty
}

TEST(CASPartManifestFormat, EntryRangeContiguousPrefix)
{
    std::vector<ManifestEntry> entries;
    for (const char * p : {"a.txt", "p.proj/data.bin", "p.proj/x.txt", "q.txt"})
    {
        ManifestEntry e;
        e.path = p;
        e.placement = EntryPlacement::Inline;
        e.inline_bytes = "v";
        entries.push_back(e);
    }
    auto [first, last] = entryRange(entries, "p.proj/");
    ASSERT_EQ(last - first, 2);
    EXPECT_EQ(first->path, "p.proj/data.bin");
    EXPECT_EQ((last - 1)->path, "p.proj/x.txt");

    auto [w1, w2] = entryRange(entries, "");                  /// empty prefix = whole span
    EXPECT_EQ(w2 - w1, 4);

    auto [n1, n2] = entryRange(entries, "zzz/");              /// no match
    EXPECT_EQ(n1, n2);
}

/// ---- `cas_part_files`: the Keeper part record (pool + Inline entries) ----

namespace
{

constexpr UInt128 kPartFilesPool = (static_cast<UInt128>(0x0011223344556677ULL) << 64) | 0x8899aabbccddeeffULL;

PartFiles partFilesSample()
{
    ManifestEntry count;
    count.path = "count.txt";
    count.placement = EntryPlacement::Inline;
    count.inline_bytes = "11961";

    ManifestEntry ck;
    ck.path = "checksums.txt";
    ck.placement = EntryPlacement::Inline;
    ck.inline_bytes = String("checksums format version: 4\n\x01\x02\n\x00\xff", 34);   /// binary, embedded '\n' and NUL

    ManifestEntry proj;
    proj.path = "p.proj/columns.txt";
    proj.placement = EntryPlacement::Inline;
    proj.inline_bytes = "";   /// an empty file is a legal Inline entry

    return PartFiles{kPartFilesPool, {count, proj, ck}};   /// entries deliberately out of canonical order
}

}

CAS_BATTERY_COVERS(PartFiles);

TEST(CASFormatBattery, PartFiles)
{
    const PartFiles pf = partFilesSample();
    const String golden =
        currentFormatHeader("cas_part_files") +
        "{\"pool\":\"00112233445566778899aabbccddeeff\"}\n"
        "{\"path\":\"checksums.txt\",\"place\":\"inline\",\"size\":34}\n"
        "{\"path\":\"count.txt\",\"place\":\"inline\",\"size\":5}\n"
        "{\"path\":\"p.proj/columns.txt\",\"place\":\"inline\",\"size\":0}\n"
        "{\"n\":3}\n"
        "==> \"checksums.txt\" size=34 <==\n" + pf.entries[2].inline_bytes + "\n"
        "==> \"count.txt\" size=5 <==\n"
        "11961\n"
        "==> \"p.proj/columns.txt\" size=0 <==\n"
        "\n";
    runFormatBattery({FormatId::PartFiles,
        [&] { return encodePartFiles(pf); },
        [](std::string_view d) { decodePartFiles(std::string(d)); },
        golden});
}

TEST(CASPartFilesFormat, RoundTripsPoolAndBytesInCanonicalOrder)
{
    const PartFiles pf = partFilesSample();
    const PartFiles got = decodePartFiles(encodePartFiles(pf));
    EXPECT_EQ(got.pool_id, kPartFilesPool);
    ASSERT_EQ(got.entries.size(), 3u);
    EXPECT_EQ(got.entries[0].path, "checksums.txt");
    EXPECT_EQ(got.entries[0].inline_bytes, pf.entries[2].inline_bytes);
    EXPECT_EQ(got.entries[0].size(), 34u);
    EXPECT_EQ(got.entries[1].path, "count.txt");
    EXPECT_EQ(got.entries[1].inline_bytes, "11961");
    EXPECT_EQ(got.entries[2].path, "p.proj/columns.txt");
    EXPECT_TRUE(got.entries[2].inline_bytes.empty());
    for (const auto & e : got.entries)
        EXPECT_EQ(e.placement, EntryPlacement::Inline);
}

TEST(CASPartFilesFormat, RejectsBlobEntriesAndMissingPool)
{
    PartFiles pf = partFilesSample();
    ManifestEntry blob;
    blob.path = "a.bin";
    blob.placement = EntryPlacement::Blob;
    blob.ref = BlobRef{BlobHashAlgo::CityHash128, codecFor(BlobHashAlgo::CityHash128).fromHex("00112233445566778899aabbccddeeff")};
    blob.blob_size = 7;
    pf.entries.push_back(blob);
#ifndef DEBUG_OR_SANITIZER_BUILD
    /// LOGICAL_ERROR aborts the process in debug/sanitizer builds rather than throwing.
    expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodePartFiles(pf); });
#endif

    /// A Blob record smuggled into the wire form fails closed on decode.
    const String forged = currentFormatHeader("cas_part_files")
        + "{\"pool\":\"00112233445566778899aabbccddeeff\"}\n"
          "{\"path\":\"a.bin\",\"place\":\"blob\",\"algo\":\"ch128\",\"digest\":\"00112233445566778899aabbccddeeff\",\"size\":7}\n"
          "{\"n\":1}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartFiles(forged); });

    /// The pool names where the derived blobs live; a record without it cannot be attached.
    const String no_pool = currentFormatHeader("cas_part_files") + "{}\n{\"n\":0}\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartFiles(no_pool); });
}

TEST(CASPartFilesFormat, RejectsDuplicateTraversalAndForeignHeader)
{
    PartFiles dup = partFilesSample();
    dup.entries.push_back(dup.entries[0]);
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { encodePartFiles(dup); });

    const String traversal = currentFormatHeader("cas_part_files")
        + "{\"pool\":\"00112233445566778899aabbccddeeff\"}\n"
          "{\"path\":\"../x\",\"place\":\"inline\",\"size\":0}\n{\"n\":1}\n==> \"../x\" size=0 <==\n\n";
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodePartFiles(traversal); });

    /// The header type gates the decoder: another class's object is not a part record.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { decodePartFiles(currentFormatHeader("cas_pool_meta") + "{\"pool\":\"00112233445566778899aabbccddeeff\"}\n{\"n\":0}\n"); });
}
