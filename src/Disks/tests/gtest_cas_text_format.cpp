#include <gtest/gtest.h>
#include "cas_format_test_battery.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <fmt/format.h>
#include <algorithm>
#include <array>
#include <set>

using namespace DB::Cas;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{
/// Run `f` and require a DB::Exception with exactly `code`.
template <typename F>
void expectCode(int code, F && f)
{
    try
    {
        f();
        FAIL() << "expected exception code " << code;
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), code);
    }
}
}

TEST(CASFormatBattery, EveryRegisteredFormatIsBatteryCovered)
{
    std::set<FormatId> registered;
    for (FormatId id : allRegisteredFormatIds())
        registered.insert(id);
    EXPECT_EQ(registered, DB::Cas::tests::batteryCoveredIds())
        << "a registered codec is missing from the common battery (or vice versa)";
}

TEST(CASFormatIds, LiveIdsKeepTheirFrozenValues)
{
    EXPECT_EQ(static_cast<uint16_t>(FormatId::Blob), 1);
    EXPECT_EQ(static_cast<uint16_t>(FormatId::PoolMeta), 8);
    EXPECT_EQ(static_cast<uint16_t>(FormatId::PartFiles), 26);
}

/// ---- Task 3: per-format traits registry ----

TEST(CASFormatTraits, CompleteUniqueAndGated)
{
    /// Completeness: every FormatId has traits.
    const FormatId all[] = {FormatId::Blob, FormatId::PoolMeta, FormatId::PartFiles};
    std::set<std::string_view> types;
    for (FormatId id : all)
    {
        const FormatTraits & t = traitsFor(id);
        EXPECT_EQ(t.id, id);
        EXPECT_TRUE(t.type.starts_with("cas_")) << t.type;
        EXPECT_TRUE(types.insert(t.type).second) << "duplicate type " << t.type;
        EXPECT_EQ(traitsForType(t.type), &t);
    }
    EXPECT_EQ(traitsForType("cas_nope"), nullptr);
}


/// ---- Task 4: JSON micro-vocabulary + JsonObjectReader ----

TEST(CASJsonVocab, WriteAndReadBack)
{
    CasJsonWriter out;
    bool first = true;
    writeKey(out, "tag", first);
    writeHex128Value(out, hexToU128("000102030405060708090a0b0c0d0e0f"));
    writeKey(out, "seq", first);
    writeU64StringValue(out, 18446744073709551615ULL);
    writeKey(out, "n", first);
    writeIntText(7, out);
    writeKey(out, "ref", first);
    writeStringValue(out, "t-1/all_1_2_0\n\"quoted\"");
    closeObject(out, first);
    const String rendered = std::move(out).take();
    EXPECT_EQ(rendered.substr(0, 45), R"({"tag":"000102030405060708090a0b0c0d0e0f","se)");

    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    JsonObjectReader r(in, KeyStrictness::Strict, "test");
    String key;
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "tag");
    EXPECT_EQ(r.readHex128(), hexToU128("000102030405060708090a0b0c0d0e0f"));
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "seq");
    EXPECT_EQ(r.readU64String(), 18446744073709551615ULL);
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "n");
    EXPECT_EQ(r.readU64Number(), 7u);
    ASSERT_TRUE(r.nextKey(key)); EXPECT_EQ(key, "ref");
    EXPECT_EQ(r.readString(), "t-1/all_1_2_0\n\"quoted\"");
    EXPECT_FALSE(r.nextKey(key));
}

TEST(CASJsonVocab, WordArrayFieldAndReaderRejectInvalidValues)
{
    CasJsonWriter out;
    bool first = true;
    const std::array<std::string_view, 2> words{"ch128", "sha256"};
    writeWordArrayField(out, WireKey{"algos_used"}, words, first);
    closeObject(out, first);
    EXPECT_EQ(std::move(out).take(), "{\"algos_used\":[\"ch128\",\"sha256\"]}");

    const auto read = [](std::string_view text)
    {
        DB::ReadBufferFromMemory in(text.data(), text.size());
        JsonObjectReader r(in, KeyStrictness::Tolerant, "test");
        String key;
        EXPECT_TRUE(r.nextKey(key));
        return r.readStringArray();
    };
    EXPECT_EQ(read(R"({"algos_used":["ch128","sha256"]})"), (std::vector<String>{"ch128", "sha256"}));
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { read(R"({"algos_used":"ch128"})"); });
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { read(R"({"algos_used":["ch128",1]})"); });
}

TEST(CASJsonVocab, FailClosedRules)
{
    auto reader = [](std::string_view text, KeyStrictness s, auto && consume)
    {
        DB::ReadBufferFromMemory in(text.data(), text.size());
        JsonObjectReader r(in, s, "test");
        consume(r);
    };
    /// duplicate key
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"a":1,"a":2})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        while (r.nextKey(k)) r.readU64Number();
    }); });
    /// unknown key: Tolerant skips (nested value), Strict rejects
    reader(R"({"zz":{"deep":[1,2]},"n":5})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        ASSERT_TRUE(r.nextKey(k)); r.skipUnknown(k);
        ASSERT_TRUE(r.nextKey(k)); EXPECT_EQ(r.readU64Number(), 5u);
        EXPECT_FALSE(r.nextKey(k));
    });
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"zz":1})", KeyStrictness::Strict, [](auto & r)
    {
        String k;
        ASSERT_TRUE(r.nextKey(k)); r.skipUnknown(k);
    }); });
    /// critical key fails closed regardless of strictness
    expectCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { reader(R"({"!x":1})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        ASSERT_TRUE(r.nextKey(k)); r.skipUnknown(k);
    }); });
    /// whitespace is not canonical
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({ "a":1})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        r.nextKey(k);
    }); });
    /// bad hex width / junk in u64 string
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"digest":"0102"})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        r.nextKey(k); r.readHex128();
    }); });
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { reader(R"({"u64_string_field":"12x"})", KeyStrictness::Tolerant, [](auto & r)
    {
        String k;
        r.nextKey(k); r.readU64String();
    }); });
}

/// ---- Task 5: header line, trailer line, readLine ----

TEST(CASTextHeader, WriteExpectSniffGate)
{
    CasJsonWriter out;
    writeHeaderLine(out, FormatId::PoolMeta);
    const String rendered = std::move(out).take();
    EXPECT_EQ(rendered, fmt::format("{{\"type\":\"cas_pool_meta\",\"v\":{}}}\n", currentCompatibilityVersion()));

    DB::ReadBufferFromMemory in(rendered.data(), rendered.size());
    const TextHeader h = expectHeaderLine(in, FormatId::PoolMeta);
    EXPECT_EQ(h.type, "cas_pool_meta");
    EXPECT_EQ(h.v, currentCompatibilityVersion());
    EXPECT_TRUE(in.eof());

    const auto sniffed = sniffHeaderLine(rendered);
    ASSERT_TRUE(sniffed.has_value());
    EXPECT_EQ(sniffed->type, "cas_pool_meta");
    EXPECT_FALSE(sniffHeaderLine("PAR1 not a cas object").has_value());

    /// wrong type -> CORRUPTED_DATA; future v -> UNKNOWN_FORMAT_VERSION
    /// `v:1` is the baseline generation, so it always passes the header gate -- the type mismatch is
    /// what has to fail here.
    const String wrong = "{\"type\":\"cas_owner\",\"v\":1}\n";
    DB::ReadBufferFromMemory in2(wrong.data(), wrong.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { expectHeaderLine(in2, FormatId::PoolMeta); });
    const String future = fmt::format("{{\"type\":\"cas_pool_meta\",\"v\":{}}}\n", currentCompatibilityVersion() + 1);
    DB::ReadBufferFromMemory in3(future.data(), future.size());
    expectCode(DB::ErrorCodes::UNKNOWN_FORMAT_VERSION, [&] { expectHeaderLine(in3, FormatId::PoolMeta); });

    const String out_of_range = "{\"type\":\"cas_pool_meta\",\"v\":4294967299}\n";
    DB::ReadBufferFromMemory in4(out_of_range.data(), out_of_range.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { expectHeaderLine(in4, FormatId::PoolMeta); });
}

TEST(CASTextLines, ReadLineAndTrailer)
{
    CasJsonWriter out;
    writeTrailerLine(out, 42);
    EXPECT_EQ(std::move(out).take(), "{\"n\":42}\n");

    const String two = "abc\ndef\n";
    DB::ReadBufferFromMemory in(two.data(), two.size());
    EXPECT_EQ(readLine(in, 16, "test"), "abc");
    EXPECT_EQ(readLine(in, 16, "test"), "def");
    /// missing terminator and over-cap both fail closed
    const String noterm = "abc";
    DB::ReadBufferFromMemory in2(noterm.data(), noterm.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { readLine(in2, 16, "test"); });
    DB::ReadBufferFromMemory in3(two.data(), two.size());
    expectCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { readLine(in3, 2, "test"); });
}

TEST(CASTextValueEscaping, ForwardSlashPinnedUnescaped)
{
    /// Goes RED if the global escape_forward_slashes default ever leaks back into CAS string values.
    /// CAS values are dense with '/' (ref-paths, object keys); their bytes must be CAS-owned so every
    /// golden text file is independent of the global default.
    CasJsonWriter out;
    writeStringValue(out, "ns/shard/all_1_2_0");
    EXPECT_EQ(std::move(out).take(), "\"ns/shard/all_1_2_0\"");
}
