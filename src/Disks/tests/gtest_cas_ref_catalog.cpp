#include "cas_format_test_battery.h"
#include "cas_test_helpers.h"
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasHotKeys.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCatalog.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
/// Explicit rather than relying on a transitive path: `DEBUG_OR_SANITIZER_BUILD` (used below to gate
/// the `*DeathTest` split) must resolve in THIS translation unit.
#include <base/defines.h>
#include <Poco/AutoPtr.h>
#include <Poco/StreamChannel.h>
#include <fmt/format.h>
#include <algorithm>
#include <expected>
#include <latch>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#include <magic_enum.hpp>

using namespace DB::Cas;
using DB::Cas::tests::CountingBackend;
using DB::Cas::tests::FakeClock;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int LIMIT_EXCEEDED;
    extern const int NETWORK_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int S3_ERROR;
}

namespace
{

/// Hand-builds one raw `entry` line, bypassing `encodeRefCatalog` entirely -- used by the decode-side
/// rejection tests, which must exercise bytes the encoder itself would refuse to produce.
String rawEntryLine(const String & ns, const String & state, const String & inc_hex,
                   std::optional<std::tuple<String, uint64_t, uint64_t>> creator = std::nullopt)
{
    if (!creator)
        return fmt::format(R"({{"kind":"entry","ns":"{}","state":"{}","life":"{}"}})", ns, state, inc_hex);
    const auto & [srid, we, fg] = *creator;
    return fmt::format(R"({{"kind":"entry","ns":"{}","state":"{}","life":"{}","creator":"{}","creator_epoch":"{}","creator_fence":"{}"}})",
                        ns, state, inc_hex, srid, we, fg);
}

/// Wraps `entry_lines` in the header/trailer a real `cas_ref_catalog` object carries. `v:1` always
/// passes the header gate because any version <= the build's `G_BUILD` does.
String rawCatalog(const std::vector<String> & entry_lines)
{
    String out = R"({"type":"cas_ref_catalog","v":1})" "\n";
    for (const String & l : entry_lines)
        out += l + "\n";
    out += fmt::format("{{\"n\":{}}}\n", entry_lines.size());
    return out;
}

String withRemovalStartedRound(String line, uint64_t round)
{
    const size_t close = line.rfind('}');
    EXPECT_NE(close, String::npos);
    line.insert(close, fmt::format(R"(,"remove_round":"{}")", round));
    return line;
}

CatalogEntry liveEntry(const String & ns, uint64_t inc)
{
    return CatalogEntry{.ns = RootNamespace{ns}, .state = NsState::Live, .incarnation = UInt128(inc)};
}

CatalogEntry entryInState(const String & ns, NsState state, uint64_t inc)
{
    CatalogEntry entry{.ns = RootNamespace{ns}, .state = state, .incarnation = UInt128(inc)};
    if (state == NsState::Creating)
        entry.creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1};
    if (state == NsState::Removing)
        entry.removal_started_round = 1;
    return entry;
}

/// Per-key counts of the WRITE primitive. `CountingBackend` counts reads, heads and lists per key but
/// only totals for writes, and its legacy per-verb counters never see a caller that speaks the
/// primitives -- which every catalog writer below does.
class WriteCountingBackend : public DB::Cas::tests::CountingBackend
{
public:
    uint64_t writes(const String & key) const
    {
        std::lock_guard lock(write_count_mutex);
        const auto it = write_counts.find(key);
        return it == write_counts.end() ? 0 : it->second;
    }

    std::expected<String, DB::Cas::Backend::RawConflict> write(const String & key, const String & bytes,
                                                               const std::optional<String> & expected_value,
                                                               DB::Cas::TransportAccess & access) override
    {
        {
            std::lock_guard lock(write_count_mutex);
            ++write_counts[key];
        }
        return CountingBackend::write(key, bytes, expected_value, access);
    }

private:
    mutable std::mutex write_count_mutex;
    std::map<String, uint64_t> write_counts;
};

/// Erases `ns`'s row from the catalog with one raw conditional write, as another server's catalog
/// mutation would land underneath a pool's cached copy.
void eraseRowRaw(CasOperation & op, const Layout & layout, const RootNamespace & ns)
{
    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    RefCatalog next = snap.catalog;
    std::erase_if(next.entries, [&](const CatalogEntry & e) { return e.ns == ns; });
    ASSERT_TRUE(std::holds_alternative<Committed>(
        op.replace(layout.refCatalogKey(), encodeRefCatalog(next), *snap.etag, Retry::standard())));
}

/// Seeds one object, failing the current test rather than returning a value nobody checks.
void seedObject(CasOperation & op, const String & key, const String & bytes)
{
    ASSERT_TRUE(std::holds_alternative<Committed>(op.create(key, bytes, Retry::standard())));
}

}

/// ---------- format-battery registration ----------

CAS_BATTERY_COVERS(RefCatalog);

TEST(CASFormatBattery, RefCatalog)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating,
        .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = 5, .fence_generation = 2}});
    c.entries.push_back(liveEntry("b", 2));
    runFormatBattery({FormatId::RefCatalog,
        [&] { return sealObject(FormatId::RefCatalog, encodeRefCatalog(c)); },
        [](std::string_view s) { decodeRefCatalog(std::string(openObject(FormatId::RefCatalog, s))); },
        currentFormatHeader("cas_ref_catalog") +
        "{\"kind\":\"entry\",\"ns\":\"a\",\"state\":\"creating\",\"life\":\"00000000000000000000000000000001\","
        "\"creator\":\"srv1\",\"creator_epoch\":\"5\",\"creator_fence\":\"2\"}\n"
        "{\"kind\":\"entry\",\"ns\":\"b\",\"state\":\"live\",\"life\":\"00000000000000000000000000000002\"}\n"
        "{\"n\":2}\n"});
}

/// Closed-set pin: the three `NsState` words, walked through `magic_enum::enum_values`, which is what
/// proves the renderer and the parser consult the SAME table: a table entry missing altogether is
/// already a build error at the coverage assert, but two delegates drifting onto different tables is
/// not.
TEST(CASRefCatalogFormat, ClosedSetPinsNsStateWords)
{
    EXPECT_EQ(nsStateToWord(NsState::Creating), "creating");
    EXPECT_EQ(nsStateToWord(NsState::Live), "live");
    EXPECT_EQ(nsStateToWord(NsState::Removing), "removing");
    for (const auto s : magic_enum::enum_values<NsState>())
        EXPECT_EQ(nsStateFromWord(nsStateToWord(s)), s);
}

/// ---------- codec round-trip ----------

TEST(CASRefCatalogFormat, RoundTripsAllThreeStates)
{
    RefCatalog in;
    in.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating,
        .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv1", .writer_epoch = 5, .fence_generation = 2}});
    in.entries.push_back(liveEntry("b", 2));
    in.entries.push_back(CatalogEntry{
        .ns = RootNamespace{"c"},
        .state = NsState::Removing,
        .incarnation = UInt128(3),
        .removal_started_round = 11});

    const RefCatalog out = decodeRefCatalog(encodeRefCatalog(in));
    EXPECT_EQ(out, in);
    EXPECT_EQ(out.entries[0].state, NsState::Creating);
    EXPECT_EQ(out.entries[1].state, NsState::Live);
    EXPECT_EQ(out.entries[2].state, NsState::Removing);
}

/// Mutation caught: making removal age caller-local or optional would let an adopted `Removing` row
/// lose the immutable round from which stuck-removal diagnostics measure.
TEST(CASRefCatalogFormat, RemovalStartedRoundIsRequiredExactlyForRemoving)
{
    CatalogEntry removing{
        .ns = RootNamespace{"removing"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 19};
    const RefCatalog catalog{.entries = {removing}};
    const String encoded = encodeRefCatalog(catalog);
    EXPECT_NE(encoded.find("\"remove_round\":\"19\""), String::npos);
    EXPECT_NE(encoded.find("\"state\":\"removing\""), String::npos);
    EXPECT_EQ(decodeRefCatalog(encoded), catalog);

    const String inc = "00000000000000000000000000000009";
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
        [&] { (void)decodeRefCatalog(rawCatalog({rawEntryLine("missing", "removing", inc)})); });
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        (void)decodeRefCatalog(rawCatalog({withRemovalStartedRound(rawEntryLine("forbidden", "live", inc), 21)}));
    });
}

TEST(CASRefCatalogFormat, EmptyCatalogRoundTrips)
{
    EXPECT_EQ(decodeRefCatalog(encodeRefCatalog(RefCatalog{})), RefCatalog{});
}

/// Mutation caught: replacing the reverse index with `emplace`-and-ignore would make the first row
/// win. Every lifecycle state participates, both duplicate ids are unresolvable, and an unrelated
/// unique row remains usable by point resolution.
TEST(CASRefCatalogLifeIndex, DuplicatePhysicalIdsAreAmbiguousWithoutPoisoningUniquePointResolution)
{
    RefCatalog catalog;
    catalog.entries = {
        entryInState("a-creating", NsState::Creating, 7),
        entryInState("b-live", NsState::Live, 7),
        entryInState("c-removing", NsState::Removing, 8),
        entryInState("d-live", NsState::Live, 8),
        entryInState("e-unique", NsState::Live, 9),
    };

    const CatalogLifeIndex index(catalog);
    EXPECT_TRUE(index.isAmbiguous(UInt128{7}));
    EXPECT_TRUE(index.isAmbiguous(UInt128{8}));
    EXPECT_THROW(index.resolve(UInt128{7}), DB::Exception);
    EXPECT_THROW(index.resolve(UInt128{8}), DB::Exception);
    const auto unique = index.resolve(UInt128{9});
    ASSERT_TRUE(unique);
    EXPECT_EQ(unique->ns.string(), "e-unique");
}

/// Catalog mutation is destructive authority: any ambiguous current id stops the mutation before a
/// candidate can be written. An unrelated unique point lookup remains available from the same cut.
TEST(CASRefCatalogLifeIndex, AmbiguityStopsCatalogMutationButNotUnrelatedPointLookup)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout("p");
    RefCatalog catalog;
    catalog.entries = {
        entryInState("a", NsState::Live, 7),
        entryInState("b", NsState::Removing, 7),
        entryInState("c", NsState::Live, 9),
    };
    seedObject(op, layout.refCatalogKey(), encodeRefCatalog(catalog));
    const auto before = op.read(layout.refCatalogKey(), Retry::standard());
    ASSERT_TRUE(before);

    EXPECT_THROW(CasRefCatalog::casUpdate(op, layout, [](const RefCatalog & current) { return current; }), DB::Exception);
    const auto after = op.read(layout.refCatalogKey(), Retry::standard());
    ASSERT_TRUE(after);
    EXPECT_EQ(after->etag, before->etag);
    EXPECT_EQ(after->bytes, before->bytes);

    const auto unique = CasRefCatalog::lifeIfCataloged(op, layout, RootNamespace{"c"});
    ASSERT_TRUE(unique);
    EXPECT_EQ(unique->incarnation, UInt128{9});
}

TEST(CASRefCatalogFormat, NamespaceAtExactByteBoundRoundTrips)
{
    RefCatalog c;
    c.entries.push_back(liveEntry(String(kMaxNamespaceBytes, 'a'), 1));
    const RefCatalog out = decodeRefCatalog(encodeRefCatalog(c));
    EXPECT_EQ(out, c);
}

/// ---------- strict rejections: encode side (LOGICAL_ERROR -- our own state, not yet durable) ----------

/// Every `expectThrowsCode(LOGICAL_ERROR, ...)` in this block aborts the process in debug/sanitizer
/// builds instead of behaving like a catchable exception (`Common/Exception.cpp`'s
/// `handle_error_code`), so each test is split: the throw-and-catch form below runs only on a plain
/// release build, and its `...DeathTest` counterpart (grouped after this block) proves the abort
/// positively on debug/sanitizer builds instead.
#ifndef DEBUG_OR_SANITIZER_BUILD

TEST(CASRefCatalogFormat, EncodeRejectsDuplicateNamespace)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 1));
    c.entries.push_back(liveEntry("a", 2));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsNonCanonicalOrder)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("b", 1));
    c.entries.push_back(liveEntry("a", 2));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsCreatorPresentOnLive)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsCreatorAbsentOnCreating)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating, .incarnation = UInt128(1)});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsZeroIncarnation)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 0));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsNameOverByteBound)
{
    RefCatalog c;
    c.entries.push_back(liveEntry(String(kMaxNamespaceBytes + 1, 'a'), 1));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

TEST(CASRefCatalogFormat, EncodeRejectsEmptyNamespace)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("", 1));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeRefCatalog(c); });
}

/// Mutation caught: making removal age caller-local or optional would let an adopted `Removing` row
/// lose the immutable round from which stuck-removal diagnostics measure.
TEST(CASRefCatalogFormat, EncodeRejectsLiveWithRemovalStartedRound)
{
    CatalogEntry live_with_round = liveEntry("live", 8);
    live_with_round.removal_started_round = 20;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { (void)encodeRefCatalog(RefCatalog{.entries = {live_with_round}}); });
}

#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsDuplicateNamespaceAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 1));
    c.entries.push_back(liveEntry("a", 2));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "not canonically ordered");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsNonCanonicalOrderAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("b", 1));
    c.entries.push_back(liveEntry("a", 2));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "not canonically ordered");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsCreatorPresentOnLiveAborts)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Live, .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}});
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "carries a creator fence");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsCreatorAbsentOnCreatingAborts)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating, .incarnation = UInt128(1)});
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "lacks a creator fence");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsZeroIncarnationAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("a", 0));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "zero incarnation");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsNameOverByteBoundAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry(String(kMaxNamespaceBytes + 1, 'a'), 1));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "admission bound");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsEmptyNamespaceAborts)
{
    RefCatalog c;
    c.entries.push_back(liveEntry("", 1));
    EXPECT_DEATH({ (void)encodeRefCatalog(c); }, "namespace must not be empty");
}

TEST(CASRefCatalogFormatDeathTest, EncodeRejectsLiveWithRemovalStartedRoundAborts)
{
    CatalogEntry live_with_round = liveEntry("live", 8);
    live_with_round.removal_started_round = 20;
    EXPECT_DEATH(
        { (void)encodeRefCatalog(RefCatalog{.entries = {live_with_round}}); }, "removal_started_round");
}

#endif

/// A namespace + creator server_root_id that both max out at their respective byte bounds (512 +
/// 255), escaped worst-case, land one `entry` line over the 4 KiB line cap (~4.7 KiB) -- reachable
/// because neither this codec nor `validateServerRootId` restricts the charset, only the length.
/// The refusal must be `LIMIT_EXCEEDED` (a capacity refusal), not `LOGICAL_ERROR` (a bug report).
TEST(CASRefCatalogFormat, EncodeLineOverCapRaisesLimitExceeded)
{
    RefCatalog c;
    c.entries.push_back(CatalogEntry{
        .ns = RootNamespace{String(kMaxNamespaceBytes, '\x01')},
        .state = NsState::Creating,
        .incarnation = UInt128(1),
        .creator = CreatorFence{.server_root_id = String(255, '\x01'), .writer_epoch = 1, .fence_generation = 1}});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED, [&] { encodeRefCatalog(c); });
}

/// ---------- strict rejections: decode side (CORRUPTED_DATA -- bytes may have come from anywhere) ----------

TEST(CASRefCatalogFormat, DecodeRejectsDuplicateNamespace)
{
    const String bad = rawCatalog({rawEntryLine("a", "live", u128ToHex(UInt128(1))),
                                    rawEntryLine("a", "live", u128ToHex(UInt128(2)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsNonCanonicalOrder)
{
    const String bad = rawCatalog({rawEntryLine("b", "live", u128ToHex(UInt128(1))),
                                    rawEntryLine("a", "live", u128ToHex(UInt128(2)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsCreatorPresentOnLive)
{
    const String bad = rawCatalog({rawEntryLine("a", "live", u128ToHex(UInt128(1)),
                                               std::make_tuple(String("srv"), uint64_t(1), uint64_t(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsCreatorAbsentOnCreating)
{
    const String bad = rawCatalog({rawEntryLine("a", "creating", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsZeroIncarnation)
{
    const String bad = rawCatalog({rawEntryLine("a", "live", u128ToHex(UInt128(0)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsNameOverByteBound)
{
    const String too_long_ns(kMaxNamespaceBytes + 1, 'a');
    const String bad = rawCatalog({rawEntryLine(too_long_ns, "live", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsUnknownState)
{
    const String bad = rawCatalog({rawEntryLine("a", "bogus", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsUnknownEntryKey)
{
    const String bad = rawCatalog(
        {R"({"kind":"entry","ns":"a","state":"live","life":"00000000000000000000000000000001","unknown":"x"})"});
    try
    {
        (void)decodeRefCatalog(bad);
        FAIL() << "expected CORRUPTED_DATA";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA);
        EXPECT_NE(e.message().find("unknown entry key"), String::npos) << e.message();
    }
}

TEST(CASRefCatalogFormat, DecodeRejectsEmptyNamespace)
{
    const String bad = rawCatalog({rawEntryLine("", "live", u128ToHex(UInt128(1)))});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

TEST(CASRefCatalogFormat, DecodeRejectsMissingNamespaceKey)
{
    /// No "ns" key at all -- must be refused exactly like an explicit empty one, not read as "".
    const String bad = rawCatalog({R"({"kind":"entry","state":"live","life":")" + u128ToHex(UInt128(1)) + "\"}"});
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeRefCatalog(bad); });
}

/// `nsStateToWord`'s only reachable input is either a live `NsState` or one `nsStateFromWord` already
/// validated on decode, so an unrecognized value is a bug in THIS process -- `LOGICAL_ERROR`, matching
/// this file's own stated taxonomy for the encode-side helper it (indirectly, via `creatorPairingOk`'s
/// error message) serves. Aborts under debug/sanitizer builds -- split like the block above;
/// `CASRefCatalogFormatDeathTest.NsStateToWordRaisesLogicalErrorOnImpossibleValueAborts` covers it there.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalogFormat, NsStateToWordRaisesLogicalErrorOnImpossibleValue)
{
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { nsStateToWord(static_cast<NsState>(99)); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogFormatDeathTest, NsStateToWordRaisesLogicalErrorOnImpossibleValueAborts)
{
    EXPECT_DEATH({ (void)nsStateToWord(static_cast<NsState>(99)); }, "outside the wire vocabulary"); // NOLINT(clang-analyzer-optin.core.EnumCastOutOfRange): the whole point of this test is an impossible enum value
}
#endif

/// ---------- registry row / raw-storage tripwire ----------

/// The registry row is part of the contract, mirroring `gtest_cas_ref_ckpt.cpp`'s
/// `RegistryRowIsControlStrictWithTightCaps`: Control/Strict decides how the decoder treats unknown
/// keys, and the caps are the first thing that fires if a foreign object ever lands at the key.
TEST(CASRefCatalogFormat, RegistryRowIsControlStrictWithRawStorage)
{
    const FormatTraits & traits = traitsFor(FormatId::RefCatalog);
    EXPECT_EQ(traits.type, "cas_ref_catalog");
    EXPECT_EQ(traits.family, TextFamily::Control);
    EXPECT_EQ(traits.strictness, KeyStrictness::Strict);
    EXPECT_EQ(traits.object_cap, 256u * 1024u * 1024u);
    EXPECT_EQ(traits.line_cap, 4u * 1024u);
    EXPECT_EQ(traitsForType("cas_ref_catalog"), &traits);
    /// Raw, so the key has no suffix: the catalog hands bytes directly to/from the backend,
    /// bypassing `sealObject`/`openObject` because both are the identity under
    /// `CompressionPolicy::Never`. This line is the TRIPWIRE for that shortcut -- a policy flip to
    /// `Always` would silently write uncompressed bodies under a `.zst` key, which this assertion
    /// catches first.
    EXPECT_EQ(storedSuffix(FormatId::RefCatalog), "");
    EXPECT_EQ(traits.compression, CompressionPolicy::Never);
}

/// ---------- capacity admission: per-predicate boundary tests ----------

TEST(CASRefCatalogAdmission, Predicate1AcceptsEqualityRefusesCapPlusOne)
{
    const uint64_t cap = traitsFor(FormatId::RefCatalog).object_cap;
    const RootNamespace ns{"admitted"};
    EXPECT_NO_THROW(checkCatalogObjectBytes(cap, ns));
    try
    {
        checkCatalogObjectBytes(cap + 1, ns);
        FAIL() << "expected LIMIT_EXCEEDED";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::LIMIT_EXCEEDED);
        EXPECT_NE(e.message().find("predicate 1"), String::npos) << e.message();
        EXPECT_NE(e.message().find(ns.string()), String::npos) << e.message();
    }
}

TEST(CASRefCatalogAdmission, AdmissionReturnsTheCheckedBytes)
{
    /// For an ordinary small catalog `checkCatalogAdmission` holds slack and returns the exact bytes
    /// `encodeRefCatalog` would produce.
    RefCatalog candidate;
    candidate.entries.push_back(liveEntry("a", 1));
    candidate.entries.push_back(liveEntry("b", 2));
    const String encoded = checkCatalogAdmission(candidate, RootNamespace{"b"});
    EXPECT_EQ(encoded, encodeRefCatalog(candidate));
}

/// ---------- Pool/CasRefCatalog: read / create / update / conflict-retry ----------

TEST(CASRefCatalog, ReadAbsentFailsClosed)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    DB::Cas::tests::expectThrowsCode(
        DB::ErrorCodes::CORRUPTED_DATA, [&] { (void)CasRefCatalog::read(op, layout); });
}

TEST(CASRefCatalog, CasUpdateRefusesWhenAbsent)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        CasRefCatalog::casUpdate(op, layout, [](const RefCatalog & cur) { return cur; });
    });
    EXPECT_FALSE(op.head(layout.refCatalogKey(), Retry::standard()).has_value());
}

TEST(CASRefCatalog, CasUpdateAppliesOnTopOfExistingState)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));

    const RefCatalog updated = CasRefCatalog::casUpdate(op, layout, [](const RefCatalog & cur)
    {
        RefCatalog next = cur;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });
    ASSERT_EQ(updated.entries.size(), 1u);
    EXPECT_EQ(updated.entries[0].ns.string(), "a");
    EXPECT_EQ(updated.entries[0].state, NsState::Removing);
}

/// `CasRefCatalog::casUpdate`'s identity-preserving refusal throws `LOGICAL_ERROR`, which aborts the
/// whole process in debug/sanitizer builds (`Common/Exception.cpp`'s `handle_error_code`) instead of
/// behaving like a catchable exception -- so the throw-and-catch form below runs only on a plain
/// release build, and `CASRefCatalogDeathTest.GenericCasUpdateCannotDeleteOrReplaceCatalogIdentityAborts`
/// proves the abort positively on debug/sanitizer builds instead.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, GenericCasUpdateCannotDeleteOrReplaceCatalogIdentity)
{
    const Layout layout("p");
    {
        auto backend = std::make_shared<InMemoryBackend>();
        CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
        CasOperation op = requests.admit();
        CasRefCatalog::initializeEmptyForNewPool(op, layout);
        CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
        DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
        {
            (void)CasRefCatalog::casUpdate(op, layout, [](const RefCatalog &) { return RefCatalog{}; });
        });
    }

    {
        auto backend = std::make_shared<InMemoryBackend>();
        CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
        CasOperation op = requests.admit();
        CasRefCatalog::initializeEmptyForNewPool(op, layout);
        CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
        DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&]
        {
            (void)CasRefCatalog::casUpdate(op, layout, [](const RefCatalog & current)
            {
                RefCatalog next = current;
                next.entries[0] = liveEntry("b", 2);
                return next;
            });
        });
    }
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, GenericCasUpdateCannotDeleteOrReplaceCatalogIdentityAborts)
{
    const Layout layout("p");
    {
        auto backend = std::make_shared<InMemoryBackend>();
        CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
        CasOperation op = requests.admit();
        CasRefCatalog::initializeEmptyForNewPool(op, layout);
        CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
        EXPECT_DEATH(
            { (void)CasRefCatalog::casUpdate(op, layout, [](const RefCatalog &) { return RefCatalog{}; }); },
            "cannot add or delete catalog entries");
    }

    {
        auto backend = std::make_shared<InMemoryBackend>();
        CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
        CasOperation op = requests.admit();
        CasRefCatalog::initializeEmptyForNewPool(op, layout);
        CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
        EXPECT_DEATH(
            {
                (void)CasRefCatalog::casUpdate(op, layout, [](const RefCatalog & current)
                {
                    RefCatalog next = current;
                    next.entries[0] = liveEntry("b", 2);
                    return next;
                });
            },
            "cannot replace catalog identity");
    }
}
#endif

TEST(CASRefCatalog, CasUpdateRetriesOnConflictAgainstFreshState)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));

    backend->refuseNextWrite(layout.refCatalogKey());   /// one-shot artificial Conflict on the next write

    int mutate_calls = 0;
    const RefCatalog result = CasRefCatalog::casUpdate(op, layout, [&](const RefCatalog & cur)
    {
        ++mutate_calls;
        RefCatalog next = cur;
        next.entries[0].state = NsState::Removing;
        next.entries[0].removal_started_round = 1;
        return next;
    });

    EXPECT_EQ(mutate_calls, 2);   /// first attempt hit the injected conflict; the retry succeeded
    ASSERT_EQ(result.entries.size(), 1u);
    EXPECT_EQ(result.entries[0].state, NsState::Removing);

    const CasRefCatalog::Snapshot snap = CasRefCatalog::read(op, layout);
    EXPECT_EQ(snap.catalog, result);
}

TEST(CASRefCatalog, BeginRemovingRechecksAdmissionAfterACatalogConflict)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation reader = requests.admit();
    const Layout layout("p");
    const CatalogEntry observed = liveEntry("a", 1);
    CasRefCatalog::initializeEmptyForNewPool(reader, layout);
    CasRefCatalog::casAdmitEntry(reader, layout, observed);
    const uint64_t writes_before = backend->writes(layout.refCatalogKey());

    /// The transition's one attempt is refused, and this actor's admission is withdrawn as it is sent.
    /// The refusal must end the call rather than start another attempt, and it must be reported as an
    /// outcome rather than thrown.
    bool admitted = true;
    backend->refuseNextWrite(layout.refCatalogKey());
    backend->onBeforeWrite(layout.refCatalogKey(), [&admitted] { admitted = false; });
    CasOperation op = requests.admit([&admitted] { return admitted; });

    const auto outcome = CasRefCatalog::beginRemoving(op, layout, observed);

    EXPECT_EQ(outcome, CasRefCatalog::BeginRemovingOutcome::FencedOut);
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), writes_before + 1)
        << "the refused attempt must not be followed by another";
    const CasRefCatalog::Snapshot after = CasRefCatalog::read(reader, layout);
    EXPECT_EQ(after.catalog.entries, std::vector<CatalogEntry>{observed})
        << "nothing may be written after the admission is gone";
}

/// A re-read that finds the catalog genuinely ABSENT after it was previously observed present is a
/// real concurrent delete, not a bootstrap -- `casUpdate` must refuse rather than silently create a
/// fresh catalog containing only this one mutation's entry (which would drop every other namespace).
/// Reproduced with a REAL delete (no fault injection needed): `mutate`'s first invocation deletes the
/// seeded object at the incarnation the update's own initial read observed, so its conditional write
/// is refused and the read that settles the refusal genuinely finds the key absent.
/// Missing mandatory authority raises `CORRUPTED_DATA`; the split remains only because the debug
/// variant historically lived in the death-test suite.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, CasUpdateThrowsOnVanishMidRetryInsteadOfReplacingTheCatalog)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
    const CasRefCatalog::Snapshot seeded = CasRefCatalog::read(op, layout);
    ASSERT_TRUE(seeded.etag.has_value());

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        CasRefCatalog::casUpdate(op, layout, [&](const RefCatalog & cur)
        {
            EXPECT_EQ(op.remove(layout.refCatalogKey(), *seeded.etag, Retry::standard()), Removal::Removed);
            RefCatalog next = cur;
            next.entries[0].state = NsState::Removing;
            next.entries[0].removal_started_round = 1;
            return next;
        });
    });

    /// Nothing was written by the failed attempt: the object is exactly as the delete left it
    /// (absent), never a fresh single-entry catalog.
    EXPECT_FALSE(op.head(layout.refCatalogKey(), Retry::standard()).has_value());
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, CasUpdateThrowsOnVanishMidRetryInsteadOfReplacingTheCatalogAborts)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
    const CasRefCatalog::Snapshot seeded = CasRefCatalog::read(op, layout);
    ASSERT_TRUE(seeded.etag.has_value());

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
            CasRefCatalog::casUpdate(op, layout, [&](const RefCatalog & cur)
            {
                EXPECT_EQ(op.remove(layout.refCatalogKey(), *seeded.etag, Retry::standard()), Removal::Removed);
                RefCatalog next = cur;
                next.entries[0].state = NsState::Removing;
                next.entries[0].removal_started_round = 1;
                return next;
            });
    });
}
#endif

/// Persistent contention ends at the write policy's DEADLINE, with the typed retryable error -- not
/// after a fixed number of unslept iterations, and not in an infinite spin. `mutate` re-arms the
/// one-shot conflict injection on every call, so every attempt is refused; the injected clock reaches
/// the deadline without the test sleeping at all.
TEST(CASRefCatalog, CasUpdateEndsAtTheDeadlineNotAfterAHundredUnsleptIterations)
{
    auto backend = std::make_shared<InMemoryBackend>();
    DB::Cas::tests::FakeClock clock;
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    requests.setNowFnForTest(clock.nowFn());
    requests.setSleepFnForTest(clock.sleepFn());
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    const uint64_t started_at = clock.now;

    int mutate_calls = 0;
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&]
    {
        CasRefCatalog::casUpdate(op, layout, [&](const RefCatalog & cur)
        {
            ++mutate_calls;
            backend->refuseNextWrite(layout.refCatalogKey());
            RefCatalog next = cur;
            return next;
        });
    });
    EXPECT_GT(mutate_calls, 1);   /// genuinely retried, not a single-shot failure
    EXPECT_FALSE(clock.sleeps.empty()) << "every retry must back off; an unslept loop would burn the "
                                          "deadline on requests instead of waiting out the contention";
    EXPECT_GE(clock.now - started_at, Retry::standard().window_ms - 5000)
        << "the loop ended at the policy's own deadline, not at an iteration count";
}

TEST(CASRefCatalog, CasAdmitEntryAcceptsAnOrdinaryCreation)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);

    const RefCatalog created = CasRefCatalog::casAdmitEntry(op, layout,
        CatalogEntry{.ns = RootNamespace{"a"}, .state = NsState::Creating, .incarnation = UInt128(1),
            .creator = CreatorFence{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1}});
    ASSERT_EQ(created.entries.size(), 1u);
    EXPECT_EQ(created.entries[0].state, NsState::Creating);
}

TEST(CASRefCatalog, CasAdmitEntryInsertsAtCanonicalPosition)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);

    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("b", 1));
    const RefCatalog after = CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 2));
    ASSERT_EQ(after.entries.size(), 2u);
    EXPECT_EQ(after.entries[0].ns.string(), "a");   /// inserted BEFORE "b", not appended
    EXPECT_EQ(after.entries[1].ns.string(), "b");
}

/// Caught by `encodeRefCatalog`'s own canonical-order/no-duplicate grammar check, inside
/// `checkCatalogAdmission` -- no separate duplicate check needed here. That `LOGICAL_ERROR` aborts
/// under debug/sanitizer builds -- split like the blocks above.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, CasAdmitEntryRejectsADuplicateNamespace)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 2)); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, CasAdmitEntryRejectsADuplicateNamespaceAborts)
{
    auto backend = std::make_shared<InMemoryBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 1));
    EXPECT_DEATH({ CasRefCatalog::casAdmitEntry(op, layout, liveEntry("a", 2)); }, "not canonically ordered");
}
#endif

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalogRemoval, AdmissionCannotCarryRemoval)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { (void)CasRefCatalog::casAdmitEntry(op, layout, removing); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogRemovalDeathTest, AdmissionCannotCarryRemovalAborts)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout("p");
    CasRefCatalog::initializeEmptyForNewPool(op, layout);
    const CatalogEntry removing{
        .ns = RootNamespace{"a"},
        .state = NsState::Removing,
        .incarnation = UInt128{7},
        .removal_started_round = 13};

    EXPECT_DEATH(
        { (void)CasRefCatalog::casAdmitEntry(op, layout, removing); },
        "cannot admit namespace.*directly as Removing");
}
#endif

TEST(CASRefCatalogRemoval, CancelStalledCreatingRequiresExactRowAndTerminalCreatorFence)
{
    auto backend = std::make_shared<WriteCountingBackend>();
    CasRequests requests = DB::Cas::tests::openRequestsForTest(backend);
    CasOperation op = requests.admit();
    const Layout layout("p");
    const CatalogEntry creating{
        .ns = RootNamespace{"a"},
        .state = NsState::Creating,
        .incarnation = UInt128{7},
        .creator = CreatorFence{.server_root_id = "server", .writer_epoch = 3, .fence_generation = 4}};
    seedObject(op, layout.refCatalogKey(), encodeRefCatalog(RefCatalog{.entries = {creating}}));
    const uint64_t writes_after_seed = backend->writes(layout.refCatalogKey());

    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(
        op, layout, creating, [](const CreatorFence &) { return false; }),
        CasRefCatalog::StalledCreatingCancelOutcome::CreatorFenceStillLive);
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), writes_after_seed);

    CatalogEntry stale = creating;
    stale.creator->writer_epoch = 2;
    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(
        op, layout, stale, [](const CreatorFence &) { return true; }),
        CasRefCatalog::StalledCreatingCancelOutcome::EntryChanged);
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), writes_after_seed);

    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(
        op, layout, creating, [](const CreatorFence &) { return true; }),
        CasRefCatalog::StalledCreatingCancelOutcome::Cancelled);
    EXPECT_TRUE(CasRefCatalog::read(op, layout).catalog.entries.empty());
    EXPECT_EQ(backend->writes(layout.refCatalogKey()), writes_after_seed + 1);
    EXPECT_EQ(backend->listTotal(), 0u);
    EXPECT_EQ(backend->deleteTotal(), 0u);
}

/// ---------- Pool/CasRefCatalog: mutations through the pool's hot-key lane ----------

namespace ProfileEvents
{
    extern const Event CASHotKeyReadStarts;
    extern const Event CASHotKeyCacheVerdictsReread;
    extern const Event CASRequestConflictPause;
    extern const Event CASRequestReissue;
}

namespace
{

#if USE_AWS_S3
std::exception_ptr s3Error(Aws::S3::S3Errors code, const String & name)
{
    return std::make_exception_ptr(DB::S3Exception("the store answered " + name, code, name));
}
#endif

/// A pool's own engine: a `CasRequests` over the pool's shared lane with a cache, on a fake clock,
/// beside a plain `CasRequests` that stands for another server.
struct PoolAndExternal
{
    explicit PoolAndExternal(std::shared_ptr<CountingBackend> backend_)
        : backend(std::move(backend_))
        , hot_keys(16ULL << 20)
        , pool(backend, Fence::open(), clock.nowFn(), clock.sleepFn(), &hot_keys)
        , external(DB::Cas::tests::openRequestsForTest(backend))
    {
    }
    std::shared_ptr<CountingBackend> backend;
    FakeClock clock;
    CasHotKeys hot_keys;
    CasRequests pool;
    CasRequests external;
};

uint64_t eventCount(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event].load();
}

}

TEST(CASRefCatalog, AStaleHintNeverProducesAFalseEntryChanged)
{
    /// The pool's last catalog write left "a" Creating; another server completed it to Live. The
    /// caller reads fresh, sees Live, and calls beginRemoving: the cached Creating row differs from
    /// what it observed and would say EntryChanged. That verdict is not delivered.
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry creating = entryInState("a", NsState::Creating, 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, creating);   /// through the lane: the cache holds Creating

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::completeCreation(other, layout, creating), CasRefCatalog::NamespaceCreationOutcome::Live);

    const CatalogEntry observed = CasRefCatalog::read(other, layout).catalog.entries.at(0);
    ASSERT_EQ(observed.state, NsState::Live);
    const uint64_t reads_before = f.backend->getCount(layout.refCatalogKey());
    const uint64_t rereads_before = eventCount(ProfileEvents::CASHotKeyCacheVerdictsReread);
    EXPECT_EQ(CasRefCatalog::beginRemoving(pool_op, layout, observed), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    EXPECT_EQ(f.backend->getCount(layout.refCatalogKey()) - reads_before, 1u) << "one lane read";
    EXPECT_EQ(eventCount(ProfileEvents::CASHotKeyCacheVerdictsReread) - rereads_before, 1u);
    EXPECT_EQ(CasRefCatalog::read(other, layout).catalog.entries.at(0).state, NsState::Removing);
}

TEST(CASRefCatalog, ATrueRefusalOnTheReadIsTheSameAsWithoutACache)
{
    /// The row really changed: the re-rendered verdict is the one a cache-less call renders.
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry creating = entryInState("a", NsState::Creating, 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, creating);

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::completeCreation(other, layout, creating), CasRefCatalog::NamespaceCreationOutcome::Live);
    const uint64_t writes_before = f.backend->writeCount(layout.refCatalogKey());
    const uint64_t reads_before = f.backend->getCount(layout.refCatalogKey());
    /// The caller believes it observed a Creating row under another incarnation. The pool's hint
    /// (Creating, incarnation 1) differs from it, so the verdict is rendered on the hint and not
    /// delivered; the read (Live, incarnation 1) differs from it too, and that verdict is the answer,
    /// the one a cache-less call renders: one read, no write. (Had the hint matched `observed`, the
    /// write path would run instead: a 412 and a resolve read, which the lane tests cover.)
    const CatalogEntry observed_elsewhere = entryInState("a", NsState::Creating, 2);
    EXPECT_EQ(CasRefCatalog::cancelStalledCreating(pool_op, layout, observed_elsewhere, [](const CreatorFence &) { return true; }),
              CasRefCatalog::StalledCreatingCancelOutcome::EntryChanged);
    EXPECT_EQ(f.backend->writeCount(layout.refCatalogKey()), writes_before) << "no write";
    EXPECT_EQ(f.backend->getCount(layout.refCatalogKey()) - reads_before, 1u) << "one lane read";
}

/// createNamespaceStep1 via createNamespace: the cache holds row "a" (Live), the external erased it
/// via a completed removal.
TEST(CASRefCatalog, AStaleHintCreateNamespaceStep1AdmitsANewIncarnation)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    const CreatorFence creator{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1};
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    ASSERT_EQ(CasRefCatalog::createNamespace(pool_op, layout, RootNamespace{"a"}, creator),
              CasRefCatalog::NamespaceCreationOutcome::Live);   /// the cache now holds "a" Live

    CasOperation other = f.external.admit();
    const CatalogEntry observed = CasRefCatalog::read(other, layout).catalog.entries.at(0);
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, observed), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    eraseRowRaw(other, layout, observed.ns);

    const uint64_t reads_before = f.backend->getCount(layout.refCatalogKey());
    EXPECT_EQ(CasRefCatalog::createNamespace(pool_op, layout, RootNamespace{"a"}, creator),
              CasRefCatalog::NamespaceCreationOutcome::Live);
    /// Two reads, not one: `createNamespace`'s own pre-check (`read`) never goes through the lane, so
    /// it always issues its own request regardless of the cache; the lane's ONE read is the one
    /// `createNamespaceStep1` pays for finding its cached hint stale.
    EXPECT_EQ(f.backend->getCount(layout.refCatalogKey()) - reads_before, 2u) << "the pre-check read plus one lane read";
}

/// createNamespaceStep1: the store holds "a" Creating under another creator -- Superseded on the read.
TEST(CASRefCatalog, ATrueRefusalCreateNamespaceStep1SeesASuperseder)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    const CreatorFence creator{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1};
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    ASSERT_EQ(CasRefCatalog::createNamespace(pool_op, layout, RootNamespace{"a"}, creator),
              CasRefCatalog::NamespaceCreationOutcome::Live);

    CasOperation other = f.external.admit();
    const CatalogEntry observed = CasRefCatalog::read(other, layout).catalog.entries.at(0);
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, observed), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    eraseRowRaw(other, layout, observed.ns);
    /// The store now carries no row for "a"; the pool's cache still holds its own stale "a" Live from
    /// the first `createNamespace` above. `createNamespace`'s own pre-check read is unconditional and
    /// always fresh, so if the rival lands its row BEFORE this call, the pre-check alone would already
    /// return `Superseded` and step 1 -- the lane -- would never run. The rival instead lands INSIDE
    /// the window `setCreateNamespaceStep1PreReadHookForTest` names: after the pre-check already saw
    /// nothing, but before step 1's own (lane) read. `createNamespaceStep1`'s decide then runs first
    /// against the CACHED hint (still "a" Live) -- which already has a row for "a", so it throws
    /// immediately without ever reaching the store -- and that verdict-on-a-hint is dropped: the lane
    /// re-reads for real and decides again, this time seeing the rival's fresh row, and throws again.
    /// That second throw is the one that reaches `createNamespace`.
    const CreatorFence other_creator{.server_root_id = "other", .writer_epoch = 1, .fence_generation = 1};
    CatalogEntry other_creating = entryInState("a", NsState::Creating, 5);
    other_creating.creator = other_creator;
    CasRefCatalog::setCreateNamespaceStep1PreReadHookForTest([&]
    {
        CasRefCatalog::casAdmitEntry(other, layout, other_creating);
    });

    /// The hook's own admission is a real write on the SAME shared backend the pool uses, so a raw
    /// backend read/write count taken across this call would count the rival's own IO alongside the
    /// lane's -- `CASHotKeyCacheVerdictsReread` does not: it increments only when a CACHED hint's
    /// verdict is dropped and re-decided, which the rival's cache-less admission (its own private,
    /// budget-0 `CasHotKeys`) never does, so it isolates the lane's own contribution cleanly.
    const uint64_t rereads_before = eventCount(ProfileEvents::CASHotKeyCacheVerdictsReread);
    EXPECT_EQ(CasRefCatalog::createNamespace(pool_op, layout, RootNamespace{"a"}, creator),
              CasRefCatalog::NamespaceCreationOutcome::Superseded);
    EXPECT_EQ(eventCount(ProfileEvents::CASHotKeyCacheVerdictsReread) - rereads_before, 1u)
        << "one lane read: the cached hint's verdict was dropped and redecided on a fresh read";

    /// No write of ours landed: the row for "a" is exactly what the rival left it as.
    const CasRefCatalog::Snapshot snap_after = CasRefCatalog::read(other, layout);
    const CatalogEntry * after = &snap_after.catalog.entries.at(0);
    EXPECT_EQ(after->state, NsState::Creating);
    ASSERT_TRUE(after->creator.has_value());
    EXPECT_EQ(*after->creator, other_creator);
    EXPECT_EQ(after->incarnation, other_creating.incarnation);
}

/// completeCreation: the cache holds "a" Creating (incarnation 1) matching the store's row for "a",
/// but the store also gained row "b" -- the cache is stale only elsewhere, so the write goes through.
TEST(CASRefCatalog, AStaleHintCompleteCreationLandsWhenItsOwnRowIsUnchanged)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    const CreatorFence creator{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1};
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a_creating = entryInState("a", NsState::Creating, 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a_creating);   /// cache: "a" Creating

    CasOperation other = f.external.admit();
    const CreatorFence other_creator{.server_root_id = "other", .writer_epoch = 1, .fence_generation = 1};
    ASSERT_EQ(CasRefCatalog::createNamespace(other, layout, RootNamespace{"b"}, other_creator),
              CasRefCatalog::NamespaceCreationOutcome::Live);

    EXPECT_EQ(CasRefCatalog::completeCreation(pool_op, layout, a_creating), CasRefCatalog::NamespaceCreationOutcome::Live);
    EXPECT_EQ(CasRefCatalog::read(other, layout).catalog.entries.size(), 2u);
}

/// completeCreation: "a" was completed to Live by the external -- Superseded on the read.
TEST(CASRefCatalog, ATrueRefusalCompleteCreationSeesItsOwnRowCompletedElsewhere)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a_creating = entryInState("a", NsState::Creating, 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a_creating);

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::completeCreation(other, layout, a_creating), CasRefCatalog::NamespaceCreationOutcome::Live);

    EXPECT_EQ(CasRefCatalog::completeCreation(pool_op, layout, a_creating), CasRefCatalog::NamespaceCreationOutcome::Superseded);
}

/// reconcileStaleCreator: the cache holds "a" Creating under creator X; the store's row for "a" is
/// unchanged but the catalog gained "b" -- Reconciled.
TEST(CASRefCatalog, AStaleHintReconcileStaleCreatorLandsWhenItsOwnRowIsUnchanged)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a_creating = entryInState("a", NsState::Creating, 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a_creating);   /// cache: "a" Creating under creator X

    CasOperation other = f.external.admit();
    const CreatorFence other_creator{.server_root_id = "other", .writer_epoch = 1, .fence_generation = 1};
    ASSERT_EQ(CasRefCatalog::createNamespace(other, layout, RootNamespace{"b"}, other_creator),
              CasRefCatalog::NamespaceCreationOutcome::Live);

    const CreatorFence new_creator{.server_root_id = "srv2", .writer_epoch = 1, .fence_generation = 1};
    EXPECT_EQ(CasRefCatalog::reconcileStaleCreator(pool_op, layout, a_creating, new_creator,
                  [](const CreatorFence &) { return true; }),
              CasRefCatalog::ReconcileCreatorOutcome::Reconciled);
    EXPECT_EQ(CasRefCatalog::read(other, layout).catalog.entries.size(), 2u);
}

/// reconcileStaleCreator: "a" was completed to Live by the external -- EntryChanged on the read.
TEST(CASRefCatalog, ATrueRefusalReconcileStaleCreatorSeesItsOwnRowCompletedElsewhere)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a_creating = entryInState("a", NsState::Creating, 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a_creating);

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::completeCreation(other, layout, a_creating), CasRefCatalog::NamespaceCreationOutcome::Live);

    const CreatorFence new_creator{.server_root_id = "srv2", .writer_epoch = 1, .fence_generation = 1};
    EXPECT_EQ(CasRefCatalog::reconcileStaleCreator(pool_op, layout, a_creating, new_creator,
                  [](const CreatorFence &) { return true; }),
              CasRefCatalog::ReconcileCreatorOutcome::EntryChanged);
}

/// casAdmitEntry: the cache holds "a"; the external erased it -- admits it again on the fresh read.
/// The stale hint's own decide sees "a" still Live and duplicates the namespace before the lane can
/// reread, so `encodeRefCatalog` throws `LOGICAL_ERROR` on the hint attempt -- split like the blocks
/// above, since constructing that exception aborts under debug/sanitizer builds before this test's own
/// rescue (the reread on a fresh, "a"-absent state) ever runs.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, AStaleHintCasAdmitEntryAdmitsWhenTheStoreHasRoom)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a = liveEntry("a", 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a);   /// cache: "a"

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, a), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    eraseRowRaw(other, layout, a.ns);

    const uint64_t reads_before = f.backend->getCount(layout.refCatalogKey());
    EXPECT_NO_THROW(CasRefCatalog::casAdmitEntry(pool_op, layout, liveEntry("a", 2)));
    EXPECT_EQ(f.backend->getCount(layout.refCatalogKey()) - reads_before, 1u) << "one lane read";
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, AStaleHintCasAdmitEntryAdmitsWhenTheStoreHasRoomAborts)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a = liveEntry("a", 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a);   /// cache: "a"

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, a), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    eraseRowRaw(other, layout, a.ns);

    /// The lane's re-render on a fresh read cannot rescue this, because constructing the
    /// `LOGICAL_ERROR` for the hint's own duplicate aborts before the reread ever happens.
    EXPECT_DEATH({ CasRefCatalog::casAdmitEntry(pool_op, layout, liveEntry("a", 2)); }, "not canonically ordered");
}
#endif

/// casAdmitEntry: "a" is still present in the fresh read (Removing, not the Live hint the cache
/// holds) -- re-admitting it duplicates the namespace, and `encodeRefCatalog`'s own canonical-order
/// check aborts with `LOGICAL_ERROR` under debug/sanitizer builds -- split like the blocks above.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASRefCatalog, ATrueRefusalCasAdmitEntrySeesItsRowStillPresent)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a = liveEntry("a", 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a);

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, a), CasRefCatalog::BeginRemovingOutcome::Transitioned);

    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR,
        [&] { CasRefCatalog::casAdmitEntry(pool_op, layout, a); });
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASRefCatalogDeathTest, ATrueRefusalCasAdmitEntrySeesItsRowStillPresentAborts)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry a = liveEntry("a", 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, a);

    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, a), CasRefCatalog::BeginRemovingOutcome::Transitioned);

    EXPECT_DEATH({ CasRefCatalog::casAdmitEntry(pool_op, layout, a); }, "not canonically ordered");
}
#endif

TEST(CASRefCatalog, AFenceLostDuringStepOneIsFencedOutNotABareMarker)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation seed = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(seed, layout);
    const CreatorFence creator{.server_root_id = "srv", .writer_epoch = 1, .fence_generation = 1};

    /// Tripped before its turn: the lane's wait refuses it, nothing is written.
    {
        bool alive = true;
        CasOperation op = f.pool.admit([&] { return alive; });
        f.hot_keys.enter_after_lane_hook_for_test = [&] { alive = false; };
        const uint64_t writes_before = f.backend->writeCount(layout.refCatalogKey());
        EXPECT_EQ(CasRefCatalog::createNamespace(op, layout, RootNamespace{"a"}, creator),
                  CasRefCatalog::NamespaceCreationOutcome::FencedOut);
        f.hot_keys.enter_after_lane_hook_for_test = {};
        EXPECT_EQ(f.backend->writeCount(layout.refCatalogKey()), writes_before);
    }
    /// Tripped between the landed step-1 write and the post-commit check: FencedOut, and the Creating
    /// row is durable for a later reconciler.
    {
        bool alive = true;
        CasOperation op = f.pool.admit([&] { return alive; });
        f.backend->onWriteCommitted(layout.refCatalogKey(), [&] { alive = false; });
        EXPECT_EQ(CasRefCatalog::createNamespace(op, layout, RootNamespace{"b"}, creator),
                  CasRefCatalog::NamespaceCreationOutcome::FencedOut);
        f.backend->onWriteCommitted(layout.refCatalogKey(), {});
        const auto snap = CasRefCatalog::read(seed, layout);
        ASSERT_EQ(snap.catalog.entries.size(), 1u);
        EXPECT_EQ(snap.catalog.entries[0].state, NsState::Creating);
    }
    /// casAdmitEntry under a tripped fence throws the transient-unavailable class, never a bare marker.
    {
        CasOperation op = f.pool.admit([] { return false; });
        DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR,
            [&] { CasRefCatalog::casAdmitEntry(op, layout, liveEntry("c", 3)); });
    }
}

#if USE_AWS_S3
TEST(CASRefCatalog, TheStoresAnswerToAHintsWriteIsDeliveredAndTheRetryLearnsTheRest)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    f.backend->setRefreshCredentialsResult(false);
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    const CatalogEntry observed = liveEntry("a", 1);
    CasRefCatalog::casAdmitEntry(pool_op, layout, observed);   /// the cache holds "a" Live

    /// Another server moved the row on; the hint still matches what this caller observed, so a write
    /// goes out, and the store refuses it definitively.
    CasOperation other = f.external.admit();
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, observed), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    f.backend->failNextWriteWith(layout.refCatalogKey(), s3Error(Aws::S3::S3Errors::ACCESS_DENIED, "AccessDenied"));
    const uint64_t writes_before = f.backend->writeCount(layout.refCatalogKey());
    const uint64_t reads_before = f.backend->getCount(layout.refCatalogKey());
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::S3_ERROR,
        [&] { (void)CasRefCatalog::beginRemoving(pool_op, layout, observed); });
    EXPECT_EQ(f.backend->writeCount(layout.refCatalogKey()) - writes_before, 1u);
    EXPECT_EQ(f.backend->getCount(layout.refCatalogKey()) - reads_before, 0u);
    /// The caller's ordinary retry after a fresh observation learns the row moved.
    EXPECT_EQ(CasRefCatalog::beginRemoving(pool_op, layout, observed), CasRefCatalog::BeginRemovingOutcome::AlreadyRemoving);

    /// The same stale hint, and the fence trips between the refused write and its resolve read.
    CasRefCatalog::casAdmitEntry(pool_op, layout, liveEntry("b", 2));
    const CatalogEntry b = liveEntry("b", 2);
    ASSERT_EQ(CasRefCatalog::beginRemoving(other, layout, b), CasRefCatalog::BeginRemovingOutcome::Transitioned);
    bool alive = true;
    CasOperation fenced = f.pool.admit([&] { return alive; });
    f.backend->onBeforeWrite(layout.refCatalogKey(), [&] { alive = false; });
    EXPECT_EQ(CasRefCatalog::beginRemoving(fenced, layout, b), CasRefCatalog::BeginRemovingOutcome::FencedOut);
    f.backend->onBeforeWrite(layout.refCatalogKey(), {});
    EXPECT_EQ(CasRefCatalog::read(other, layout).catalog.entries.at(1).state, NsState::Removing) << "nothing of ours landed";
    CasOperation readmitted = f.pool.admit();
    EXPECT_EQ(CasRefCatalog::beginRemoving(readmitted, layout, b), CasRefCatalog::BeginRemovingOutcome::AlreadyRemoving);
}
#endif

TEST(CASRefCatalog, TheCatalogLoopPacesAConflictByWhetherItSettledAFault)
{
    PoolAndExternal f(std::make_shared<CountingBackend>());
    const Layout layout("p");
    CasOperation pool_op = f.pool.admit();
    CasRefCatalog::initializeEmptyForNewPool(pool_op, layout);
    CasRefCatalog::casAdmitEntry(pool_op, layout, liveEntry("a", 1));
    CasOperation other = f.external.admit();
    const String key = layout.refCatalogKey();

    /// K clean races: the external moves the catalog before each of the pool's first K writes. The
    /// two scenarios below admit under DIFFERENT namespace prefixes -- the first scenario's "x" rows
    /// are still on the catalog when the second starts, and re-admitting the same namespace would
    /// duplicate it rather than race it.
    constexpr int K = 3;
    /// The settled-fault scenario draws 8, not 3: an upper-bound-only check on `backoff`'s draws
    /// cannot tell the growing schedule from the flat one (both stay under 200 ms plenty often at
    /// small K), so that scenario also needs a floor. With `backoff(attempt) = uniform(0, min(5000,
    /// 200 << (attempt-1)))`, the flat schedule can NEVER draw over 200 ms, so a single draw over
    /// 200 ms is proof by itself that the schedule grew; drawing 8 times keeps the growing schedule's
    /// own chance of missing that floor purely by bad luck around 2^-28.
    constexpr int K2 = 8;
    int moved = 0;
    int limit = K;
    bool inside = false;
    bool ambiguous = false;
    String prefix = "x";
    uint64_t incarnation_base = 10;
    f.backend->onBeforeWrite(key, [&]
    {
        if (inside || moved >= limit)
            return;
        inside = true;
        CasRefCatalog::casAdmitEntry(other, layout, liveEntry(prefix + std::to_string(moved), incarnation_base + moved));
        if (ambiguous)
            f.backend->injectAmbiguousWrite(key);
        ++moved;
        inside = false;
    });
    const auto pauses_before = eventCount(ProfileEvents::CASRequestConflictPause);
    CasRefCatalog::casUpdate(pool_op, layout, [](const RefCatalog & cur)
    {
        RefCatalog next = cur;
        for (auto & e : next.entries)
            if (e.ns.string() == "a") { e.state = NsState::Removing; e.removal_started_round = 1; }
        return next;
    });
    ASSERT_EQ(f.clock.sleeps.size(), static_cast<size_t>(K));
    for (uint64_t s : f.clock.sleeps)
        EXPECT_LE(s, 200u);
    EXPECT_EQ(eventCount(ProfileEvents::CASRequestConflictPause) - pauses_before, 0u) << "the lane's caller pauses itself; the engine's counter is for its own loops";

    /// K2 conflicts that each settled a fault: the growing schedule, on the loop's own count. An
    /// upper-bound check alone is satisfied by the flat schedule too (its draws are a subset of the
    /// growing schedule's early-attempt range), so this also asserts a floor: at least one draw over
    /// 200 ms, which the flat schedule can never produce.
    f.clock.sleeps.clear();
    moved = 0;
    limit = K2;
    ambiguous = true;
    prefix = "y";
    incarnation_base = 20;
    CasRefCatalog::casUpdate(pool_op, layout, [](const RefCatalog & cur) { return cur; });
    ASSERT_EQ(f.clock.sleeps.size(), static_cast<size_t>(K2));
    for (size_t i = 0; i < f.clock.sleeps.size(); ++i)
        EXPECT_LE(f.clock.sleeps[i], std::min<uint64_t>(5000, 200ull << i));
    EXPECT_TRUE(std::any_of(f.clock.sleeps.begin(), f.clock.sleeps.end(), [](uint64_t s) { return s > 200u; }))
        << "the flat schedule can never draw over 200 ms; a growing schedule almost certainly does "
           "somewhere in 8 draws, so this is what tells the two schedules apart";
}

