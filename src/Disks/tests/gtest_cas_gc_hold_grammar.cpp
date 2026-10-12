#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasByteBudget.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFoldSealFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <base/defines.h>
#include "cas_test_helpers.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

/// DURABLE HOLDS (spec 2026-07-27 "ref chain complete cut" §5).
///
/// A namespace whose ref-log walk meets an IMPOSSIBLE shape stops there, and that stop has to survive
/// the round. A classification alone cannot preserve the cause and position of the stop; without
/// durable hold evidence, that information lives only in a log line and an
/// in-memory anomaly, both gone by the next round. That is not enough for three separate reasons:
///
///   * the next round could not RETRY the exact position, so a hold only survived while the round's
///     hint happened to keep mentioning the namespace;
///   * the hold could be cleared by an ABSENT — precisely the observation a lying store produces, and
///     precisely the shape that made the hold necessary in the first place;
///   * REBUILD rewrote coverage from owner state and silently dropped every hold, handing back a
///     baseline that looked proven when it was not.
///
/// So the hold is now DURABLE and STRICTLY GRAMMARED: `{reason, offending_position, retry_count,
/// next_retry_round}` present if and only if `classification == CoverageClass::Clamped`, rejected in
/// both directions otherwise. It rides the seal across rounds — including rounds whose hint omits the
/// namespace entirely — and across REBUILD, and it clears by exactly ONE event: the fold resolving the
/// offending position and that result being adopted in `gc/state`.
///
/// The carried hold is also a WITNESS, and a better one than the listing: it is durable proof that the
/// walk once reached that position, so an absent below it is a gap rather than a frontier no matter
/// what the hint says this round. That is what makes "retry the exact offending position" work for a
/// hold that sits above an epoch boundary.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

namespace
{

/// ===================== FIXTURES =====================

UInt128 fixtureLifeId(std::string_view key)
{
    return key.ends_with("/1") ? UInt128{2} : UInt128{1};
}

RefCoverage & fixtureCoverage(CasFoldSeal & seal, std::string_view key)
{
    return seal.ref_lives[fixtureLifeId(key)].coverage;
}

const RefCoverage & fixtureCoverage(const CasFoldSeal & seal, std::string_view key)
{
    return seal.ref_lives.at(fixtureLifeId(key)).coverage;
}

/// A seal carrying exactly one held coverage row with every numeric at its maximum and a coverage key
/// that needs escaping — the widest row the per-row line budget has to survive. The caller supplies the
/// key, which is what the line-cap tests below grow byte by byte.
CasFoldSeal maximalHoldSeal(const String & map_key)
{
    CasFoldSeal seal;
    seal.generation = std::numeric_limits<uint64_t>::max();
    seal.parent_generation = std::numeric_limits<uint64_t>::max();
    RefCoverage cov;
    cov.classification = CoverageClass::Clamped;
    cov.last_folded_ref_id = RefTxnId{std::numeric_limits<uint64_t>::max(),
                                      std::numeric_limits<uint64_t>::max()};
    cov.hold = RefHold{.reason = HoldReason::UnconsumedSealCrossing,   /// the longest reason word
                       .offending_position = RefTxnId{std::numeric_limits<uint64_t>::max(),
                                                      std::numeric_limits<uint64_t>::max()},
                       .retry_count = std::numeric_limits<uint32_t>::max(),
                       .next_retry_round = std::numeric_limits<uint64_t>::max()};
    fixtureCoverage(seal, map_key) = cov;
    return seal;
}

/// The `cov` line of an encoded seal (line 3: header, meta, then the single record).
String covLineOf(const String & encoded)
{
    size_t begin = encoded.find('\n') + 1;   /// past the header
    begin = encoded.find('\n', begin) + 1;   /// past the meta line
    return encoded.substr(begin, encoded.find('\n', begin) - begin);
}

/// A one-row seal whose coverage is ordinary and CLEAN: folded through its cursor, nothing held.
CasFoldSeal cleanSeal(const String & map_key)
{
    CasFoldSeal seal;
    seal.generation = 3;
    seal.parent_generation = 2;
    RefCoverage cov;
    cov.classification = CoverageClass::Folded;
    cov.last_folded_ref_id = RefTxnId{4, 5};
    fixtureCoverage(seal, map_key) = cov;
    return seal;
}

/// A one-row seal whose coverage is HELD at an exact position — the row every erasure shape below is
/// trying to make disappear.
CasFoldSeal heldSeal(const String & map_key)
{
    CasFoldSeal seal = cleanSeal(map_key);
    RefCoverage & cov = fixtureCoverage(seal, map_key);
    cov.classification = CoverageClass::Clamped;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{4, 6},
                       .retry_count = 7, .next_retry_round = 99};
    return seal;
}

/// The header and meta lines (1 and 2) of an encoded seal, terminators included.
String headerAndMetaOf(const String & encoded)
{
    const size_t past_meta = encoded.find('\n', encoded.find('\n') + 1) + 1;
    EXPECT_NE(past_meta, 0u);
    return encoded.substr(0, past_meta);
}

/// Assemble a raw seal object from `records` (one record per element, no terminators), on `prototype`'s
/// header and meta lines, closed by the trailer count those records imply. This is the ONLY way to put a
/// repeated record key on the wire: `CasFoldSeal` stores keyed maps, so a duplicate is not a value any
/// producer can hold — it is a shape a forged, truncated, or mis-merged object has.
String sealTextWith(const String & prototype, const std::vector<String> & records)
{
    String text = headerAndMetaOf(prototype);
    for (const String & record : records)
        text += record + "\n";
    return text + "{\"n\":" + std::to_string(records.size()) + "}\n";
}

/// Replace the FIRST occurrence of `field` with `replacement` (both are whole `"key":value` fragments),
/// so a test states the exact wire shape it is feeding the decoder.
String withField(const String & encoded, const String & field, const String & replacement)
{
    const size_t at = encoded.find(field);
    EXPECT_NE(at, String::npos) << "the encoder does not emit " << field;
    return encoded.substr(0, at) + replacement + encoded.substr(at + field.size());
}

/// Every coverage row the ENCODER must refuse, each paired with why producing it would be a bug in our
/// own fold rather than corruption arriving from a store. Shared by the two builds' assertions below so
/// the release expectation and the sanitizer death expectation can never drift apart.
std::vector<std::pair<const char *, CasFoldSeal>> illFormedSealsTheEncoderMustRefuse()
{
    std::vector<std::pair<const char *, CasFoldSeal>> out;

    /// The pairing, both ways round.
    CasFoldSeal hold_on_folded = heldSeal("ns/0");
    fixtureCoverage(hold_on_folded, "ns/0").classification = CoverageClass::Folded;
    out.emplace_back("a hold on a folded row claims a stop that did not happen", hold_on_folded);

    CasFoldSeal clamped_without_hold = heldSeal("ns/0");
    fixtureCoverage(clamped_without_hold, "ns/0").hold.reset();
    out.emplace_back("a clamped row with no hold is indistinguishable from a clean cursor once "
                     "durable", clamped_without_hold);

    /// The closed set is now the enum's declared values, so only an explicit cast reaches outside it.
    /// 4 is the sharpest value to plant: it is outside the closed wire vocabulary.
    CasFoldSeal classification_retired_wire_value = cleanSeal("ns/0");
    fixtureCoverage(classification_retired_wire_value, "ns/0").classification
        = static_cast<CoverageClass>(4);
    out.emplace_back("classification 4 is outside the four values the wire table declares",
                     classification_retired_wire_value);

    CasFoldSeal classification_max = cleanSeal("ns/0");
    fixtureCoverage(classification_max, "ns/0").classification = static_cast<CoverageClass>(255);
    out.emplace_back("classification 255 is outside the four values the wire table declares",
                     classification_max);

    /// The self-erasing hold, and its half-zero sibling.
    CasFoldSeal hold_at_zero = heldSeal("ns/0");
    fixtureCoverage(hold_at_zero, "ns/0").hold->offending_position = RefTxnId{};
    out.emplace_back("a hold at {0,0} is cleared by the first record the next round folds", hold_at_zero);

    CasFoldSeal hold_zero_sequence = heldSeal("ns/0");
    fixtureCoverage(hold_zero_sequence, "ns/0").hold->offending_position = RefTxnId{7, 0};
    out.emplace_back("a hold position with a zero component is not a renderable id", hold_zero_sequence);

    return out;
}

}

/// ===================== THE SHARED BYTE ARITHMETIC =====================
///
/// Two caps, two predicates, one place they are computed. Stage B's catalog reuses THESE functions for
/// its additive "does one more entry still fit" question, so their boundary behaviour is pinned here
/// rather than re-derived per format: a cap is the largest PERMITTED value (equality fits), and every
/// sum saturates, because a wrapped sum answers "fits" for an object that does not — turning an
/// overflow into a durable object nothing can read.
TEST(CASGCHoldGrammarBudget, BothPredicatesAcceptEqualityAndRefuseOneMore)
{
    static_assert(fitsLineCap(64, 64));
    static_assert(!fitsLineCap(65, 64));
    static_assert(fitsObjectCap(40, 24, 64));
    static_assert(!fitsObjectCap(40, 25, 64));

    EXPECT_TRUE(fitsLineCap(64, 64));
    EXPECT_FALSE(fitsLineCap(65, 64));
    EXPECT_TRUE(fitsObjectCap(64, 0, 64));
    EXPECT_FALSE(fitsObjectCap(64, 1, 64));

    /// A cap of 0 means the format declares none (a streamed object never materialized whole).
    EXPECT_TRUE(fitsLineCap(std::numeric_limits<uint64_t>::max(), 0));
    EXPECT_TRUE(fitsObjectCap(std::numeric_limits<uint64_t>::max(), 1, 0));
}

TEST(CASGCHoldGrammarBudget, SumsSaturateInsteadOfWrapping)
{
    constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
    static_assert(addByteBudget(kMax, 1) == kMax);
    static_assert(addByteBudget(kMax, kMax) == kMax);
    static_assert(addByteBudget(3, 4) == 7);

    /// The predicate that matters: a reservation that would wrap must REFUSE, not report a tiny sum.
    EXPECT_FALSE(fitsObjectCap(kMax, 2, 256 * 1024 * 1024));
}

/// ===================== THE STRICT CLAMPED-CLASSIFICATION GRAMMAR =====================

TEST(CASGCHoldGrammar, EveryHoldReasonRoundTrips)
{
    for (const HoldReason reason : {HoldReason::GapBelowWitness, HoldReason::UnconsumedSealCrossing,
                                    HoldReason::WitnessDisappeared, HoldReason::BodyUndecodable,
                                    HoldReason::ManifestBodyMissing, HoldReason::CheckpointUndecodable})
    {
        CasFoldSeal seal;
        seal.generation = 3;
        seal.parent_generation = 2;
        RefCoverage cov;
        cov.classification = CoverageClass::Clamped;
        cov.last_folded_ref_id = RefTxnId{4, 5};
        cov.hold = RefHold{.reason = reason, .offending_position = RefTxnId{4, 6},
                           .retry_count = 7, .next_retry_round = 99};
        fixtureCoverage(seal, "ns/0") = cov;

        const CasFoldSeal back = decodeFoldSeal(encodeFoldSeal(seal));
        EXPECT_EQ(back, seal) << "hold reason " << static_cast<int>(reason);
        ASSERT_TRUE(fixtureCoverage(back, "ns/0").hold.has_value());
        EXPECT_EQ(fixtureCoverage(back, "ns/0").hold->reason, reason);
    }
}

/// THE ENCODER'S HALF OF THE GRAMMAR, in one place. Every shape here is OUR OWN fold handing the codec
/// a row it must never make durable, so the refusal is `LOGICAL_ERROR` — the code `encodeGcState` raises
/// for the same category of impossible input — and not the `CORRUPTED_DATA` reserved for bytes that
/// arrived from a store. Under a debug or sanitizer build that code ABORTS at construction
/// (`handle_error_code`), so the same table is asserted as a death expectation there; the contract
/// ("these bytes are never produced") is what both forms pin.
#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(CASGCHoldGrammar, TheEncoderRefusesEveryIllFormedCoverageRow)
{
    for (const auto & entry : illFormedSealsTheEncoderMustRefuse())
    {
        SCOPED_TRACE(entry.first);
        expectThrowsCode(DB::ErrorCodes::LOGICAL_ERROR, [&] { encodeFoldSeal(entry.second); });
    }
}
#endif

#if defined(DEBUG_OR_SANITIZER_BUILD)
TEST(CASGCHoldGrammarDeathTest, TheEncoderRefusesEveryIllFormedCoverageRow)
{
    for (const auto & entry : illFormedSealsTheEncoderMustRefuse())
    {
        SCOPED_TRACE(entry.first);
        EXPECT_DEATH({ (void)encodeFoldSeal(entry.second); }, "");
    }
}
#endif

TEST(CASGCHoldGrammar, AHoldOnAnyOtherClassificationIsRefusedByTheDecoder)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;

    /// Bytes some other producer wrote. Built by demoting a legitimate held row's classification, so the
    /// hold fields are exactly the ones the encoder emits.
    cov.classification = CoverageClass::Clamped;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{1, 2},
                       .retry_count = 0, .next_retry_round = 1};
    fixtureCoverage(seal, "ns/0") = cov;
    const String text = withField(encodeFoldSeal(seal), R"("class":"clamped")", R"("class":"folded")");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(text); });
}

TEST(CASGCHoldGrammar, ClampedWithoutAHoldIsRefusedByTheDecoder)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;
    cov.classification = CoverageClass::Clamped;
    cov.last_folded_ref_id = RefTxnId{1, 1};

    /// Every single hold field is REQUIRED: dropping any one of them is corruption, not a default.
    cov.hold = RefHold{.reason = HoldReason::BodyUndecodable, .offending_position = RefTxnId{1, 2},
                       .retry_count = 3, .next_retry_round = 4};
    fixtureCoverage(seal, "ns/0") = cov;
    const String whole = encodeFoldSeal(seal);
    for (const String & field : {String(R"("hold_reason":"body_undecodable")"), String(R"("hold_epoch":"1")"),
                                 String(R"("hold_seq":"2")"), String(R"("retries":3)"), String(R"("retry_round":"4")")})
    {
        SCOPED_TRACE("without " + field);
        const size_t at = whole.find(field);
        ASSERT_NE(at, String::npos) << "the encoder does not emit " << field;
        String without = whole;
        without.erase(at - 1, field.size() + 1);   /// the field and the ',' before it
        expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(without); });
    }
}

TEST(CASGCHoldGrammar, DuplicateHoldKeyIsCorruptedData)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;
    cov.classification = CoverageClass::Clamped;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{1, 2},
                       .retry_count = 0, .next_retry_round = 5};
    fixtureCoverage(seal, "ns/0") = cov;

    const String whole = encodeFoldSeal(seal);
    const String field = R"("hold_reason":"gap_below_witness")";
    const size_t at = whole.find(field);
    ASSERT_NE(at, String::npos);
    /// The same key twice, with a DIFFERENT value: last-wins would silently rewrite the reason.
    String doubled = whole;
    doubled.insert(at, R"("hold_reason":"witness_disappeared",)");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(doubled); });
}

TEST(CASGCHoldGrammar, UnknownHoldReasonWordIsCorruptedData)
{
    CasFoldSeal seal;
    seal.generation = 1;
    RefCoverage cov;
    cov.classification = CoverageClass::Clamped;
    cov.hold = RefHold{.reason = HoldReason::GapBelowWitness, .offending_position = RefTxnId{1, 2},
                       .retry_count = 0, .next_retry_round = 5};
    fixtureCoverage(seal, "ns/0") = cov;

    String text = encodeFoldSeal(seal);
    const size_t at = text.find("gap_below_witness");
    ASSERT_NE(at, String::npos);
    text.replace(at, strlen("gap_below_witness"), "gap_below_witnesX");
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(text); });
}

/// ===================== THE THREE WAYS A SEAL CAN ERASE A HOLD =====================
///
/// The three shapes below are one finding, and it is about what a fold seal is FOR. The hold is the only
/// durable record that a namespace stopped and where; everything downstream reads the seal and nothing
/// re-derives the stop. So a seal that decodes into "no hold here" is not a lossy read, it is a licence
/// to delete: the sweep's §6 refusals are stated as `classification == Clamped` / `== Absent` /
/// `hold.has_value()`, and a row that slips past all three reaches an irreversible delete of a manifest
/// the fold never accounted for. Each shape gets past a DIFFERENT one of the decoder's checks, which is
/// why they are pinned separately rather than as one "malformed seal" case.

/// (1) The classification is a WORD, closed the same way `hold_reason` already is:
/// `coverageClassFromWord` refuses anything outside the four named values as `CORRUPTED_DATA` before a
/// `CoverageClass` is ever constructed, so there is no wide-integer narrowing attack left to catch here —
/// the wire carries no integer at all.
TEST(CASGCHoldGrammar, AClassificationOutsideTheGrammarIsCorruptedData)
{
    const String clean = encodeFoldSeal(cleanSeal("ns/0"));
    ASSERT_EQ(fixtureCoverage(decodeFoldSeal(clean), "ns/0").classification, CoverageClass::Folded)
        << "the unmodified row is the one every case below deviates from";

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        decodeFoldSeal(withField(clean, R"("class":"folded")", R"("class":"foldedx")"));
    });

    /// The bare number `4` is the classification's pre-cut wire representation — the old byte-valued
    /// form. A retired spelling is legal here because this is a marked negative fixture proving the
    /// decoder still refuses it now that `class` takes a word; the byte-delta pins hold the other such
    /// fixtures, and both kinds are exempt from the vocabulary sweeps for the same reason.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        decodeFoldSeal(withField(clean, R"("class":"folded")", R"("class":4)"));
    });
}

/// And the field itself is required: an absent `class` reads as `absent`, which is not "nothing was
/// said about this namespace" but the positive claim "no round folded it".
TEST(CASGCHoldGrammar, ACoverageRowWithoutAClassificationIsCorruptedData)
{
    const String clean = encodeFoldSeal(cleanSeal("ns/0"));
    const String field = R"("class":"folded",)";
    const size_t at = clean.find(field);
    ASSERT_NE(at, String::npos);
    String without = clean;
    without.erase(at, field.size());
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&] { decodeFoldSeal(without); });
}

/// (2) The hold that clears itself. `{0,0}` passes the completeness check — every field is present — and
/// then the carry rule drops it on the next round, because a hold rides forward only while the walk
/// stops BELOW its position and nothing is below zero. The namespace advances with no record that it was
/// ever held. A zero in EITHER component is the same defect, and is additionally unnameable: the sweep
/// renders the position when it reports what it retained, and `renderRefTxnId` refuses a zero component.
TEST(CASGCHoldGrammar, AHoldWhoseOffendingPositionHasAZeroComponentIsCorruptedData)
{
    const String held = encodeFoldSeal(heldSeal("ns/0"));
    ASSERT_TRUE(fixtureCoverage(decodeFoldSeal(held), "ns/0").hold.has_value());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA, [&]
    {
        decodeFoldSeal(withField(withField(held, R"("hold_epoch":"4")", R"("hold_epoch":"0")"),
                                 R"("hold_seq":"6")", R"("hold_seq":"0")"));
    });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(held, R"("hold_epoch":"4")", R"("hold_epoch":"0")")); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(held, R"("hold_seq":"6")", R"("hold_seq":"0")")); });
}

/// (3) The duplicate row. Two `cov` records for the same (namespace, shard) — held first, clean second —
/// used to be accepted with last-wins, so a single appended line erased a hold without touching the one
/// that recorded it. There is exactly one row per key, and a second one is corruption.
TEST(CASGCHoldGrammar, ASecondCoverageRowForTheSameKeyIsCorruptedData)
{
    const String held_line = covLineOf(encodeFoldSeal(heldSeal("ns/0")));
    const String clean_line = covLineOf(encodeFoldSeal(cleanSeal("ns/0")));
    const String other_clean_line = covLineOf(encodeFoldSeal(cleanSeal("ns/1")));
    const String prototype = encodeFoldSeal(heldSeal("ns/0"));

    /// The CONTROL first: the same two-record assembly with DIFFERENT keys decodes, so the refusal below
    /// is about the repeated key and not about the way these bytes are forged.
    const CasFoldSeal two_keys = decodeFoldSeal(sealTextWith(prototype, {held_line, other_clean_line}));
    ASSERT_EQ(two_keys.ref_lives.size(), 2u);
    ASSERT_TRUE(fixtureCoverage(two_keys, "ns/0").hold.has_value());

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(prototype, {held_line, clean_line})); });
    /// Order does not redeem it: a clean row followed by a held one is the same broken object.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(prototype, {clean_line, held_line})); });
    /// Nor does repeating the identical row.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(prototype, {held_line, held_line})); });

    /// The ENCODER needs no matching check, and this is why: the seal stores keyed maps, so a second row
    /// for a key is not a value any producer can construct — assigning it replaces the first.
    CasFoldSeal seal = heldSeal("ns/0");
    fixtureCoverage(seal, "ns/0") = fixtureCoverage(cleanSeal("ns/0"), "ns/0");
    EXPECT_EQ(seal.ref_lives.size(), 1u);
}

/// The same one-record-per-key rule applies to `condemned`: a repeated row rewrites a shard's condemned
/// totals, which graduation paces on.
TEST(CASGCHoldGrammar, ASecondCondemnedSummaryRecordIsCorruptedData)
{
    CasFoldSeal seal = cleanSeal("ns/0");
    seal.condemned_summary[0] = CondemnedSummary{.condemned_total = 5, .pending_total = 1,
                                                 .oldest_nonpending_condemn_round = 3};
    const String encoded = encodeFoldSeal(seal);

    /// Lines 3..4 are `ref_life`, `condemned` in the encoder's fixed order.
    std::vector<String> lines;
    for (size_t begin = headerAndMetaOf(encoded).size(); begin < encoded.size();)
    {
        const size_t end = encoded.find('\n', begin);
        ASSERT_NE(end, String::npos);
        lines.push_back(encoded.substr(begin, end - begin));
        begin = end + 1;
    }
    ASSERT_EQ(lines.size(), 3u) << "ref_life, condemned and the trailer";
    const String ref_life_line = lines[0];
    const String condemned_line = lines[1];

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(sealTextWith(encoded, {ref_life_line, condemned_line, condemned_line})); });
    /// The unduplicated assembly is the control.
    const std::vector<String> one_of_each{ref_life_line, condemned_line};
    EXPECT_NO_THROW(decodeFoldSeal(sealTextWith(encoded, one_of_each)));
}

/// Unified cleanup evidence still requires a canonical nonzero removal transaction id. Decoding
/// foreign bytes must fail this read, never the process.
TEST(CASGCHoldGrammar, CleanupEvidenceWithAZeroRemovalIdIsCorruptedData)
{
    CasFoldSeal seal = cleanSeal("ns/0");
    seal.ref_lives.at(fixtureLifeId("ns/0")).cleanup_evidence =
        RefCleanupEvidence{.remove_txn_id = RefTxnId{2, 3}};
    const String encoded = encodeFoldSeal(seal);

    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(encoded, R"("remove_epoch":"2")", R"("remove_epoch":"0")")); });
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(encoded, R"("remove_seq":"3")", R"("remove_seq":"0")")); });
    /// Omitted entirely is the same thing: the fields default to zero.
    expectThrowsCode(DB::ErrorCodes::CORRUPTED_DATA,
                     [&] { decodeFoldSeal(withField(encoded, R"("remove_epoch":"2",)", "")); });
}

/// The OBJECT cap bounds the whole seal. Nothing on the fold-seal READ path enforces it (the seal
/// is read raw, never through `openObject`), so an oversized PUT would leave a durable seal that no
/// later round can decode — unrecoverable. The gate therefore sits before the bytes are handed out, and
/// equality is still accepted: the cap is the largest permitted size, not the first forbidden one.
TEST(CASGCHoldGrammar, ObjectCapAcceptsEqualityAndRefusesOneMoreByte)
{
    const uint64_t object_cap = foldSealCaps().object_cap;
    ASSERT_EQ(object_cap, 256u * 1024 * 1024);

    EXPECT_NO_THROW(checkFoldSealObjectBytes(object_cap - 1));
    EXPECT_NO_THROW(checkFoldSealObjectBytes(object_cap));
    expectThrowsCode(DB::ErrorCodes::LIMIT_EXCEEDED, [&] { checkFoldSealObjectBytes(object_cap + 1); });

    /// An ordinary seal is nowhere near it, so the gate costs a comparison and changes nothing.
    EXPECT_NO_THROW(encodeFoldSeal(maximalHoldSeal("ns/0")));
}
