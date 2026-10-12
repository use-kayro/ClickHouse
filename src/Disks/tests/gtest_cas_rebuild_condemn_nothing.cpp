#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include "cas_test_helpers.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

/// fsck WALKS STREAMS BY ARITHMETIC (spec 2026-07-27 "ref chain complete cut" §7).
///
/// fsck may not rest a verdict on a listing. It
/// walks each namespace's ref stream by ARITHMETIC from `_ckpt.checkpoint` upward, reading every id by
/// exact key, and reports one verdict per namespace — `chain-broken` (a 404 below a CONFIRMED durable
/// same-epoch id: a hole, fatal in the summary AND in the exit code), `unchecked` (could not prove it
/// either way), or nothing at all. A finding is RECORDED, never thrown: an fsck that dies on the first
/// bad namespace says nothing about the ones it never reached.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

const RootNamespace kNsA{"00/aa@cas@"};
const RootNamespace kNsB{"00/zz@cas@"};

std::optional<DB::Cas::Object> readOf(Backend & backend, const String & key)
{
    OperationForTest op(backend);
    return (*op).read(key, Retry::standard());
}

/// Publish the exact `_ckpt` that makes a raw fixture recoverable. The real writers go through
/// `publishCkpt`, which merges by semantic maximum and additionally refuses a `life_epoch` below the
/// durable one; this helper instead makes the fixture's one admissible recovery frontier explicit.
void writeCkptRaw(Backend & backend, const Layout & layout, const RootNamespace & ns, const RefCkpt & ckpt)
{
    writeRecoverableCkptForRawFixture(backend, layout, ns, ckpt);
}

/// The table state after applying exactly `ids`, through the same builder as recovery — so a snapshot
/// built from it is what the codec itself would have published.
RefTableState stateAfter(Backend & backend, const Layout & layout, const RootNamespace & ns,
                         const std::vector<RefTxnId> & ids)
{
    RefReplayBuilder builder(std::nullopt);
    for (const RefTxnId & id : ids)
    {
        const auto got = readOf(backend, layout.refLogKey(fixture::fixtureLife(ns), id));
        if (!got)
            throw std::runtime_error("stateAfter: fixture log " + std::to_string(id.writer_epoch) + "-"
                                     + std::to_string(id.ref_sequence) + " is missing");
        builder.applyOne(decodeRefLogTxn(openObject(FormatId::RefLog, got->bytes), ns.string(), id),
                         got->bytes.size());
    }
    return std::move(builder).finish().state;
}

/// Two writer epochs joined by a real seal: `{1,1} {1,2}` then the `{1,3}` seal, then `{2,1}` naming it
/// as its `prev_epoch_seal` and `{2,2}` after it. The exact-authority walk must handle this crossing.
void seedSealedTwoEpochStream(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    publishAt(backend, layout, ns, RefTxnId{1, 1}, "ref_a", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(backend, layout, ns, RefTxnId{1, 2}, "ref_b", 2, DB::UInt128(2));
    writeSealAt(backend, layout, ns, RefTxnId{1, 3});
    publishAt(backend, layout, ns, RefTxnId{2, 1}, "ref_c", 1, DB::UInt128(3), /*birth=*/false,
              /*prev_epoch_seal=*/RefTxnId{1, 3});
    publishAt(backend, layout, ns, RefTxnId{2, 2}, "ref_d", 2, DB::UInt128(4));
}

/// The number of rows in `cls` whose note mentions `needle`, over the whole report.
size_t rowsMentioning(const FsckReport & rep, FsckClass cls, const String & needle)
{
    size_t n = 0;
    for (const FsckObject & o : rep.objects)
    {
        if (o.cls != cls)
            continue;
        for (const String & note : o.reachable_from)
            if (note.find(needle) != String::npos)
                ++n;
    }
    return n;
}

}

/// ---- REBUILD condemns nothing ----

/// ---- fsck: arithmetic streams ----

/// A pool with nothing wrong reports nothing: no hole, no unproven namespace, a clean bill of health
/// and a zero exit. `unchecked` is not a resting state — it is a verdict a healthy pool never reaches.
TEST(CASRebuildCondemnNothingFsck, HealthyArithmeticPoolIsClean)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    publishAt(*backend, layout, kNsA, RefTxnId{1, 1}, "ref_a", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, kNsA, RefTxnId{1, 2}, "ref_b", 2, DB::UInt128(2));
    publishAt(*backend, layout, kNsA, RefTxnId{1, 3}, "ref_c", 3, DB::UInt128(3));
    writeCkptRaw(*backend, layout, kNsA,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 3},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});

    const FsckReport rep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(rep.clean()) << formatFsckSummary(rep);
    EXPECT_EQ(rep.chain_broken, 0u);
    EXPECT_EQ(rep.unchecked, 0u) << "a healthy namespace is PROVEN, not merely uncomplained-about";
    EXPECT_EQ(rep.ref_records_walked, 3u);
    EXPECT_EQ(rep.dangling, 0u);
}

/// A 404 BELOW a durable same-epoch id. Ids are dense `1..T` within `(namespace, epoch)` (INV-1), so
/// this cannot be the end of a stream: a durable record is missing and every transaction above it is
/// unreachable. The verdict is FATAL — it appears in the machine-parseable summary line and it makes
/// the report unclean, which is what turns into the command's nonzero exit.
TEST(CASRebuildCondemnNothingFsck, MidChainHoleBelowAWitnessIsChainBroken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    publishAt(*backend, layout, kNsA, RefTxnId{1, 1}, "ref_a", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, kNsA, RefTxnId{1, 2}, "ref_b", 2, DB::UInt128(2));
    publishAt(*backend, layout, kNsA, RefTxnId{1, 3}, "ref_c", 3, DB::UInt128(3));
    writeCkptRaw(*backend, layout, kNsA,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 3},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});

    /// Punch the hole: {1,2} is gone while {1,3} stays durable and listed.
    const String holed = layout.refLogKey(fixture::fixtureLife(kNsA), RefTxnId{1, 2});
    OperationForTest hole_op(*backend);
    const auto h = (*hole_op).head(holed, Retry::standard());
    ASSERT_TRUE(h.has_value());
    ASSERT_EQ((*hole_op).remove(holed, h->etag, Retry::standard()), Removal::Removed);

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail=*/true))
        << "a finding is RECORDED, never thrown — an fsck that dies reports nothing";
    EXPECT_EQ(rep.chain_broken, 1u);
    EXPECT_FALSE(rep.clean()) << "chain-broken is a hard finding";
    EXPECT_NE(formatFsckSummary(rep).find("chain_broken=1"), String::npos) << formatFsckSummary(rep);

    bool row = false;
    for (const FsckObject & o : rep.objects)
        if (o.cls == FsckClass::ChainBroken)
            row = true;
    EXPECT_TRUE(row) << "the fatal must name the position it was detected at";
}

/// The tail ABOVE `_ckpt.checkpoint` is WALKED, not assumed. Here the store lists neither of the two
/// records above the checkpoint, so a listing-driven audit would see an empty tail and report a clean
/// pool it never read. Arithmetic reads them by exact key: they are walked, counted, and the namespace
/// comes back PROVEN — not `unchecked`, which is reserved for what cannot be proved at all.
TEST(CASRebuildCondemnNothingFsck, TailAboveTheCheckpointIsWalkedNotUnchecked)
{
    auto backend = std::make_shared<HintHoleBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    publishAt(*backend, layout, kNsA, RefTxnId{1, 1}, "ref_a", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, kNsA, RefTxnId{1, 2}, "ref_b", 2, DB::UInt128(2));
    publishAt(*backend, layout, kNsA, RefTxnId{1, 3}, "ref_c", 3, DB::UInt128(3));
    publishAt(*backend, layout, kNsA, RefTxnId{1, 4}, "ref_d", 4, DB::UInt128(4));

    /// A published snapshot at {1,2}, named by the checkpoint. Its bytes are the codec's own view of
    /// the state at {1,2}, so exact checkpoint-base validation accepts it.
    writeRefSnapshotRaw(*backend, layout,
                        snapshotOf(stateAfter(*backend, layout, kNsA, {RefTxnId{1, 1}, RefTxnId{1, 2}}), kNsA.string()));
    writeCkptRaw(*backend, layout, kNsA,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 4},
                         .checkpoint_snapshot_id = RefTxnId{1, 2}, .last_epoch_seal = std::nullopt});

    /// The store stops listing the tail. It stays perfectly readable by exact key.
    backend->hide(layout.refLogKey(fixture::fixtureLife(kNsA), RefTxnId{1, 3}));
    backend->hide(layout.refLogKey(fixture::fixtureLife(kNsA), RefTxnId{1, 4}));

    const FsckReport rep = runFsck(*store, /*detail=*/true);
    ASSERT_GT(backend->holesServed(), 0u) << "the tail was never actually hidden from a LIST";
    EXPECT_EQ(rep.ref_records_walked, 2u) << "the two records above the checkpoint must be read by exact key";
    EXPECT_EQ(rep.unchecked, 0u) << "a walked tail is PROVEN; `unchecked` is not a default";
    EXPECT_EQ(rep.chain_broken, 0u);
    EXPECT_TRUE(rep.clean()) << formatFsckSummary(rep);
}

/// A SEALED multi-epoch stream, walked. The epoch boundary is crossed the way the protocol proves it —
/// through the next epoch's `prev_epoch_seal` back-chain — never by guessing `epoch + 1`. The
/// checkpoint sits below the seal, so the tail the walk owes covers the boundary itself.
TEST(CASRebuildCondemnNothingFsck, SealedStreamIsWalkedAcrossTheEpochBoundary)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    seedSealedTwoEpochStream(*backend, layout, kNsA);
    writeRefSnapshotRaw(*backend, layout,
                        snapshotOf(stateAfter(*backend, layout, kNsA, {RefTxnId{1, 1}, RefTxnId{1, 2}}), kNsA.string()));
    writeCkptRaw(*backend, layout, kNsA,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{2, 2},
                         .checkpoint_snapshot_id = RefTxnId{1, 2}, .last_epoch_seal = RefTxnId{1, 3}});

    const FsckReport rep = runFsck(*store, /*detail=*/true);
    EXPECT_TRUE(rep.clean()) << formatFsckSummary(rep);
    EXPECT_EQ(rep.chain_broken, 0u);
    EXPECT_EQ(rep.unchecked, 0u) << "a PROVED crossing is not an unproven one";
    EXPECT_EQ(rep.ref_records_walked, 3u) << "the seal plus both records of the epoch it opened";
}

/// An exact `_ckpt` frontier turns an impossible epoch crossing into a hard chain break. Here `ns_a`'s
/// epoch 1 ends at the PRESENT ordinary record `{1,3}`, while both `_ckpt` and `{2,1}` falsely claim
/// that position as the closing seal. The finite range is complete and proves the contradiction: this
/// is NOT `unchecked`, and there is no earlier missing record that could make the test pass instead.
/// The healthy `ns_b` in the same pool is unaffected — one broken namespace never spreads.
TEST(CASRebuildCondemnNothingFsck, ExactFrontierMakesAnUnsealedEpochCrossingChainBroken)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    publishAt(*backend, layout, kNsA, RefTxnId{1, 1}, "ref_a", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, kNsA, RefTxnId{1, 2}, "ref_b", 2, DB::UInt128(2));
    publishAt(*backend, layout, kNsA, RefTxnId{1, 3}, "ref_c", 3, DB::UInt128(3));
    /// `{1,3}` exists but is an ordinary owner transaction, not an `EpochSeal`. Claiming it as the
    /// predecessor must not authorize the transition to epoch 2.
    publishAt(*backend, layout, kNsA, RefTxnId{2, 1}, "ref_d", 1, DB::UInt128(4), /*birth=*/false,
              /*prev_epoch_seal=*/RefTxnId{1, 3});

    publishAt(*backend, layout, kNsB, RefTxnId{1, 1}, "ref_z", 1, DB::UInt128(9), /*birth=*/true);
    writeCkptRaw(*backend, layout, kNsA,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{2, 1},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = RefTxnId{1, 3}});
    writeCkptRaw(*backend, layout, kNsB,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 1},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail=*/true));
    EXPECT_EQ(rep.unchecked, 0u) << "the exact frontier proves this crossing inconsistent";
    EXPECT_EQ(rep.chain_broken, 1u) << "exactly the malformed namespace must be reported";
    const String missing_key = layout.refLogKey(fixture::fixtureLife(kNsA), RefTxnId{1, 4});
    EXPECT_EQ(std::count_if(rep.objects.begin(), rep.objects.end(), [&](const FsckObject & object)
    {
        return object.cls == FsckClass::ChainBroken && object.key == missing_key;
    }), 1u) << "the present ordinary 1-3 cannot close epoch 1, so arithmetic continuation requires 1-4";
    EXPECT_GE(rowsMentioning(rep, FsckClass::ChainBroken, "checkpoint requires id 1-4"), 1u)
        << "the verdict must expose that the claimed ordinary predecessor did not authorize a crossing";
    EXPECT_GE(rowsMentioning(rep, FsckClass::ChainBroken, "inclusive frontier 2-1"), 1u)
        << "the verdict must name the authority that made the absence a proven chain break";
}

/// W2 (Task-3 review): a holed namespace used to make the WHOLE scan throw — `applyOne` raises
/// `CORRUPTED_DATA` on a non-contiguous replay and nothing caught it, so one bad table aborted the
/// audit and every namespace after it went unexamined. For recovery, throwing is the correct
/// fail-close; for a read-only diagnostic it violates "record and continue, never wedge".
TEST(CASRebuildCondemnNothingFsck, OneBadNamespaceDoesNotAbortTheAudit)
{
    auto backend = std::make_shared<InMemoryBackend>();
    auto store = openPoolForTest(backend);
    const Layout & layout = store->layout();

    /// `ns_a` sorts FIRST, so a scan that dies on it never reaches `ns_b`.
    publishAt(*backend, layout, kNsA, RefTxnId{1, 1}, "ref_a", 1, DB::UInt128(1), /*birth=*/true);
    publishAt(*backend, layout, kNsA, RefTxnId{1, 2}, "ref_b", 2, DB::UInt128(2));
    publishAt(*backend, layout, kNsA, RefTxnId{1, 3}, "ref_c", 3, DB::UInt128(3));
    writeCkptRaw(*backend, layout, kNsA,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 3},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});
    const String holed = layout.refLogKey(fixture::fixtureLife(kNsA), RefTxnId{1, 2});
    OperationForTest hole_op(*backend);
    const auto h = (*hole_op).head(holed, Retry::standard());
    ASSERT_TRUE(h.has_value());
    ASSERT_EQ((*hole_op).remove(holed, h->etag, Retry::standard()), Removal::Removed);

    publishAt(*backend, layout, kNsB, RefTxnId{1, 1}, "ref_z", 1, DB::UInt128(9), /*birth=*/true);
    writeCkptRaw(*backend, layout, kNsB,
                 RefCkpt{.life_epoch = 1, .committed_through = RefTxnId{1, 1},
                         .checkpoint_snapshot_id = std::nullopt, .last_epoch_seal = std::nullopt});

    FsckReport rep;
    ASSERT_NO_THROW(rep = runFsck(*store, /*detail=*/true));
    EXPECT_EQ(rep.chain_broken, 1u);
    EXPECT_GE(rep.reachable, 1u) << "the namespace AFTER the broken one must still have been examined";
    EXPECT_EQ(rep.dangling, 0u) << "`ns_b` is healthy; a wedged scan would have reported nothing about it";
}
