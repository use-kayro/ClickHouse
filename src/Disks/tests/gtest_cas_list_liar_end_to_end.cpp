#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefProtocol.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include "cas_test_helpers.h"

#include <map>
#include <set>
#include <string>
#include <vector>

/// THE 2026-07-25 RELEASE BLOCKER, AS A PERMANENT REGRESSION.
///
/// The defect the object store actually exhibited (`reports/2026-07-26-list-incompleteness-proof/`):
/// objects that were durable, acked, and readable by exact key were OMITTED from enumeration, while a
/// LATER key under the same prefix was listed. Nothing was lost and nothing was corrupt -- the store
/// simply under-reported what it held.
///
/// Every CAS reader that treated a listing as a CENSUS then drew a false conclusion from it, and the
/// two that mattered drew ruinous ones. The GC fold walked the ids the listing returned, so it skipped
/// the omitted records' owner edges AND sealed a cursor above them -- and nothing ever re-reads below a
/// sealed cursor, so those edges were lost permanently: blobs that were still referenced looked
/// unreferenced forever after. Recovery replayed the listing, so a table came back missing an ACKED
/// transaction while looking perfectly healthy.
///
/// The answer is that a listing is a HINT and arithmetic is the census. Ids are dense `1..T`
/// within `(namespace, writer_epoch)` (INV-1), so the next record's id is COMPUTABLE and every record
/// is read by EXACT KEY. A hidden-but-durable contiguous id is then a NON-EVENT -- the walk finds it
/// anyway -- while a genuinely absent expected id is a durable HOLD, never a silent skip.
///
/// This file is that claim stated end to end, against a store that lies exactly the way the real one
/// did. `setListOmissions` names the omitted keys; every other primitive keeps serving them honestly.
/// Each test below asserts the lie changed NOTHING -- not the recovered table, not fsck's verdict.
///
/// Real recovery, real fsck, one lying store.
///
/// The suite name is prefixed `Cas` so the `Cas*` unit-test gate filter covers it.

using namespace DB::Cas;
using namespace DB::Cas::tests;

namespace
{

/// The lying store: LIST omits the named keys, everything else serves them honestly. Composed over
/// `CountingBackend` so the arms whose subject is reclamation can assert on DELETES rather than only on
/// what survived.
using LiarBackend = HintHoleBackendOn<CountingBackend>;

/// The pool's view of a table, rendered so a failing comparison prints something a human can read.
std::map<String, String> refsOf(const PoolPtr & store, const RootNamespace & ns)
{
    std::map<String, String> out;
    for (const auto & [ref_name, resolved] : store->listRefs(ns))
        out[ref_name] = std::to_string(resolved.manifest_id.ref.writer_epoch) + "/"
            + std::to_string(resolved.manifest_id.ref.build_sequence) + "/"
            + std::to_string(resolved.manifest_id.ref.manifest_ordinal);
    return out;
}

/// THE STREAM UNDER TEST, written identically into any backend: five ordinary publishes at
/// `{1,1}..{1,5}`, each pinning its own blob, plus the `_ckpt` a recovering reader starts from. Shared
/// so the oracle arms can seed a lying store and an honest one from the SAME code and compare outcomes
/// rather than compare against a hand-written expectation.
void seedFiveRecordStream(Backend & backend, const Layout & layout, const RootNamespace & ns)
{
    seedPoolMetaForRestart(backend, layout.poolPrefix());
    for (uint64_t i = 1; i <= 5; ++i)
        publishAt(backend, layout, ns, RefTxnId{1, i}, "ref_" + std::to_string(i), i,
                  DB::UInt128(i), /*birth=*/i == 1);
    writeRecoverableCkptForRawFixture(backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = RefTxnId{1, 5},
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });
}

/// The exact defect shape: ids 3 and 4 invisible while the LATER id 5 is visible.
std::vector<String> hiddenMiddleOf(const Layout & layout, const RootNamespace & ns)
{
    return {layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 3}),
            layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 4})};
}

PoolConfig recoveryPoolConfig()
{
    PoolConfig config;
    config.pool_prefix = "p";
    config.server_root_id = "test";
    config.server_id = DB::UInt128(1);
    /// No background publication: a threshold-triggered snapshot would move the base under the
    /// comparison these tests make about what recovery reconstructed.
    config.snapshot_log_count_threshold = 1ULL << 40;
    config.snapshot_log_bytes_threshold = 1ULL << 40;
    return config;
}

PoolPtr openRecoveryPool(const std::shared_ptr<LiarBackend> & backend)
{
    seedPoolMetaForRestart(*backend, "p");
    return Pool::open(backend, recoveryPoolConfig());
}

}

/// RECOVERY, UNDER THE SAME LIE, AGAINST AN HONEST ORACLE. The comparison is against a second pool
/// seeded by the SAME code over a store that does not lie -- not against a hand-written expectation,
/// which could encode the same mistake the code makes.
TEST(CASListLiarEndToEnd, RecoveryUnderTheSameLieReconstructsExactlyTheTruth)
{
    const Layout layout("p");
    const RootNamespace ns{"00/recover@cas@"};

    auto honest_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*honest_backend, layout, ns);
    auto honest = openRecoveryPool(honest_backend);
    const std::map<String, String> truth = refsOf(honest, ns);

    auto lying_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*lying_backend, layout, ns);
    lying_backend->setListOmissions(hiddenMiddleOf(layout, ns));
    auto lying = openRecoveryPool(lying_backend);
    const std::map<String, String> recovered = refsOf(lying, ns);

    /// Recovery reads every record by exact key and asks no listing what to read next, and an existing
    /// pool reopens on the exact read of `_pool_meta` alone -- so nothing above ever LISTed the stream.
    /// That is the point, but it also means the lie has to be PROVEN in effect here, or the comparison
    /// below would pass against a store that hid nothing.
    {
        DB::Cas::tests::OperationForTest op(*lying_backend);
        std::set<String> listed;
        (*op).forEachListedKey(layout.namespaceStreamPrefix(fixture::fixtureLife(ns)),
                               [&](const ListedKey & key) { listed.insert(key.key); return true; },
                               Retry::standard());
        for (const String & hidden : hiddenMiddleOf(layout, ns))
            ASSERT_FALSE(listed.contains(hidden)) << "the store was told to hide " << hidden << " and did not";
    }
    ASSERT_GT(lying_backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    EXPECT_EQ(truth.size(), 5u) << "the oracle itself must see all five published refs";
    EXPECT_EQ(recovered, truth)
        << "a table recovered under an omitting listing must be byte-identical to the truth; the "
           "blocker's recovery came back missing an ACKED transaction and looked healthy";
}

/// ===================== FSCK =====================
///
/// fsck runs two checkpoint-grounded passes over a namespace's ref stream.
///
///   * `checkRefStream` walks arithmetically by exact key. An omitted-but-durable record is a
///     non-event to it, exactly as it is to the GC fold. That is the pass the arms below pin.
///   * the reachability pass uses the same catalog row and exact `_ckpt` to recover the ref table
///     without stream enumeration.
///
/// Both arms are written against an HONEST TWIN seeded by the same code, not against hand-written
/// expectations: the claim is "identical to the truth", and a pass that quietly examined fewer records
/// would satisfy a hand-written "clean" just as well.

TEST(CASListLiarEndToEnd, FsckArithmeticStreamAuditIsUnmovedByAHiddenMiddle)
{
    const Layout layout("p");
    const RootNamespace ns{"00/fsck@cas@"};

    auto honest_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*honest_backend, layout, ns);
    auto honest = openRecoveryPool(honest_backend);
    const FsckReport truth = runFsck(*honest, /*detail=*/true);

    auto lying_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*lying_backend, layout, ns);
    lying_backend->setListOmissions(hiddenMiddleOf(layout, ns));
    auto lying = openRecoveryPool(lying_backend);
    const FsckReport under_lie = runFsck(*lying, /*detail=*/true);

    ASSERT_GT(lying_backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";

    EXPECT_TRUE(truth.clean()) << "the oracle itself must be clean, or the comparison means nothing";
    EXPECT_GT(truth.ref_records_walked, 0u);
    EXPECT_GT(truth.reachable, 0u)
        << "the honest oracle must recover at least one live object, or reachability equality is vacuous";

    /// The arithmetic pass: a hidden record is a non-event, and no finding is manufactured out of it.
    EXPECT_TRUE(under_lie.clean())
        << "fsck must not manufacture a finding out of an omitted-but-durable record";
    EXPECT_EQ(under_lie.chain_broken, 0u)
        << "a record the listing hid is NOT a broken chain: the walk reads it by exact key";
    EXPECT_EQ(under_lie.dangling, 0u);
    EXPECT_EQ(under_lie.ref_records_walked, truth.ref_records_walked)
        << "the arithmetic walk must read the SAME number of records under the lie";

    EXPECT_EQ(under_lie.unchecked, 0u)
        << "an omitted durable record must not turn a healthy checkpoint-bounded namespace unchecked";
    EXPECT_EQ(under_lie.reachable, truth.reachable)
        << "the reachability recovery must observe the same exact committed frontier under the lie";
}

/// A hidden tail record is the silent variant of the historical residual: a LIST-driven replay could
/// return a plausible but short table. Checkpoint-bounded recovery must produce the honest table even
/// though the list omission is served.
TEST(CASListLiarEndToEnd, FsckReachabilityRecoveryMatchesTruthUnderAHiddenTailTransaction)
{
    const Layout layout("p");
    const RootNamespace ns{"00/fsck_tail@cas@"};
    /// Stage B (Task 4-C): no pin needed -- `seedFiveRecordStream` below calls `publishAt` (draining
    /// into `writeRefLogTxnRaw`), which admits `ns` into each of the two independent backends' own
    /// catalogs itself.

    auto honest_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*honest_backend, layout, ns);
    auto honest = openRecoveryPool(honest_backend);
    const FsckReport truth = runFsck(*honest, /*detail=*/true);

    auto lying_backend = std::make_shared<LiarBackend>();
    seedFiveRecordStream(*lying_backend, layout, ns);
    lying_backend->setListOmissions({layout.refLogKey(fixture::fixtureLife(ns), RefTxnId{1, 5})});
    auto lying = openRecoveryPool(lying_backend);
    const FsckReport under_lie = runFsck(*lying, /*detail=*/true);

    ASSERT_GT(lying_backend->holesServed(), 0u)
        << "the omission was never actually served -- the test would pass vacuously";
    EXPECT_GT(truth.reachable, 0u)
        << "the honest oracle must recover at least one live object, or reachability equality is vacuous";

    /// The arithmetic pass is unmoved here too: it probes `{1,5}` by exact key and finds it.
    EXPECT_EQ(under_lie.chain_broken, 0u);
    EXPECT_EQ(under_lie.ref_records_walked, truth.ref_records_walked)
        << "the arithmetic walk reads the hidden tail by exact key, so it counts the same records";

    EXPECT_EQ(under_lie.unchecked, 0u)
        << "a LIST omission must not make a checkpoint-bounded namespace unchecked";
    EXPECT_EQ(under_lie.reachable, truth.reachable)
        << "the exact committed frontier must include the hidden tail transaction";
}
