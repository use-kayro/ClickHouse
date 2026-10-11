#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedSettings.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasServerRoot.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

/// THE RETIREMENT SWEEP's executable half.
///
/// A mechanism of the pre-v9 ref protocol lost its premise when the ref stream became a contiguous,
/// arithmetically-walkable chain, and this file is where its retirement is proved rather than asserted
/// in a comment:
///
///   THE MATERIALIZATION GRACE (`T_mat`). A post-reclaim sleep, long enough for a straggler
///      conditional `PUT` from a dying epoch to land or exhaust its retries BEFORE the successor
///      trusted its recovery LISTINGS. Recovery does not trust listings; it closes every dead epoch
///      with an in-band `EpochSeal` written as a conditional create, and the straggler's own create
///      loses to it. The wait is deleted outright, setting and all -- the feature never shipped, so
///      there is no config to protect and no parsed-but-inert period to serve.
///
/// The retirement rationale (premise / verdict / replacement / evidence, one row per retired item)
/// is captured in `docs/en/antalya/cas/architecture/design-history.md`.

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::SharedWaitLog;
using DB::Cas::tests::u128Of;

namespace
{

/// Every write of a matching key is a lost response, for as long as `fault_key_substr` names one. It
/// has to be every one: the request engine settles an ambiguity by an exact read and then reissues, so
/// a counted fault is outlived by the reissues and the write commits -- the difference between the
/// wedge this fixture needs and a clean commit. Clearing `fault_key_substr` disarms it.
class UnresolvedPutBackend final : public InMemoryBackend
{
public:
    String fault_key_substr;
    /// How many matching writes actually hit the fault, so a caller can prove the engine reissued
    /// rather than infer it from a give-up that a non-retrying policy would also reach.
    int fault_hits = 0;

    std::expected<String, RawConflict> write(const String & key, const String & bytes,
                                             const std::optional<String> & expected_value,
                                             TransportAccess & access) override
    {
        if (!fault_key_substr.empty() && key.find(fault_key_substr) != String::npos)
        {
            ++fault_hits;
            throw Poco::TimeoutException("UnresolvedPutBackend: simulated ambiguous result (response lost)");
        }
        return InMemoryBackend::write(key, bytes, expected_value, access);
    }
};

/// GC's fence-out applied directly to the mount lease: preserve the body, set `gc_fenced`, bump `seq`
/// (token-guarded). A subsequent `tryRemountOnce` then reclaims a fresh incarnation.
bool headExists(Backend & backend, const String & key)
{
    DB::Cas::tests::OperationForTest op(backend);
    return (*op).head(key, Retry::standard()).has_value();
}

void fenceOutMount(Backend & backend, const String & mount_key)
{
    DB::Cas::tests::OperationForTest op(backend);
    const auto got = (*op).read(mount_key, Retry::standard());
    ASSERT_TRUE(got.has_value());
    MountLease m = decodeMountLease(got->bytes);
    m.gc_fenced = true;
    m.seq += 1;
    ASSERT_TRUE(std::holds_alternative<Committed>((*op).replace(mount_key, encodeMountLease(m), got->etag, Retry::standard())));
}

/// Publish one part `ref` with a single content blob whose payload is `payload`.
ManifestId publishOneBlobPart(const PoolPtr & s, const RootNamespace & ns, const String & ref,
                              const String & payload)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    auto build = s->beginPartWrite(info);

    ManifestEntry e;
    e.path = "data.bin";
    e.placement = EntryPlacement::Blob;
    e.ref = BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of(payload))};
    e.blob_size = payload.size();

    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

/// Every ref-log key of `ns` currently listed, in key order.
std::set<String> listRefLogKeys(Backend & b, const Layout & l, const RootNamespace & ns)
{
    DB::Cas::tests::OperationForTest op(b);
    std::set<String> out;
    String cursor;
    while (true)
    {
        const ListPage page = (*op).list(l.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)), cursor, 1000, Retry::standard());
        for (const ListedKey & k : page.keys)
            if (const auto parsed = l.parseRefObjectKey(k.key); parsed && parsed->kind == RefObjectKind::Log)
                out.insert(k.key);
        if (page.next_cursor.empty())
            break;
        cursor = page.next_cursor;
    }
    return out;
}

/// The greatest ref-log id of `ns` in the current listing.
RefTxnId greatestLoggedId(Backend & b, const Layout & l, const RootNamespace & ns)
{
    RefTxnId best{};
    for (const String & key : listRefLogKeys(b, l, ns))
        if (const auto parsed = l.parseRefObjectKey(key); parsed && best < parsed->txn_id)
            best = parsed->txn_id;
    return best;
}

Poco::AutoPtr<Poco::Util::XMLConfiguration> makeDiskConfig(const std::string & inner)
{
    std::istringstream iss("<clickhouse><disk>" + inner + "</disk></clickhouse>");
    return new Poco::Util::XMLConfiguration(iss);
}

}


/// ==================== item 2: the materialization grace, retired ====================

/// THE MECHANISM THAT REPLACED THE WAIT, tested directly. A ref lane is left holding an UNDECIDED
/// conditional `PUT` when the fence trips -- the exact state `T_mat` was introduced to wait out. The
/// remount proceeds with no wait at all, the next recovery closes the dead epoch with an in-band
/// `EpochSeal` at the slot the straggler would have taken, and the straggler's own conditional create
/// then LOSES to it.
///
/// The assertion is the conflict itself, not the absence of damage: "nothing bad happened" would also
/// be true of a run where the straggler simply never arrived.
TEST(CASRetirementSweep, AStragglerFromTheDyingEpochLosesItsCreateToTheRecoverySeal)
{
    CasRequestBudget budget;
    budget.attempt_timeout_ms = 100;
    budget.lease_safety_margin_ms = 100;

    auto backend = std::make_shared<UnresolvedPutBackend>();
    /// What the request engine reserves per attempt is the BACKEND's attempt timeout, not the budget
    /// field alone; pair the two so the mount lease's admission arithmetic sees what the budget claims.
    backend->setAttemptTimeoutMs(budget.attempt_timeout_ms);
    /// Held in shared, heap-owned state, not plain locals: the hooks below mutate them, and the Pool
    /// can outlive this stack frame (a background publish holds `shared_from_this()`), so a
    /// by-reference capture of a local would dangle.
    auto fake_boot = std::make_shared<std::atomic<uint64_t>>(1'000'000);
    auto waits = std::make_shared<SharedWaitLog>();
    /// The append's own retry clock and its sleep log, installed further down; declared here, before
    /// the store, because the store's teardown still calls the now-function they back.
    auto fake_retry = std::make_shared<std::atomic<uint64_t>>(0);
    auto retry_sleeps = std::make_shared<SharedWaitLog>();
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "p", .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(30000),
        .cas_request_budget = budget,
        .boot_ms_fn = [fake_boot]
        {
            return fake_boot->load();
        },
        .wait_sleep_fn = [fake_boot, waits](uint64_t ms)
        {
            *fake_boot += ms;
            waits->push(ms);
        },
    });
    ASSERT_TRUE(store);
    const Layout & layout = store->layout();
    const RootNamespace ns{"srv/straggler"};
    /// Pin to the transition life before the first real touch, and give that raw `Live` row the exact
    /// empty checkpoint authority that production birth would have published before recovery.
    DB::Cas::tests::fixture::admitLive(*backend, layout, ns);
    DB::Cas::tests::writeRecoverableCkptForRawFixture(*backend, layout, ns, RefCkpt{
        .life_epoch = 1,
        .committed_through = std::nullopt,
        .checkpoint_snapshot_id = std::nullopt,
        .last_epoch_seal = std::nullopt,
    });

    publishOneBlobPart(store, ns, "x", "straggler-payload");
    ASSERT_EQ(store->liveWriterEpoch(), 1u);

    /// Drive the next ref-log append into the Unresolved/wedge outcome: every attempt it makes fails
    /// ambiguously, so this process can never learn whether its conditional PUT landed. That
    /// undecidability is the whole reason the resolution is a conditional CREATE and not a GET. The
    /// give-up is the append's own retry window -- paced on ITS OWN virtual clock, separate from
    /// `fake_boot` (the mount fence's), so the standard policy's full window is available to reissue
    /// against rather than being cut short by the 30s lease `fake_boot` also measures.
    store->setCasRequestNowFnForTest([fake_retry]
    {
        return fake_retry->load();
    });
    store->setCasRetrySleepForTest([fake_retry, retry_sleeps](uint64_t ms)
    {
        *fake_retry += ms + 1;
        retry_sleeps->push(ms);
    });
    backend->fault_key_substr = layout.namespaceStreamPrefix(DB::Cas::tests::fixture::fixtureLife(ns)) + "_log/";
    DB::Cas::tests::expectThrowsCode(DB::ErrorCodes::NETWORK_ERROR, [&] { store->dropRef(ns, "x"); });
    ASSERT_TRUE(store->refLaneWedgedForTest(ns));
    EXPECT_GT(backend->fault_hits, 1)
        << "the append must have reissued more than once against the persistent fault before giving up "
           "-- a single attempt would not distinguish this from a non-retrying policy";
    EXPECT_GT(retry_sleeps->size(), 1u) << "more than one paced retry must have occurred before the give-up";
    EXPECT_GT(fake_retry->load(), 0u) << "the retry clock must have advanced past the policy's own deadline";

    /// The id the straggler would occupy: one past the greatest record that is actually durable in the
    /// dying epoch. That is also, by construction, where the recovery seal goes.
    const RefTxnId greatest = greatestLoggedId(*backend, layout, ns);
    ASSERT_EQ(greatest.writer_epoch, 1u);
    const RefTxnId straggler_slot{greatest.writer_epoch, greatest.ref_sequence + 1};
    ASSERT_FALSE(headExists(*backend, layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), straggler_slot)))
        << "the slot must be empty before recovery -- otherwise this test proves nothing about who won";

    /// Fence and remount. No wait: this is the case that used to cost 30 seconds.
    *fake_boot += 30001;
    fenceOutMount(*backend, layout.mountKey("test"));
    ASSERT_TRUE(store->tryRemountOnce());
    ASSERT_EQ(store->liveWriterEpoch(), 2u);
    EXPECT_TRUE(waits->empty())
        << "the remount blocked on an operator-configured wait; the grace is supposed to be gone";

    /// Touch the namespace so it re-recovers under the new epoch: the walk closes epoch 1 in band. The
    /// ref itself is still THERE -- the removal's PUT was the undecided one and (in this fixture) never
    /// landed, which is precisely the state that leaves a straggler outstanding.
    backend->fault_key_substr.clear();
    EXPECT_EQ(store->listRefs(ns).size(), 1u);
    ASSERT_TRUE(headExists(*backend, layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), straggler_slot)))
        << "recovery did not seal the dead epoch at the slot a straggler would take -- without that "
           "seal there is nothing for the straggler's create to lose to";

    /// THE STRAGGLER ARRIVES. Its conditional create is refused, whenever it happens to land.
    DB::Cas::tests::OperationForTest straggler_op(backend);
    const WriteResult put = (*straggler_op).create(
        layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), straggler_slot), "ghost-body", Retry::once());
    EXPECT_TRUE(std::holds_alternative<Conflict>(put))
        << "the dying epoch's straggler overwrote (or joined) a slot the successor had already sealed";
}

/// THE FAIL-CLOSE THAT REPLACES THE DELETED SETTING. `materialization_grace_ms` is gone from the
/// settings table outright -- no parsed-but-inert period, no deprecation log -- so a config that still
/// asks for the wait is refused at disk open by the generic unknown-key path, loudly, instead of being
/// silently ignored by a server that no longer honours it. The feature never shipped, so there is no
/// deployed config this can break.
TEST(CASRetirementSweep, AConfigStillAskingForTheMaterializationGraceIsRejected)
{
    auto cfg = makeDiskConfig(
        "<cas_server_root_id>srv1</cas_server_root_id><cas_materialization_grace_ms>30000</cas_materialization_grace_ms>");
    DB::ContentAddressedSettings s;
    EXPECT_THROW(
        s.loadFromConfig(*cfg, "disk", "/scratch", "/scratch", [](const std::string & v) { return v; }),
        DB::Exception)
        << "a retired setting must fail the disk open, not be quietly accepted and ignored";
}
