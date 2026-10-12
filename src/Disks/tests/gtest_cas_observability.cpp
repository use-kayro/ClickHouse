#include <gtest/gtest.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEvent.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Common/ProfileEvents.h>
#include <Poco/Exception.h>
#include <algorithm>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>

namespace ProfileEvents
{
extern const Event CASMountRenewalAttempts;
extern const Event CASMountRenewalRetries;
extern const Event CASMountRenewalResolved;
extern const Event CASMountRenewalRecovered;
extern const Event CASMountRenewalDeadlineExceeded;
}

using namespace DB::Cas;
using DB::Cas::tests::u128Of;

namespace
{

PoolPtr openPool(std::shared_ptr<InMemoryBackend> & b)
{
    b = std::make_shared<InMemoryBackend>();
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

class RenewalCounterBackend final : public InMemoryBackend
{
public:
    enum class Fault : uint8_t
    {
        None,
        ThrowBefore,
        LandThenThrow,
    };

    Fault fault = Fault::None;

    /// The fault sits on the WRITE PRIMITIVE, and only on a CONDITIONAL one: the renewal issues
    /// `op.replace`, which reaches the store here, and a create on the same key must not consume the
    /// one-shot fault.
    std::expected<String, RawConflict> write(
        const String & key,
        const String & bytes,
        const std::optional<String> & expected_value,
        TransportAccess & access) override
    {
        if (!expected_value)
            return InMemoryBackend::write(key, bytes, expected_value, access);

        const Fault current = std::exchange(fault, Fault::None);
        if (current == Fault::ThrowBefore)
            throw Poco::TimeoutException("injected renewal timeout before commit");

        auto result = InMemoryBackend::write(key, bytes, expected_value, access);
        if (current == Fault::LandThenThrow)
            throw Poco::TimeoutException("injected renewal response loss after commit");
        return result;
    }
};

CasRequestBudget renewalCounterBudget(uint32_t /*max_attempts*/ = 2)
{
    return CasRequestBudget{
        .attempt_timeout_ms = 10,
        .lease_safety_margin_ms = 20,
        .connect_timeout_cap_ms = std::nullopt,
    };
}

struct RenewalCounterSnapshot
{
    uint64_t attempts;
    uint64_t retries;
    uint64_t resolved;
    uint64_t recovered;
    uint64_t deadline_exceeded;
};

RenewalCounterSnapshot renewalCounters()
{
    using ProfileEvents::global_counters;
    return {
        .attempts = global_counters[ProfileEvents::CASMountRenewalAttempts].load(),
        .retries = global_counters[ProfileEvents::CASMountRenewalRetries].load(),
        .resolved = global_counters[ProfileEvents::CASMountRenewalResolved].load(),
        .recovered = global_counters[ProfileEvents::CASMountRenewalRecovered].load(),
        .deadline_exceeded = global_counters[ProfileEvents::CASMountRenewalDeadlineExceeded].load(),
    };
}

void expectRenewalCounterDelta(
    const RenewalCounterSnapshot & before,
    const RenewalCounterSnapshot & after,
    uint64_t attempts,
    uint64_t retries,
    uint64_t resolved,
    uint64_t recovered,
    uint64_t deadline_exceeded)
{
    EXPECT_EQ(after.attempts - before.attempts, attempts);
    EXPECT_EQ(after.retries - before.retries, retries);
    EXPECT_EQ(after.resolved - before.resolved, resolved);
    EXPECT_EQ(after.recovered - before.recovered, recovered);
    EXPECT_EQ(after.deadline_exceeded - before.deadline_exceeded, deadline_exceeded);
}

TEST(CASObservability, RenewalCountersHaveExactPhysicalAndLogicalDeltas)
{
    const auto run = [](RenewalCounterBackend::Fault fault, uint64_t attempts, uint64_t retries, uint64_t resolved, uint64_t recovered)
    {
        auto backend = std::make_shared<RenewalCounterBackend>();
        backend->setAttemptTimeoutMs(renewalCounterBudget().attempt_timeout_ms);
        /// Captured by value: `boot_ms` is never mutated in this test, and the Pool can outlive this
        /// lambda's own stack frame (a background publish holds `shared_from_this()`), so a
        /// by-reference capture of a local would dangle.
        const uint64_t boot_ms = 100;
        auto store = Pool::open(backend, PoolConfig{
            .pool_prefix = "renewal-counter-" + std::to_string(attempts) + "-" + std::to_string(resolved),
            .server_root_id = "test",
            .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
            .cas_request_budget = renewalCounterBudget(),
            .boot_ms_fn = []
            {
                return boot_ms;
            },
        });
        backend->fault = fault;
        const RenewalCounterSnapshot before = renewalCounters();
        EXPECT_NO_THROW(store->renewWatermarkOnce());
        const RenewalCounterSnapshot after = renewalCounters();
        expectRenewalCounterDelta(before, after, attempts, retries, resolved, recovered, 0);
    };

    run(RenewalCounterBackend::Fault::None, /*attempts=*/1, /*retries=*/0, /*resolved=*/0, /*recovered=*/0);
    run(RenewalCounterBackend::Fault::ThrowBefore, /*attempts=*/2, /*retries=*/1, /*resolved=*/0, /*recovered=*/1);
    run(RenewalCounterBackend::Fault::LandThenThrow, /*attempts=*/1, /*retries=*/0, /*resolved=*/1, /*recovered=*/1);
}

TEST(CASObservability, ExternalLeaseDeadlineCountsOnceWithoutReconstructingAttempts)
{
    auto backend = std::make_shared<RenewalCounterBackend>();
    backend->setAttemptTimeoutMs(renewalCounterBudget().attempt_timeout_ms);
    /// Held in a shared atomic, not a plain local: this test mutates it below, and the Pool can
    /// outlive this stack frame (a background publish holds `shared_from_this()`), so a by-reference
    /// capture of a local would dangle.
    auto boot_ms = std::make_shared<std::atomic<uint64_t>>(100);
    auto store = Pool::open(backend, PoolConfig{
        .pool_prefix = "renewal-deadline-counter",
        .server_root_id = "test",
        .mount_lease_ttl_ms = std::chrono::milliseconds(1000),
        .cas_request_budget = renewalCounterBudget(),
        .boot_ms_fn = [boot_ms]
        {
            return boot_ms->load();
        },
    });

    /// The fence deadline is 1100 and the safety margin 20, so admission refuses once fewer than
    /// twenty milliseconds of lease remain. At 1090 only ten milliseconds remain, short of the margin
    /// however much a single attempt reserves, so nothing can be started and the logical renewal ends
    /// without reconstructing a sent attempt.
    boot_ms->store(1090);
    const RenewalCounterSnapshot before = renewalCounters();
    EXPECT_THROW(store->renewWatermarkOnce(), DB::Exception);
    const RenewalCounterSnapshot after = renewalCounters();
    expectRenewalCounterDelta(
        before, after, /*attempts=*/0, /*retries=*/0, /*resolved=*/0, /*recovered=*/0,
        /*deadline_exceeded=*/1);
}

}

/// B170/Task 1 (Part A audit events): `PartWriteTxn::stageManifest` writes a part-manifest body but never
/// emitted an audit row for it — the log could not answer "when was this manifest written." Verifies
/// the emitted `ManifestPut` event (exactly once per successful stage).
TEST(CASObservability, StageManifestEmitsManifestPut)
{
    std::shared_ptr<InMemoryBackend> b;
    /// Heap-owned, not a plain local: declaring it before the Pool (ASan 2026-07-09) only protects
    /// against an ordinary same-thread unwind, not a detached background completion holding an extra
    /// `shared_from_this()` that can still be running on another thread after this frame returns.
    auto seen = std::make_shared<DB::Cas::tests::SharedEventLog>();
    auto s = openPool(b);
    s->setEventSink([seen](const CasEvent & e)
    {
        seen->push(e);
    });

    const RootNamespace ns{"srv/tbl@cas@"};
    auto build = s->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/all_0_0_0", .intended_namespace = ns});
    ManifestEntry e;
    e.path = "f";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "AAA";
    const ManifestId id = build->stageManifest({e});
    s->setEventSink(nullptr);

    const std::vector<CasEvent> observed = seen->snapshot();
    EXPECT_EQ(std::count_if(observed.begin(), observed.end(),
        [](const CasEvent & x){ return x.type == CasEventType::ManifestPut; }), 1);

    const auto it = std::find_if(observed.begin(), observed.end(),
        [](const CasEvent & x){ return x.type == CasEventType::ManifestPut; });
    ASSERT_NE(it, observed.end());
    EXPECT_EQ(it->object_kind, CasEventObjectKind::Manifest);
    EXPECT_EQ(it->object_hash, manifestRefDebugString(id.ref));
    EXPECT_FALSE(it->token.empty());
}

/// `PartWriteTxn::abandon` removes a live precommit's owner binding (the correctness-bearing step) but never
/// audited the removal — the log could not distinguish "never precommitted" from "precommitted then
/// abandoned." Verifies the emitted `PrecommitRemoved` event (exactly once, only when a precommit was
/// actually live).
TEST(CASObservability, AbandonEmitsPrecommitRemoved)
{
    std::shared_ptr<InMemoryBackend> b;
    /// Heap-owned, not a plain local: declaring it before the Pool (ASan 2026-07-09) only protects
    /// against an ordinary same-thread unwind, not a detached background completion holding an extra
    /// `shared_from_this()` that can still be running on another thread after this frame returns.
    auto seen = std::make_shared<DB::Cas::tests::SharedEventLog>();
    auto s = openPool(b);

    const RootNamespace ns{"srv/tbl@cas@"};
    auto build = s->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/all_0_0_0", .intended_namespace = ns});
    ManifestEntry e;
    e.path = "f";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "AAA";
    const ManifestId id = build->stageManifest({e});
    build->precommitAdd(ns, "all_0_0_0", id);

    s->setEventSink([seen](const CasEvent & x)
    {
        seen->push(x);
    });
    build->abandon();
    s->setEventSink(nullptr);

    const std::vector<CasEvent> observed = seen->snapshot();
    EXPECT_EQ(std::count_if(observed.begin(), observed.end(),
        [](const CasEvent & x){ return x.type == CasEventType::PrecommitRemoved; }), 1);

    const auto it = std::find_if(observed.begin(), observed.end(),
        [](const CasEvent & x){ return x.type == CasEventType::PrecommitRemoved; });
    ASSERT_NE(it, observed.end());
    EXPECT_EQ(it->namespace_, ns.string());
    EXPECT_EQ(it->ref_name, "all_0_0_0");
    EXPECT_EQ(it->object_kind, CasEventObjectKind::Root);
    EXPECT_EQ(it->object_hash, manifestRefDebugString(id.ref));
}

/// A build that never precommitted has nothing to remove: `abandon` must not fabricate a
/// `PrecommitRemoved` row for a binding that was never live.
TEST(CASObservability, AbandonWithoutPrecommitEmitsNoPrecommitRemoved)
{
    std::shared_ptr<InMemoryBackend> b;
    /// Heap-owned, not a plain local: declaring it before the Pool (ASan 2026-07-09) only protects
    /// against an ordinary same-thread unwind, not a detached background completion holding an extra
    /// `shared_from_this()` that can still be running on another thread after this frame returns.
    auto seen = std::make_shared<DB::Cas::tests::SharedEventLog>();
    auto s = openPool(b);

    const RootNamespace ns{"srv/tbl@cas@"};
    auto build = s->beginPartWrite(PartWriteInfo{.intended_ref = ns.string() + "/all_0_0_0", .intended_namespace = ns});
    ManifestEntry e;
    e.path = "f";
    e.placement = EntryPlacement::Inline;
    e.inline_bytes = "AAA";
    build->stageManifest({e});   /// staged, never precommitted

    s->setEventSink([seen](const CasEvent & x)
    {
        seen->push(x);
    });
    build->abandon();
    s->setEventSink(nullptr);

    const std::vector<CasEvent> observed = seen->snapshot();
    EXPECT_EQ(std::count_if(observed.begin(), observed.end(),
        [](const CasEvent & x){ return x.type == CasEventType::PrecommitRemoved; }), 0);
}

/// Task 3 (Part B, `clickhouse-disks cas-inspect`): `caInspectToJson` is a FREE function (no
/// disk/backend involved) that decodes any CA bucket object at `key` and renders it as JSON, purely
/// by matching `key` against `Layout`'s prefixes/key-shapes and calling the matching `decode*`.
/// These tests drive it directly against real encoder output (one per recognized key shape) plus the
/// unknown-key fail-closed path — the same function the CLI command (`CommandCaInspect.cpp`) calls.

/// The legacy mutable ref-shard object is gone (snapshot+log ref model); inspect now decodes the two
/// immutable ref objects. A `_snap/<id>.proto` renders as a ref-table snapshot...
TEST(CASObservability, CaInspectDecodesRefSnapshotToJson)
{
    using DB::Cas::tests::committedRow;
    using DB::Cas::tests::minimalLiveSnapshot;
    Layout layout("p");
    const RootNamespace ns{"srv/tbl@cas@"};
    const RefTxnId snap_id{1, 7};
    const RefTableSnapshot snap = minimalLiveSnapshot(ns.string(), snap_id,
        {committedRow("all_0_0_0", ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1})});
    const String key = layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), snap_id);
    const String json = caInspectToJson(
        layout, key, encodeRefTableSnapshot(snap), DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("object":"ref_snapshot")"), String::npos) << json;
    EXPECT_NE(json.find(R"("namespace":"srv/tbl@cas@")"), String::npos) << json;
    EXPECT_NE(json.find(R"("snapshot_id":{"writer_epoch":1,"ref_sequence":7})"), String::npos) << json;
    EXPECT_NE(json.find(R"("ref_name":"all_0_0_0")"), String::npos) << json;
    EXPECT_NE(json.find(R"("precommits":[])"), String::npos) << json;
    EXPECT_EQ(json.find("\"lifecycle\""), String::npos)
        << "generation-8 snapshot inspection must not recreate lifecycle state retired from the snapshot DTO";
}

/// ...and a `_log/<txn-id>` renders as a ref-transaction log.
TEST(CASObservability, CaInspectDecodesRefLogToJson)
{
    Layout layout("p");
    const RootNamespace ns{"srv/tbl@cas@"};
    const RefTxnId txn_id{1, 8};
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = txn_id;
    RefOp add;
    add.kind = RefOpKind::OwnerTransition;
    add.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "all_0_0_0",
        ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 1}};
    txn.ops = {add};
    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), txn_id);
    const String json = caInspectToJson(
        layout, key, encodeRefLogTxn(txn), DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find("ref_log"), String::npos);
    EXPECT_NE(json.find("owner_transition"), String::npos);
    EXPECT_NE(json.find("all_0_0_0"), String::npos);
}

TEST(CASObservability, CaInspectDecodesPartManifestToJson)
{
    Layout layout("p");
    const RootNamespace ns{"srv/tbl@cas@"};

    PartManifest m;
    m.ref = ManifestRef{.writer_epoch = 1, .build_sequence = 2, .manifest_ordinal = 3};
    m.root_namespace_id = ns;
    ManifestEntry inline_entry;
    inline_entry.path = "data.bin";
    inline_entry.placement = EntryPlacement::Inline;
    inline_entry.inline_bytes = "hello";
    ManifestEntry blob_entry;
    blob_entry.path = "payload.bin";
    blob_entry.placement = EntryPlacement::Blob;
    blob_entry.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of("payload.bin"))};
    blob_entry.blob_size = 5;
    m.entries = {inline_entry, blob_entry};
    m.payload_digest = computePayloadDigest(m);

    const ManifestId id{.root_namespace = ns, .ref = m.ref};
    const String key = layout.manifestKey(id);
    const String json = caInspectToJson(layout, key, encodePartManifest(m));
    EXPECT_NE(json.find("\"root_namespace_id\""), String::npos);
    EXPECT_NE(json.find("data.bin"), String::npos);
    EXPECT_NE(json.find("\"manifest_ordinal\":3"), String::npos);
    /// `EntryPlacement` renders as its full wire word (`inline`/`blob`), not the enumerator spelling.
    EXPECT_NE(json.find(R"("placement":"inline")"), String::npos) << json;
    EXPECT_NE(json.find(R"("placement":"blob")"), String::npos) << json;
}

TEST(CASObservability, CaInspectDecodesMountLeaseToJson)
{
    Layout layout("p");
    MountLease lease;
    lease.server_uuid = hexToU128("000000000000000000000000000000ab");
    lease.writer_epoch = 5;
    lease.write_attempt_id = hexToU128("00112233445566778899aabbccddeeff");
    lease.hostname = "host1";
    lease.pid = 123;

    const String key = layout.mountKey("srid1");
    const String json = caInspectToJson(layout, key, encodeMountLease(lease));
    EXPECT_NE(json.find("\"writer_epoch\":5"), String::npos);
    EXPECT_NE(json.find("\"write_attempt_id\":\"00112233445566778899aabbccddeeff\""), String::npos);
    EXPECT_NE(json.find("host1"), String::npos);
}

/// `ObjectKind`/`ProvenanceOp` render as their full wire words, not the enumerator spelling. Loops
/// over every `ProvenanceOp` value so each one ends up pinned, not just whichever one a single case
/// would have picked.
TEST(CASObservability, CaInspectDecodesEnvelopeHeaderWithEveryProvenanceOpWord)
{
    Layout layout("p");
    const BlobRef ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(u128Of("envelope-inspect"))};
    const String key = layout.blobKey(ref);

    const std::vector<std::pair<ProvenanceOp, String>> ops = {
        {ProvenanceOp::Other, "other"},
        {ProvenanceOp::Insert, "insert"},
        {ProvenanceOp::Merge, "merge"},
        {ProvenanceOp::Mutation, "mutation"},
        {ProvenanceOp::Attach, "attach"},
        {ProvenanceOp::Repack, "repack"},
    };
    for (const auto & [op, word] : ops)
    {
        EnvelopeHeader h;
        h.kind = ObjectKind::Blob;
        h.provenance = Provenance{.op = op};
        const String bytes = encodeEnvelopeHeader(h, 256);

        const String json = caInspectToJson(layout, key, bytes);
        EXPECT_NE(json.find(R"("kind":"blob")"), String::npos) << json;
        EXPECT_NE(json.find("\"op\":\"" + word + "\""), String::npos) << json;
    }
}

TEST(CASObservability, CaInspectUnknownKeyThrows)
{
    Layout layout("p");
    EXPECT_THROW(caInspectToJson(layout, "p/not/a/ca/object", "xxxx"), DB::Exception);   /// BAD_ARGUMENTS
}
