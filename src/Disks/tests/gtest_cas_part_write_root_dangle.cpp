#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/tests/cas_test_helpers.h>

#include <string>

using namespace DB::Cas;
using DB::Cas::tests::idOf;
using DB::Cas::tests::u128Of;

namespace
{

/// Mirrors the B140 repro.
PoolPtr openTestPool(std::shared_ptr<InMemoryBackend> & out_backend)
{
    out_backend = std::make_shared<InMemoryBackend>();
    return Pool::open(out_backend, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

ManifestEntry blobEntry(const String & name, const String & payload)
{
    ManifestEntry e;
    e.path = name;
    e.placement = EntryPlacement::Blob;
    e.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(u128Of(payload))};

    e.blob_size = payload.size();
    return e;
}

}

/// B171 INV-COMMIT-FAILCLOSED: even if the build-root precommit is PREMATURELY RECLAIMED mid-build
/// (e.g. a live build whose watermark renewer froze and was falsely judged dead), the real commit must
/// NEVER publish a table ref over a missing dependency. It must fail closed — abort — never dangle.
///
/// Setup mirrors the primary repro: PartWriteTxn A publishes refA -> t1 -> { data.bin: P } then retires; PartWriteTxn
/// B adopts P, assembles t2 -> { other.bin: P }, and precommits t2 (a real build-root edge now protects
/// P). We then SIMULATE the premature reclaim by manually dropping the build-root ref (as GC's reclaim
/// would) AND dropping refA, then renew the watermark and run GC to fixpoint. With P's only protection
/// (the precommit edge) gone and its owner retired, GC deletes P. PartWriteTxn B's publish must now ABORT
/// (`checkAndResolveDeps` finds the adopted blob absent and not re-creatable) instead of committing a dangle.
TEST(CASPartWriteTxnRootDangle, PrematureReclaimCommitFailsClosed)
{
    std::shared_ptr<InMemoryBackend> backend;
    auto s = openTestPool(backend);
    const RootNamespace ns{"test/tbl"};
    const String P = "shared-blob-payload-P-reclaim";

    /// PartWriteTxn A: upload P, publish refA -> manifest, retire A so min_active_build_sequence advances past it.
    {
        PartWriteInfo info;
        info.intended_ref = ns.string() + "/refA";
        auto a = s->beginPartWrite(info);
        const ManifestId id = a->stageManifest({blobEntry("data.bin", P)});
        a->precommitAdd(ns, "refA", id);
        a->putBlob(idOf(P), BlobSource::fromString(P));
        a->promote(ns, "refA", a->buildId(), id);
    }
    s->renewWatermarkOnce();

    /// PartWriteTxn B: adopt P via tokenless evidence, assemble its manifest, precommitAdd it (the precommit
    /// owner binding for refB now protects P with a +1 fold edge).
    PartWriteInfo binfo;
    binfo.intended_ref = ns.string() + "/refB";
    auto b = s->beginPartWrite(binfo);
    const ManifestEntry pe2 = blobEntry("other.bin", P);
    b->adoptEvidence(pe2);
    const ManifestId t2 = b->stageManifest({pe2});
    b->precommitAdd(ns, "refB", t2);

    /// SIMULATE a premature reclaim having already collected P: had the precommit binding been wrongly
    /// reclaimed with no other owner, GC would condemn+delete P's closure. Reproduce that END STATE
    /// directly by deleting P's blob object. (The durable ref-log stream is owned by the live writer, so a
    /// RAW removal append would collide with the writer's own `RefTxnId` sequence allocation on the next
    /// flush; the property under test is the COMMIT gate's fail-closed behavior against a missing
    /// dependency, not the reclaim mechanics -- so we go straight to the reclaimed state.)
    DB::Cas::tests::OperationForTest reclaim_op(*backend);
    {
        const String pkey = s->layout().blobKey(idOf(P));
        const auto h = (*reclaim_op).head(pkey, Retry::once());
        ASSERT_TRUE(h.has_value()) << "P must be present before the simulated reclaim";
        ASSERT_EQ((*reclaim_op).remove(pkey, h->etag, Retry::once()), Removal::Removed);
    }
    /// Drop the source ref too (the state a real premature reclaim leaves: P unprotected and gone).
    s->dropRef(ns, "refA");
    s->renewWatermarkOnce();

    /// The shared blob must be GONE (the premature reclaim collected it).
    ASSERT_FALSE((*reclaim_op).head(s->layout().blobKey(idOf(P)), Retry::once()).has_value())
        << "premature-reclaim setup invalid: P should have been collected after losing its precommit";

    /// §4 manifest-trust (test name is legacy — B171 INV-COMMIT-FAILCLOSED for an ADOPTED leaf now moves to
    /// fsck): P is a committed-source adopted leaf, so PartWriteTxn B's promote TRUSTS it (no HEAD/loadMeta probe)
    /// and COMMITS refB. On the real reuse/relink path this dangle is UNREACHABLE: precommitAdd durably
    /// appended refB's Precommit OwnerTransition (CasPartWriteTxn.cpp precommitAdd) BEFORE promote, and promote
    /// re-proves that edge is the LIVE owner (WPromote owner==bld) BEFORE trusting P — so P has in-degree
    /// >= 1 and GC (the sole deleter) cannot collect it. This test injects the collection DIRECTLY (a raw
    /// deleteExact while refB's precommit is still live), which the live-precommit invariant excludes. So
    /// promote SUCCEEDS; the dangle is not prevented at promote but DETECTED by fsck (the backstop).
    ASSERT_NO_THROW(b->promote(ns, "refB", b->buildId(), t2))
        << "§4: an adopted leaf is trusted at promote — a missing dependency is not re-observed here";

    /// Trust never fabricates the missing blob (it never touches P); refB IS committed (naming absent P).
    ASSERT_FALSE((*reclaim_op).head(s->layout().blobKey(idOf(P)), Retry::once()).has_value())
        << "trust never fabricates the missing blob — P stays absent";
    ASSERT_TRUE(s->resolveRef(ns, "refB").has_value())
        << "§4: refB commits under trust (the D4 trade-off); the dangle is caught by fsck, below";

    /// THE BACKSTOP (INV-NO-DANGLE-via-fsck): fsck's reachable-but-absent scan reports refB's absent P as
    /// dangling — this is where the B171 guarantee lives under §4. Detection moved, it did not disappear.
    const FsckReport rep = runFsck(*s, /*detail=*/true);
    EXPECT_GE(rep.dangling, 1u)
        << "§4 D4 backstop: refB committed over the deleted P; fsck must report it dangling (dangling="
        << rep.dangling << ", reachable=" << rep.reachable << ")";
}

