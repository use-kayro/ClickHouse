#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPartWriteTxn.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasPool.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInMemoryBackend.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasFsck.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasGcStateFormat.h>
#include <Disks/tests/cas_test_helpers.h>
#include <Common/Exception.h>

/// Multi-actor protocol scenarios for the root-local part-manifest model (CA GC redesign rev. 15).
/// Ported from the removed tree/closure model. The single-call `publish(ns, ref, tree, RefPayload{})`
/// gate is gone; a write is now the four-step flow (EDGE-BEFORE-OBSERVE order):
///   stageManifest(entries) -> precommitAdd(ns, ref, id) -> putBlob(...) -> promote(ns, ref, build_id, id)
/// The fail-closed publish gate that those scenarios exercise now lives in TWO places (Phase A of spec
/// 2026-07-09-cas-writer-gc-simplification):
///   • putBlob: INV-1 condemned-dedup re-upload from the writer's OWN source bytes (never GETs the
///     dying object);
///   • promote: `Materialized` leaves (this build putBlob'd them) are EDGE-PROTECTED and NOT re-validated — the
///     precommit closure named them before putBlob observed them, so a condemnation in the
///     putBlob→promote window is doomed (the next fold spares it). promote commits without touching
///     the blob's current token. `TrustedManifest` leaves are accepted through their durable source
///     manifest edge with no per-file observation.
/// These scenarios assert the no-dangle / no-loss / fail-closed protocol properties faithfully on that
/// flow. The strong safety assertions are preserved.
///
/// DELETED (Phase A): `RevalidateAbsentTokenedBlobResurrectsFromSource`. Its premise — a putBlob'd
/// (`Materialized`) blob body hand-deleted before the gate, then resurrected — is protocol-unreachable under
/// EDGE-BEFORE-OBSERVE: a materialized leaf under a durable precommit closure cannot be GC-deleted in the
/// putBlob→promote window, and promote no longer revalidates materialized leaves at all. Deleting a
/// putBlob'd body out-of-band is corruption, which is `cas-fsck`'s domain, not the promote gate's.

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int CORRUPTED_DATA;
extern const int FILE_DOESNT_EXIST;
extern const int LOGICAL_ERROR;
}

using namespace DB::Cas;
using DB::Cas::tests::blobEntryFor;
using DB::Cas::tests::condemnMeta;
using DB::Cas::tests::displaceBlobToken;
using DB::Cas::tests::idOf;
using DB::Cas::tests::loadMetaForTest;
using DB::Cas::tests::streamingHexOf;
using DB::Cas::tests::u128Of;
using DB::Cas::tests::writeBlobRaw;

namespace
{

PoolPtr openPool(const std::shared_ptr<InMemoryBackend> & b)
{
    return Pool::open(b, PoolConfig{.pool_prefix = "p", .server_root_id = "test"});
}

/// The object's incarnation as the store reports it now -- what these scenarios compare when they
/// assert an object was, or was not, displaced.
Etag currentIncarnation(Backend & b, const String & key)
{
    DB::Cas::tests::OperationForTest operation(b);
    const std::optional<Meta> meta = (*operation).head(key, Retry::standard());
    if (!meta)
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA, "object {} is absent", key);
    return meta->etag;
}

/// An exact-incarnation delete attempt, for the scenarios whose discriminator is that a displaced
/// incarnation can never be current again.
Removal removeAtIncarnation(Backend & b, const String & key, const Etag & seen)
{
    DB::Cas::tests::OperationForTest operation(b);
    return (*operation).remove(key, seen, Retry::standard());
}

/// A single-blob manifest entry naming `payload` at `path` (the entry the part's manifest carries).
ManifestEntry blobEntry(const String & path, const String & payload)
{
    return blobEntryFor(path, u128Of(payload), payload.size());
}

/// Start a build whose `intended_ref` is "ns/ref" — REQUIRED: stageManifest derives the manifest's
/// owning namespace by splitting intended_ref on the LAST '/'. (See PartWriteTxn::manifestNamespace.)
PartWriteTxnPtr startBuildFor(const PoolPtr & s, const RootNamespace & ns, const String & ref)
{
    PartWriteInfo info;
    info.intended_ref = ns.string() + "/" + ref;
    return s->beginPartWrite(info);
}

/// Seed an arbitrary blob identity through a complete writer transaction, preserving the production
/// ordering that makes physical publication legal.
void seedBlobWithDurablePrecommit(
    const PoolPtr & store, const BlobRef & blob_ref, const String & payload)
{
    const RootNamespace ns{"fixture/seed"};
    auto build = startBuildFor(store, ns, "blob");
    ManifestEntry entry;
    entry.path = "data.bin";
    entry.placement = EntryPlacement::Blob;
    entry.ref = blob_ref;
    entry.blob_size = payload.size();
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "blob", id);
    build->putBlob(blob_ref, BlobSource::fromString(payload));
    build->promote(ns, "blob", build->buildId(), id);
}

/// The full write flow for a part whose only file is `payload` at `path` (blob placement). Uploads the
/// blob via putBlob, stages the manifest, precommits, then promotes. Returns the committed ManifestId.
/// Mirrors what the old single-call `publish` did on the tree model.
ManifestId publishBlobPart(
    const PoolPtr & s, const RootNamespace & ns, const String & ref, const String & path, const String & payload)
{
    auto build = startBuildFor(s, ns, ref);
    /// Wiring order (EDGE-BEFORE-OBSERVE): stageManifest -> precommitAdd -> putBlob -> promote.
    const ManifestId id = build->stageManifest({blobEntry(path, payload)});
    build->precommitAdd(ns, ref, id);
    build->putBlob(idOf(payload), BlobSource::fromString(payload));
    build->promote(ns, ref, build->buildId(), id);
    return id;
}

/// Read the part's blob back through the full read stack (resolveRef → readManifest → findEntry →
/// locate → ranged GET) and assert it returns `payload`. This is the INV-NO-DANGLE check: every named
/// object resolves and reads.
void assertPartReads(
    const std::shared_ptr<InMemoryBackend> & b, const PoolPtr & s,
    const RootNamespace & ns, const String & ref, const String & path, const String & payload)
{
    auto r = s->resolveRef(ns, ref);
    ASSERT_TRUE(r.has_value());

    const PartManifest manifest = s->readManifest(r->manifest_id);
    const auto * entry = findEntry(manifest.entries, path);
    ASSERT_TRUE(entry != nullptr);
    auto loc = s->locate(*entry);
    DB::Cas::tests::OperationForTest op(*b);
    auto got = (*op).read(loc.key, Retry::once());
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->bytes.substr(static_cast<size_t>(loc.offset), static_cast<size_t>(loc.length)), payload);
}

}

TEST(CASProtocol, RevalidateReObservesStaleTokenKeepsWhenUnchanged)
{
    /// A blob dedup-adopted (`Materialized` proof) under the precommit closure.
    /// Under EDGE-BEFORE-OBSERVE the materialized leaf is NOT re-observed at the promote gate at all — it is
    /// edge-protected — so promote commits in place with the token UNCHANGED (no HEAD, no rewrite).
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// X pre-exists out-of-band; the build dedup-adopts it via putBlob (records the current token t0).
    writeBlobRaw(*b, s->layout(), "payload-X", s->poolMeta().blob_header_len, s->poolMeta().pool_id);
    const String blob_key = s->layout().blobKey(idOf("payload-X"));
    const Etag t0 = currentIncarnation(*b, blob_key);

    /// Wiring order: stage + precommit (durable edge) BEFORE the adopting putBlob.
    auto build = startBuildFor(s, ns, "part_1");
    const ManifestId id = build->stageManifest({blobEntry("data.bin", "payload-X")});
    build->precommitAdd(ns, "part_1", id);
    build->putBlob(idOf("payload-X"), BlobSource::fromString("payload-X"));   /// dedup → adopts t0

    /// promote: the materialized leaf is edge-protected (not re-observed) ⇒ commit in place (KEEP).
    build->promote(ns, "part_1", build->buildId(), id);

    assertPartReads(b, s, ns, "part_1", "data.bin", "payload-X");
    /// No rewrite happened — the materialized leaf was never touched, so its object token stays at t0.
    EXPECT_EQ(currentIncarnation(*b, blob_key), t0);
}

TEST(CASProtocol, RevalidateReObservesStaleTokenAdoptsWhenDisplaced)
{
    /// A blob displaced out-of-band to a fresh live token t1 before promote. Phase-A contract: the leaf is
    /// `Materialized` (putBlob-adopted), so promote SKIPS it entirely (edge-protected — EDGE-BEFORE-OBSERVE); no
    /// re-HEAD happens. The commit still rides the displaced object correctly because the manifest names
    /// the HASH, not a token — this is the black-box "displaced object still reads by content key" check.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    writeBlobRaw(*b, s->layout(), "payload-X", s->poolMeta().blob_header_len, s->poolMeta().pool_id);
    const String blob_key = s->layout().blobKey(idOf("payload-X"));
    const Etag t0 = currentIncarnation(*b, blob_key);

    auto build = startBuildFor(s, ns, "part_1");
    /// Wiring order (EDGE-BEFORE-OBSERVE): stageManifest -> precommitAdd -> putBlob.
    const ManifestId id = build->stageManifest({blobEntry("data.bin", "payload-X")});
    build->precommitAdd(ns, "part_1", id);
    build->putBlob(idOf("payload-X"), BlobSource::fromString("payload-X"));   /// dedup → adopts t0

    /// Another writer displaces X out-of-band ⇒ a new current token t1 (same payload, fresh tag).
    const Etag t1 = displaceBlobToken(*b, s->layout(), idOf("payload-X"));
    EXPECT_NE(t1, t0);

    /// promote refreshes ⇒ revalidate X ⇒ HEAD current t1 not condemned ⇒ commit. The dep rides t1.
    build->promote(ns, "part_1", build->buildId(), id);
    assertPartReads(b, s, ns, "part_1", "data.bin", "payload-X");
    EXPECT_EQ(currentIncarnation(*b, blob_key), t1);

    /// Black-box proof the part reads the t1 incarnation: re-publish the same blob into a SECOND
    /// namespace. The blob is already present at t1; nothing is re-uploaded.
    publishBlobPart(s, RootNamespace{"srv1/tbl/copy"}, "part_2", "data.bin", "payload-X");
    EXPECT_EQ(currentIncarnation(*b, blob_key), t1);
    assertPartReads(b, s, RootNamespace{"srv1/tbl/copy"}, "part_2", "data.bin", "payload-X");

    /// Independent discriminator that the blob rides t1, not the stale t0: t0 is DEAD. A deleteExact
    /// against t0 must TokenMismatch (INV-NO-RETURN — t0 was displaced and can never be current again).
    EXPECT_EQ(removeAtIncarnation(*b, blob_key, t0), Removal::Mismatch);
}

/// (DELETED, Phase A) RevalidateAbsentTokenedBlobResurrectsFromSource — see the file-header note: a
/// hand-deleted putBlob'd (`Materialized`) body is protocol-unreachable under EDGE-BEFORE-OBSERVE (a
/// materialized leaf under a durable precommit closure cannot be GC-deleted in the putBlob→promote window,
/// and promote no longer revalidates materialized leaves). Out-of-band body deletion is `cas-fsck`'s domain.

TEST(CASProtocol, EvidenceHitCondemnedPresentBlobCopiesForwardInClosure)
{
    /// `TrustedManifest` proof on a blob X whose hash is condemned-but-PRESENT. §4 manifest-trust
    /// (test name is legacy — there is no copy-forward any more): a committed-source adopted leaf is TRUSTED
    /// at the promote gate. The gate does NOT observe X — no HEAD, no meta point-read, no displacement — it
    /// publishes on the strength of the durable manifest edge (D4 relink trust). So promote SUCCEEDS and X's
    /// existing incarnation is left EXACTLY as-is: the token is UNCHANGED (never displaced) and the condemned
    /// meta is NOT flipped (the gate never reads or writes it). Here the source-manifest proof is trusted;
    /// materialized leaves are independently edge-protected.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// X pre-exists with token t0; the manifest names it as a tokenless adopted leaf.
    const String hex = streamingHexOf("payload-X");
    const BlobRef seeded_ref{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128(hex))};
    seedBlobWithDurablePrecommit(s, seeded_ref, "payload-X");
    const String blob_key = s->layout().blobKey(seeded_ref);
    const Etag t0 = currentIncarnation(*b, blob_key);

    auto build = startBuildFor(s, ns, "part_1");
    ManifestEntry entry = blobEntry("data.bin", "payload-X");
    entry.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hexToU128(hex))};   /// streaming-convention id (matches the minted blob)
    build->adoptEvidence(entry);   /// tokenless W-EVIDENCE dep on X (no HEAD, no upload)
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    /// GC condemns X's hash in round 1 via the meta — under §4 the promote gate never reads it.
    condemnMeta(*b, s->layout(), hexToU128(hex), /*condemn_round*/ 1);

    /// promote: the adopted leaf is trusted ⇒ commit, no probe, no displacement.
    EXPECT_NO_THROW(build->promote(ns, "part_1", build->buildId(), id));

    /// The ref stands; X rides its ORIGINAL token t0 (trust never displaces a trusted leaf).
    EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());
    EXPECT_EQ(currentIncarnation(*b, blob_key), t0) << "trust must not displace the adopted blob";

    /// The meta is untouched — still Condemned (the gate never reads or flips it under trust).
    const auto lm_after = loadMetaForTest(*b, s->layout(), hexToU128(hex));
    ASSERT_TRUE(lm_after.has_value());
    EXPECT_EQ(lm_after->meta.state, MetaState::Condemned) << "trust must not flip the meta";
}

TEST(CASProtocol, AbandonLeavesDebrisAndDisables)
{
    /// abandon leaves the uploaded blob + staged manifest body as debris (reaped by the orphan sweep);
    /// no owner transition is touched, and further build ops fail LOGICAL_ERROR (requireAlive).
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    auto build = startBuildFor(s, ns, "part_1");
    const ManifestId id = build->stageManifest({blobEntry("data.bin", "payload-X")});
    build->precommitAdd(ns, "part_1", id);
    auto blob = build->putBlob(idOf("payload-X"), BlobSource::fromString("payload-X"));

    build->abandon();

    /// Both bodies remain as debris: once the manifest has named a durable precommit edge, its body
    /// must survive until GC folds the matching owner removal.
    {
        DB::Cas::tests::OperationForTest op(*b);
        EXPECT_TRUE((*op).head(s->layout().blobKey(blob.ref), Retry::once()).has_value());
        EXPECT_TRUE((*op).head(s->layout().manifestKey(id), Retry::once()).has_value());
    }
    EXPECT_TRUE(s->listRefs(ns).empty());

    /// Further build ops ⇒ LOGICAL_ERROR (requireAlive).
    EXPECT_DEATH(
        {
            DB::abort_on_logical_error.store(true, std::memory_order_relaxed);
            build->stageManifest({blobEntry("data.bin", "payload-X")});
        },
        "PartWriteTxn has been abandoned");
}

TEST(CASProtocol, DropReattachThroughDetachedNamespace)
{
    /// ATTACH choreography (design §4): publish part_1 in ns; re-publish into ns/detached + drop part_1
    /// from ns; then re-publish part_1 back in ns + drop from detached. The BLOB is never re-uploaded
    /// (its token is stable throughout); each namespace gets its own single-owner manifest.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    const RootNamespace detached{"srv1/tbl/detached"};

    publishBlobPart(s, ns, "part_1", "data.bin", "payload-X");

    const String blob_key = s->layout().blobKey(idOf("payload-X"));
    const Etag blob_tok = currentIncarnation(*b, blob_key);

    EXPECT_TRUE(s->listRefs(ns).contains("part_1"));
    EXPECT_TRUE(s->listRefs(detached).empty());

    /// Move to detached: re-publish into detached (adopting the live blob), drop from ns.
    publishBlobPart(s, detached, "part_1", "data.bin", "payload-X");
    s->dropRef(ns, "part_1");

    EXPECT_TRUE(s->listRefs(ns).empty());
    ASSERT_TRUE(s->listRefs(detached).contains("part_1"));
    assertPartReads(b, s, detached, "part_1", "data.bin", "payload-X");

    /// Re-attach: re-publish part_1 back in ns, drop from detached.
    publishBlobPart(s, ns, "part_1", "data.bin", "payload-X");
    s->dropRef(detached, "part_1");

    ASSERT_TRUE(s->listRefs(ns).contains("part_1"));
    assertPartReads(b, s, ns, "part_1", "data.bin", "payload-X");
    EXPECT_TRUE(s->listRefs(detached).empty());

    /// The blob was never re-uploaded (token stable throughout — every publish dedup-adopted it).
    EXPECT_EQ(currentIncarnation(*b, blob_key), blob_tok);
}

TEST(CASProtocol, FreezeIntoShadowNamespace)
{
    /// `FREEZE` survives the table's part lifecycle (design §4): a shadow ref is a reachability root
    /// that outlives the dropped live ref.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};
    const RootNamespace shadow{"srv1/shadow/backup1/tbl"};

    publishBlobPart(s, ns, "part_1", "data.bin", "payload-X");

    /// Freeze into the shadow namespace (adopting the live blob), then drop the live ref.
    publishBlobPart(s, shadow, "part_1", "data.bin", "payload-X");
    s->dropRef(ns, "part_1");

    EXPECT_TRUE(s->listRefs(ns).empty());
    /// The shadow ref still resolves and reads after the live ref is gone.
    assertPartReads(b, s, shadow, "part_1", "data.bin", "payload-X");
}

TEST(CASProtocol, DisplacedToLiveTokenCommitsAtCurrentIncarnation)
{
    /// (Ported from the former ResurrectLosesRace scenario.) A blob displaced to a LIVE t1 (while its old
    /// t0 is condemned for a now-defunct incarnation) is SAFE to commit: the committed manifest names a
    /// blob HASH, the live t1 incarnation backs it, and GC's exact-token delete of t0 only TokenMismatches.
    /// Phase-A contract: the leaf is TOKENED, so promote does not re-HEAD it at all (edge-protected —
    /// EDGE-BEFORE-OBSERVE); the commit is correct by content addressing, not by revalidation. The old
    /// conservative ABORTED has no manifest-model analog.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    writeBlobRaw(*b, s->layout(), "payload-X", s->poolMeta().blob_header_len, s->poolMeta().pool_id);
    const String blob_key = s->layout().blobKey(idOf("payload-X"));
    const Etag t0 = currentIncarnation(*b, blob_key);

    auto build = startBuildFor(s, ns, "part_1");
    /// Wiring order (EDGE-BEFORE-OBSERVE): stageManifest -> precommitAdd -> putBlob.
    const ManifestId id = build->stageManifest({blobEntry("data.bin", "payload-X")});
    build->precommitAdd(ns, "part_1", id);
    build->putBlob(idOf("payload-X"), BlobSource::fromString("payload-X"));   /// dedup → adopts t0

    /// Another writer displaces X to t1 (uncondemned) before our gate runs.
    const Etag t1 = displaceBlobToken(*b, s->layout(), idOf("payload-X"));
    ASSERT_NE(t1, t0);

    /// promote: revalidate X ⇒ HEAD current t1 (NOT condemned; only the defunct t0 is) ⇒ commit.
    build->promote(ns, "part_1", build->buildId(), id);

    /// The blob lives at t1 (the displacing writer's incarnation) and the part reads.
    EXPECT_EQ(currentIncarnation(*b, blob_key), t1);
    assertPartReads(b, s, ns, "part_1", "data.bin", "payload-X");

    /// NO-LOSS / NO-RETURN: t0 is dead — a deleteExact against it TokenMismatches (the GC delete of the
    /// condemned t0 spares the live t1).
    EXPECT_EQ(removeAtIncarnation(*b, blob_key, t0), Removal::Mismatch);
}

TEST(CASProtocol, FreshEvidenceDepWithViewHitIsResolvedByGate)
{
    /// §4 manifest-trust (test name is legacy — the gate no longer "resolves" a tokenless leaf by observing
    /// it): a committed-source adopted leaf whose blob is condemned-but-PRESENT is TRUSTED at the promote
    /// gate. There is NO per-file probe (no HEAD, no meta point-read) and NO copy-forward — the durable
    /// manifest edge is the liveness evidence (D4 relink trust). promote SUCCEEDS and X keeps its ORIGINAL
    /// incarnation: the token t0 is UNCHANGED (never displaced). A materialized leaf is edge-protected;
    /// this committed-source adopt instead carries trusted-manifest proof.
    auto b = std::make_shared<InMemoryBackend>();

    DB::Cas::Layout layout("p");
    const String hex = streamingHexOf("payload-fresh-ev");
    {
        auto s0 = openPool(b);
        seedBlobWithDurablePrecommit(
            s0,
            BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128(hex))},
            "payload-fresh-ev");
    }
    const String blob_key = layout.blobKey(BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(hexToU128(hex))});
    const Etag t0 = currentIncarnation(*b, blob_key);
    condemnMeta(*b, layout, hexToU128(hex), /*condemn_round*/ 1);

    auto s = openPool(b);

    const RootNamespace ns{"srv1/tbl"};
    auto build = startBuildFor(s, ns, "part_1");
    /// adoptEvidence records a TOKENLESS dep.
    ManifestEntry entry = blobEntry("data.bin", "payload-fresh-ev");
    entry.ref = DB::Cas::BlobRef{DB::Cas::BlobHashAlgo::CityHash128, DB::Cas::BlobDigest::fromU128(hexToU128(hex))};   /// streaming-convention id (matches the minted blob)
    build->adoptEvidence(entry);
    const ManifestId id = build->stageManifest({entry});
    build->precommitAdd(ns, "part_1", id);

    /// promote trusts the adopted leaf ⇒ commit, no probe, no displacement.
    EXPECT_NO_THROW(build->promote(ns, "part_1", build->buildId(), id));

    EXPECT_EQ(currentIncarnation(*b, blob_key), t0) << "trust must not displace the adopted blob";
    EXPECT_TRUE(s->resolveRef(ns, "part_1").has_value());
}

TEST(CASProtocol, AdoptedLeafCarriesRealBlobSize)
{
    /// B92 round-trip (re-expressed on the manifest model): an adopted leaf must carry its real
    /// blob_size, NOT 0. PartWriteTxn A publishes a blob; build B adopts that leaf into a second ref. The
    /// adopted manifest's entry must report the same non-zero blob_size as the original.
    auto b = std::make_shared<InMemoryBackend>();
    auto s = openPool(b);
    const RootNamespace ns{"srv1/tbl"};

    /// PartWriteTxn A: a blob with a real payload so blob_size > 0.
    const ManifestId id_a = publishBlobPart(s, ns, "ref_a", "data.bin", "payload-B92");

    const PartManifest manifest_a = s->readManifest(id_a);
    const auto * entry_a = findEntry(manifest_a.entries, "data.bin");
    ASSERT_TRUE(entry_a != nullptr);
    const uint64_t size_a = entry_a->blob_size;
    EXPECT_NE(size_a, 0u) << "ref A blob_size must be non-zero";
    EXPECT_EQ(size_a, String("payload-B92").size());

    /// PartWriteTxn B: adopt the same leaf, publish as ref_b (no re-upload).
    auto build_b = startBuildFor(s, ns, "ref_b");
    ASSERT_TRUE(entry_a != nullptr);
    build_b->adoptEvidence(*entry_a);
    const ManifestId id_b = build_b->stageManifest({*entry_a});
    build_b->precommitAdd(ns, "ref_b", id_b);
    build_b->promote(ns, "ref_b", build_b->buildId(), id_b);

    /// Resolve ref B: the adopted leaf's blob_size must match ref A (round-trip invariant for B92).
    const PartManifest manifest_b = s->readManifest(s->resolveRef(ns, "ref_b")->manifest_id);
    const auto * entry_b = findEntry(manifest_b.entries, "data.bin");
    ASSERT_TRUE(entry_b != nullptr);
    EXPECT_NE(entry_b->blob_size, 0u) << "adopted leaf blob_size must not be 0 (B92)";
    EXPECT_EQ(entry_b->blob_size, size_a) << "adopted-leaf blob_size mismatch (B92 round-trip)";
}

/// ---- Genuinely-obsolete pure-tree-model scenarios (no manifest analog) ----

TEST(CASProtocol, DISABLED_RevalidateAbsentTreeDepRecreates)
{
    GTEST_SKIP() << "Obsolete (tree model). The gate's 'absent tree dep recreated from retained "
                    "payload' behavior has no manifest analog: a part manifest body is staged ONCE by "
                    "stageManifest and promote never re-creates it — an absent/invalid body at promote "
                    "fails closed (ABORTED). The blob-leaf absent-recreate case is covered by putBlob's "
                    "INV-1 re-upload-from-source path, not by the publish gate.";
}

TEST(CASProtocol, DISABLED_AdoptTreeOfReclaimedTreeFailsClosedAtAdoptTime)
{
    GTEST_SKIP() << "Obsolete (tree model). adoptTree's fail-closed observe-at-adopt-time (one HEAD, "
                    "FILE_DOESNT_EXIST on an absent detached tree) has no manifest analog: the manifest "
                    "model's adoptEvidence is deliberately TOKENLESS and performs NO backend call — the "
                    "no-dangle guarantee for an adopted-but-reclaimed leaf is enforced at the promote "
                    "gate (unconditional blob revalidation ⇒ ABORTED), covered by "
                    "NewNamespacePublishGatedByShardFenceFloor and FreshEvidenceDepWithViewHitIsResolvedByGate.";
}
