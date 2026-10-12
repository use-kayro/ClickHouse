#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/WriteBufferFromString.h>
#include <fmt/format.h>

using namespace DB;
using namespace DB::Cas;

namespace
{
String lineAt(const String & text, size_t index)
{
    size_t begin = 0;
    for (size_t i = 0; i < index; ++i)
        begin = text.find('\n', begin) + 1;
    const size_t end = text.find('\n', begin);
    return text.substr(begin, end - begin + 1);
}

void expectDelta(const String & old_bytes, const String & new_bytes, size_t expected)
{
    EXPECT_EQ(new_bytes.size() - old_bytes.size(), expected) << "old: " << old_bytes << "new: " << new_bytes;
}
}

/// The `CASEncodingPins` literals below pin the CANONICAL BYTES of the CAS text encoders: canonical
/// text is byte-compared on retries and deterministic adoption, and the incremental ref budget
/// counters assume these exact sizes. Never edit one of those expected strings to make a test pass —
/// that means the encoder's bytes drifted, which is the bug.
///
/// The `CASWireCutDeltas` literals are the opposite kind: each is a HISTORICAL pre-cut row, kept so
/// the cost of the semantic-key rename stays measurable against what it replaced. They are
/// deliberately not the current bytes and must never be refreshed toward them — a delta measured
/// against today's encoder on both sides would always be zero.

TEST(CASEncodingPins, RefLogTxnAllOpKinds)
{
    RefLogTxn txn;
    txn.ns = "roots/pin";
    txn.txn_id = RefTxnId{7, 9};

    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops.push_back(birth);

    RefOp transition;
    transition.kind = RefOpKind::OwnerTransition;
    transition.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "20260101_0_1_1_1", ManifestRef{1, 2, 3}};
    transition.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "20260101_0_1_1_1", ManifestRef{1, 2, 3}};
    txn.ops.push_back(transition);

    RefOp set_published_at;
    set_published_at.kind = RefOpKind::SetPublishedAt;
    /// NOTE the split literals: "\x01" "e" (else the hex escape would swallow the 'e') and
    /// "\xA8" "f" (else it would swallow the 'f'). `checkCanonicalRefName` forbids '\\' and NUL but
    /// not quote/newline/control bytes/U+2028, so `ref_name` -- the only free-form string `RefOp`
    /// still carries now that `payload` is gone -- exercises quote, newline, a bare control byte,
    /// and the three-byte U+2028 sequence. Backslash escaping is pinned separately, over an
    /// unrestricted string, by the JSON-writer escaping suite.
    set_published_at.ref_name = String("20260101_0_1_1_1\"c\nd") + "\x01" "e" + "\xE2\x80\xA8" "f";
    set_published_at.expected_manifest_ref = ManifestRef{1, 2, 3};
    set_published_at.published_at_ms = 1234;
    txn.ops.push_back(set_published_at);

    RefOp removal;
    removal.kind = RefOpKind::RemoveNamespace;
    txn.ops.push_back(removal);

    const String expected = fmt::format("{{\"type\":\"cas_ref_log\",\"v\":{}}}\n", currentCompatibilityVersion()) +
        "{\"namespace\":\"roots/pin\",\"txn_epoch\":\"7\",\"txn_seq\":\"9\"}\n"
        "{\"op\":\"namespace_birth\"}\n"
        "{\"op\":\"owner_transition\",\"old_kind\":\"precommit\",\"old_ref\":\"20260101_0_1_1_1\","
        "\"old_epoch\":\"1\",\"old_build\":\"2\",\"old_ord\":3,\"new_kind\":\"committed\",\"new_ref\":\"20260101_0_1_1_1\","
        "\"new_epoch\":\"1\",\"new_build\":\"2\",\"new_ord\":3}\n"
        "{\"op\":\"set_published_at\",\"ref\":\"20260101_0_1_1_1\\\"c\\nd\\u0001e\\u2028f\","
        "\"epoch\":\"1\",\"build\":\"2\",\"ord\":3,\"published_ms\":1234}\n"
        "{\"op\":\"remove_namespace\"}\n"
        "{\"n\":4}\n";
    EXPECT_EQ(encodeRefLogTxn(txn), expected);
}

TEST(CASEncodingPins, RefSnapshotLive)
{
    RefTableSnapshot snap;
    snap.ns = "roots/pin";
    snap.snapshot_id = RefTxnId{7, 9};

    RefCommittedRow row;
    row.ref_name = "20260101_0_1_1_1";
    row.manifest_ref = ManifestRef{1, 2, 3};
    row.published_at_ms = 5;
    snap.committed.push_back(row);

    snap.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "20260102_0_2_2_2", ManifestRef{4, 5, 6}});

    const String expected = fmt::format("{{\"type\":\"cas_ref_snap\",\"v\":{}}}\n", currentCompatibilityVersion()) +
        "{\"namespace\":\"roots/pin\",\"snapshot_epoch\":\"7\",\"snapshot_seq\":\"9\",\"lifecycle\":\"live\"}\n"
        "{\"kind\":\"committed\",\"ref\":\"20260101_0_1_1_1\",\"epoch\":\"1\",\"build\":\"2\",\"ord\":3,\"published_ms\":5}\n"
        "{\"kind\":\"precommit\",\"ref\":\"20260102_0_2_2_2\",\"epoch\":\"4\",\"build\":\"5\",\"ord\":6}\n"
        "{\"n\":2}\n";
    EXPECT_EQ(encodeRefTableSnapshot(snap), expected);
}

TEST(CASWireCutDeltas, BlobPartManifestEntry)
{
    PartManifest manifest;
    manifest.ref = ManifestRef{1, 2, 3};
    manifest.root_namespace_id = RootNamespace{"root"};
    manifest.entries = {ManifestEntry{"a", EntryPlacement::Blob, BlobRef{BlobHashAlgo::CityHash128, BlobDigest::fromU128(UInt128(4))}, 9, {}}};
    const String text = encodePartManifest(manifest);
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"p\":\"a\",\"pm\":\"blob\",\"ha\":\"ch128\",\"h\":\"00000000000000000000000000000004\",\"sz\":9}\n";
    expectDelta(old_bytes, lineAt(text, 2), 15);
}

TEST(CASWireCutDeltas, InlinePartManifestEntry)
{
    PartManifest manifest;
    manifest.ref = ManifestRef{1, 2, 3};
    manifest.root_namespace_id = RootNamespace{"root"};
    manifest.entries = {ManifestEntry{"a", EntryPlacement::Inline, {}, 0, "x"}};
    const String text = encodePartManifest(manifest);
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"p\":\"a\",\"pm\":\"inline\",\"il\":1}\n";
    expectDelta(old_bytes, lineAt(text, 2), 8);
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_banner = "==> \"a\" il=1 <==\n";
    expectDelta(old_banner, lineAt(text, 4), 2);
}

/// The ref-log's own op rows: the highest-cardinality record of the format and, for
/// `owner_transition`, the largest single-row cost of the whole cut -- both old-side groups and both
/// new-side groups are renamed at once.
TEST(CASWireCutDeltas, OwnerTransitionRefLogOpRow)
{
    RefLogTxn txn;
    txn.ns = "root";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Precommit, "r", ManifestRef{3, 4, 5}};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Committed, "r", ManifestRef{3, 4, 5}};
    txn.ops.push_back(op);
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"op\":\"owner_transition\",\"obk\":\"precommit\",\"orn\":\"r\",\"ome\":\"3\",\"omb\":\"4\",\"omo\":5,"
                             "\"nbk\":\"committed\",\"nrn\":\"r\",\"nme\":\"3\",\"nmb\":\"4\",\"nmo\":5}\n";
    expectDelta(old_bytes, lineAt(encodeRefLogTxn(txn), 2), 50);
}

TEST(CASWireCutDeltas, SetPublishedAtRefLogOpRow)
{
    RefLogTxn txn;
    txn.ns = "root";
    txn.txn_id = RefTxnId{1, 1};
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "r";
    op.expected_manifest_ref = ManifestRef{3, 4, 5};
    op.published_at_ms = 6;
    txn.ops.push_back(op);
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"op\":\"set_published_at\",\"rn\":\"r\",\"me\":\"3\",\"mb\":\"4\",\"mo\":5,\"ts\":6}\n";
    expectDelta(old_bytes, lineAt(encodeRefLogTxn(txn), 2), 18);
}

/// The body-less ops are the cut's only free rows: the record is the `op` key alone. This measures
/// that the ROW costs nothing extra, not that the word itself is unchanged -- it builds its old side
/// from the current word, so it cannot see a word rename. The words are pinned literally by the
/// closed-set tests; what this adds is that no framing crept in around them.
TEST(CASWireCutDeltas, BodylessRefLogOpRowsAreUnchanged)
{
    for (const RefOpKind kind : {RefOpKind::NamespaceBirth, RefOpKind::EpochSeal})
    {
        RefLogTxn txn;
        txn.ns = "root";
        txn.txn_id = RefTxnId{1, 1};
        RefOp op;
        op.kind = kind;
        txn.ops.push_back(op);
        const String old_bytes = fmt::format("{{\"op\":\"{}\"}}\n", refOpKindToWireWord(kind));
        expectDelta(old_bytes, lineAt(encodeRefLogTxn(txn), 2), 0);
    }
}

TEST(CASWireCutDeltas, CommittedRefSnapshotRow)
{
    RefTableSnapshot snapshot;
    snapshot.ns = "root";
    snapshot.snapshot_id = RefTxnId{1, 2};
    snapshot.committed.push_back(RefCommittedRow{"r", ManifestRef{3, 4, 5}, 6});
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"k\":\"c\",\"rn\":\"r\",\"me\":\"3\",\"mb\":\"4\",\"mo\":5,\"ts\":6}\n";
    expectDelta(old_bytes, lineAt(encodeRefTableSnapshot(snapshot), 2), 29);
}

TEST(CASWireCutDeltas, PrecommitRefSnapshotRow)
{
    RefTableSnapshot snapshot;
    snapshot.ns = "root";
    snapshot.snapshot_id = RefTxnId{1, 2};
    snapshot.precommits.push_back(RefOwnerBinding{RefOwnerKind::Precommit, "r", ManifestRef{3, 4, 5}});
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"k\":\"p\",\"rn\":\"r\",\"me\":\"3\",\"mb\":\"4\",\"mo\":5}\n";
    expectDelta(old_bytes, lineAt(encodeRefTableSnapshot(snapshot), 2), 19);
}

TEST(CASWireCutDeltas, BaseRefCatalogRow)
{
    RefCatalog catalog{{CatalogEntry{.ns = RootNamespace{"root"}, .state = NsState::Live, .incarnation = UInt128(7)}}};
    /// This literal is the pre-cut baseline this delta is measured against.
    const String old_bytes = "{\"k\":\"ent\",\"ns\":\"root\",\"st\":\"live\",\"inc\":\"00000000000000000000000000000007\"}\n";
    expectDelta(old_bytes, lineAt(encodeRefCatalog(catalog), 1), 9);
}
