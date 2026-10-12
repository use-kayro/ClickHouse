#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefSnapshotFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/WriteBufferFromString.h>
#include "cas_test_helpers.h"

using namespace DB::Cas;

namespace
{

ManifestRef manifestRef(uint64_t epoch, uint64_t seq, uint32_t ordinal)
{
    return ManifestRef{epoch, seq, ordinal};
}

}

/// Stage-1 T12 (spec §4 "RefOp payload removal"): `cas-inspect` renders the renamed `SetPublishedAt`
/// op kind, and neither the ref-log nor the ref-snapshot rendering carries a `payload_size` key --
/// `RefOp`/`RefCommittedRow` no longer have a `payload` field to size.

TEST(CASInspect, RendersSetPublishedAtOpWithNoPayloadSizeKey)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{7, 9};

    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    RefOp op;
    op.kind = RefOpKind::SetPublishedAt;
    op.ref_name = "all_1_1_0";
    op.expected_manifest_ref = manifestRef(1, 1, 1);
    op.published_at_ms = 42;
    txn.ops.push_back(op);

    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("kind":"set_published_at")"), String::npos) << json;
    EXPECT_EQ(json.find("payload"), String::npos) << json;
}

/// Task-1 review finding M5: `cas inspect` renders the new `EpochSeal` op kind and the txn-level
/// `prev_epoch_seal` chain field, needed to debug INV-2 seal chains without a raw byte dump.
TEST(CASInspect, RendersEpochSealTxnWithPrevEpochSeal)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{3, 1};

    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    txn.prev_epoch_seal = RefTxnId{2, 9};
    RefOp op;
    op.kind = RefOpKind::EpochSeal;
    txn.ops.push_back(op);

    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("kind":"epoch_seal")"), String::npos) << json;
    EXPECT_NE(json.find(R"("prev_epoch_seal":{"writer_epoch":2,"ref_sequence":9})"), String::npos) << json;
}

/// The remaining two `RefOpKind` words this file's other tests do not exercise: a namespace's birth
/// record and its removal terminator.
TEST(CASInspect, RendersNamespaceBirthAndRemoveNamespaceOpKinds)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};

    RefLogTxn birth_txn;
    birth_txn.ns = ns.string();
    birth_txn.txn_id = RefTxnId{1, 1};
    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    birth_txn.ops.push_back(birth);
    const String birth_key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), birth_txn.txn_id);
    const String birth_bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(birth_txn));
    const String birth_json = caInspectToJson(
        layout, birth_key, birth_bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(birth_json.find(R"("kind":"namespace_birth")"), String::npos) << birth_json;

    RefLogTxn remove_txn;
    remove_txn.ns = ns.string();
    remove_txn.txn_id = RefTxnId{1, 2};
    RefOp remove;
    remove.kind = RefOpKind::RemoveNamespace;
    remove_txn.ops.push_back(remove);
    const String remove_key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), remove_txn.txn_id);
    const String remove_bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(remove_txn));
    const String remove_json = caInspectToJson(
        layout, remove_key, remove_bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(remove_json.find(R"("kind":"remove_namespace")"), String::npos) << remove_json;
}

/// `RefOwnerKind` renders as its full wire word (`committed`/`precommit`), not the enumerator spelling,
/// at both binding slots an `owner_transition` op carries.
TEST(CASInspect, RendersRefOwnerKindWireWords)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{1, 3};

    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = id;
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{RefOwnerKind::Committed, "all_1_1_0", manifestRef(1, 1, 1)};
    op.new_binding = RefOwnerBinding{RefOwnerKind::Precommit, "all_1_1_0", manifestRef(1, 1, 1)};
    txn.ops.push_back(op);

    const String key = layout.refLogKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("old_binding":{"kind":"committed")"), String::npos) << json;
    EXPECT_NE(json.find(R"("new_binding":{"kind":"precommit")"), String::npos) << json;
}

TEST(CASInspect, RendersCommittedRowWithNoPayloadSizeKey)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const RefTxnId id{7, 9};

    RefTableSnapshot snap;
    snap.ns = ns.string();
    snap.snapshot_id = id;
    RefCommittedRow row;
    row.ref_name = "all_1_1_0";
    row.manifest_ref = manifestRef(1, 1, 1);
    row.published_at_ms = 42;
    snap.committed.push_back(row);

    const String key = layout.refSnapshotKey(DB::Cas::tests::fixture::fixtureLife(ns), id);
    const String bytes = sealObject(FormatId::RefSnapshot, encodeRefTableSnapshot(snap));

    const String json = caInspectToJson(layout, key, bytes, DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_EQ(json.find("payload"), String::npos) << json;
    EXPECT_EQ(json.find("lifecycle"), String::npos) << json;
    EXPECT_EQ(json.find("remove_txn_id"), String::npos) << json;
    EXPECT_NE(json.find(R"("published_at_ms":42)"), String::npos) << json;
}

/// Stage A task 5 (spec INV-4): the `_ckpt` renders as its own object kind. It is point-addressed in
/// `cas/ns/state/` with no transaction id, so it has a separate dispatch from stream objects and once
/// fell through to
/// `BAD_ARGUMENTS` for it -- and it is precisely the object an operator reaches for when asking "what
/// is recovery's base" or "why is cleanup not reclaiming anything".
TEST(CASInspect, RendersRefCkptWithEveryFieldPresent)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};

    const RefCkpt ckpt{.life_epoch = std::optional<uint64_t>{7},
                       .committed_through = RefTxnId{7, 9},
                       .checkpoint_snapshot_id = RefTxnId{7, 9},
                       .last_epoch_seal = RefTxnId{6, 4}};

    const String json = caInspectToJson(
        layout, layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)), encodeRefCkpt(ckpt),
        DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("object":"ref_ckpt")"), String::npos) << json;
    /// The namespace comes from the KEY: a `_ckpt` body does not name it.
    EXPECT_NE(json.find(R"("namespace":"srv1/db/tbl")"), String::npos) << json;
    EXPECT_NE(json.find(R"("life_epoch":7)"), String::npos) << json;
    EXPECT_NE(json.find(R"("committed_through":{"writer_epoch":7,"ref_sequence":9})"), String::npos) << json;
    EXPECT_NE(json.find(R"("writer_epoch":7,"ref_sequence":9)"), String::npos) << json;
    EXPECT_NE(json.find(R"("writer_epoch":6,"ref_sequence":4)"), String::npos) << json;
}

/// The absences are the interesting readings, so they render as explicit `null`s rather than missing
/// keys: no checkpoint means recovery has no base AND nothing is deletable, which is a very different
/// report from "the key is there and I could not tell you what is in it".
TEST(CASInspect, RendersRefCkptAbsencesAsExplicitNulls)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/fresh"};

    const String json = caInspectToJson(
        layout, layout.refCkptKey(DB::Cas::tests::fixture::fixtureLife(ns)), encodeRefCkpt(RefCkpt{}),
        DB::Cas::tests::fixture::fixtureLife(ns));
    EXPECT_NE(json.find(R"("object":"ref_ckpt")"), String::npos) << json;
    EXPECT_NE(json.find(R"("life_epoch":null)"), String::npos) << json;
    EXPECT_NE(json.find(R"("checkpoint_snapshot_id":null)"), String::npos) << json;
    EXPECT_NE(json.find(R"("last_epoch_seal":null)"), String::npos) << json;
}

/// A listed physical id cannot supply a namespace. Inspect must receive the unique catalog join, and
/// a different logical spelling at the same id is rejected by the decoded object's own namespace.
TEST(CASInspect, RefObjectRequiresTheExactCatalogResolution)
{
    const Layout layout("p");
    const RootNamespace ns{"srv1/db/tbl"};
    const NamespaceLifeId life = NamespaceLifeId::fromCatalogEntry(ns, UInt128{91});
    RefLogTxn txn;
    txn.ns = ns.string();
    txn.txn_id = RefTxnId{1, 1};
    RefOp birth;
    birth.kind = RefOpKind::NamespaceBirth;
    txn.ops = {birth};
    const String key = layout.refLogKey(life, txn.txn_id);
    const String bytes = sealObject(FormatId::RefLog, encodeRefLogTxn(txn));

    EXPECT_THROW(caInspectToJson(layout, key, bytes), DB::Exception);
    EXPECT_THROW(caInspectToJson(
        layout, key, bytes, NamespaceLifeId::fromCatalogEntry(RootNamespace{"redirected"}, life.incarnation)),
        DB::Exception);
    EXPECT_NO_THROW(caInspectToJson(layout, key, bytes, life));
}
