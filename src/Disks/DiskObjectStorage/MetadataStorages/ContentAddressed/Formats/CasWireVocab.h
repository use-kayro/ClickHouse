#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTable.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <optional>
#include <string_view>

namespace DB::Cas
{

/// Shared JSON vocabulary for the value sub-types the CAS text codecs embed (`_pool_meta`, the Keeper
/// part record, the blob envelope). Every reverse map rejects an unrecognized value with
/// `CORRUPTED_DATA`; silently choosing a default would turn malformed persisted data into a different
/// valid-looking record.

/// The `ObjectKind` wire vocabulary; coverage is proven in `CasWireVocab.cpp`.
inline constexpr EnumWireTable<ObjectKind, 1> kObjectKindWords{{{
    {ObjectKind::Blob, "blob"},
}}};

/// Parse a canonical blob-hash algorithm word. The write side uses `blobHashAlgoName` directly.
/// `what` identifies the containing field in the `CORRUPTED_DATA` exception.
BlobHashAlgo blobHashAlgoFromWord(std::string_view w, std::string_view what);

/// The word for an `ObjectKind`.
std::string_view objectKindToWord(ObjectKind k);

/// Writes a blob reference's `algo` and `digest` fields into an open JSON object.
void writeBlobRefFields(CasJsonWriter & out, bool & first, const BlobRef & r);

namespace SharedWire
{
    inline constexpr WireKey algo{"algo"};
    inline constexpr WireKey digest{"digest"};
}

/// The `algo`/`digest` pair of one blob reference, collected while a JSON object is read.
struct BlobRefFields
{
    std::optional<String> algo_word;
    std::optional<String> digest_hex;

    /// Requires both fields, parses the algorithm word, and checks the digest hex width against the
    /// algorithm's width BEFORE calling `fromHex` -- a width mismatch must surface as `CORRUPTED_DATA`
    /// (malformed persisted input), not `DigestCodec::fromHex`'s `BAD_ARGUMENTS` (a caller-contract
    /// violation). `what` identifies the field in the exception.
    BlobRef build(std::string_view what) const;
};

inline bool matchBlobRefFields(std::string_view key, JsonObjectReader & r, BlobRefFields & fields)
{
    if (key == SharedWire::algo)   { fields.algo_word = r.readString(); return true; }
    if (key == SharedWire::digest) { fields.digest_hex = r.readString(); return true; }
    return false;
}

}
