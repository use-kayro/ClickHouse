#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Tools/CasInspect.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <Common/Exception.h>
#include <fmt/format.h>
#include <cstdint>
#include <vector>

namespace DB::Cas
{

namespace
{

/// Escapes `s` as a JSON string LITERAL (including the surrounding quotes). Handles the standard
/// two-char escapes plus a `\uXXXX` fallback for any other control byte; everything else (including
/// raw multi-byte UTF-8) passes through unchanged. This is a debug/inspection rendering, not a wire
/// format, so it deliberately does not attempt full Unicode validation.
String jsonEscape(std::string_view s)
{
    String out;
    out.reserve(s.size() + 2);
    out += '"';
    for (unsigned char c : s)
    {
        switch (c)
        {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20)
                    out += fmt::format("\\u{:04x}", c);
                else
                    out += static_cast<char>(c);
        }
    }
    out += '"';
    return out;
}

/// u128 fields (hashes, ids, tokens-as-u128) render as a lowercase-hex JSON string, matching
/// `u128ToHex` — never as a nested {high,low} object or a decimal number.
String jsonHex(const UInt128 & v) { return jsonEscape(u128ToHex(v)); }
String jsonUInt(uint64_t v) { return std::to_string(v); }

/// A minimal JSON object builder: each `add` takes a key and an already-rendered JSON fragment
/// (a quoted string, a number, `true`/`false`/`null`, or a nested `{...}`/`[...]`) and joins them
/// with commas. No pretty-printing — this is a debug/inspection tool, not a wire format.
class JsonObj
{
public:
    JsonObj & add(std::string_view key, const String & raw_value)
    {
        if (!first)
            out += ",";
        first = false;
        out += jsonEscape(key);
        out += ":";
        out += raw_value;
        return *this;
    }

    String str() const { return "{" + out + "}"; }

private:
    String out;
    bool first = true;
};

String jsonArray(const std::vector<String> & items)
{
    String out = "[";
    for (size_t i = 0; i < items.size(); ++i)
    {
        if (i)
            out += ",";
        out += items[i];
    }
    out += "]";
    return out;
}

String renderProvenance(const Provenance & p)
{
    return JsonObj()
        .add("created_at_ms", jsonUInt(p.created_at_ms))
        .add("creator_server_id", jsonHex(p.creator_server_id))
        .add("ch_version", jsonUInt(p.ch_version))
        .add("op", jsonEscape(provenanceOpToWireWord(p.op)))
        .str();
}

String renderEnvelopeHeader(const EnvelopeHeader & h)
{
    return JsonObj()
        .add("kind", jsonEscape(objectKindToWord(h.kind)))
        /// The blob identity is carried by the object key, so the envelope keeps only the provenance
        /// fields needed for forensics (`chver` and `build`) together with its compatibility version.
        .add("compatibility_version", jsonUInt(h.compatibility_version))
        .add("incarnation_tag", jsonHex(h.incarnation_tag))
        .add("build_id", jsonHex(h.build_id))
        .add("header_len", jsonUInt(h.header_len))
        .add("provenance", h.provenance ? renderProvenance(*h.provenance) : "null")
        .add("intended_ref", h.intended_ref ? jsonEscape(*h.intended_ref) : "null")
        .str();
}

String renderPoolMeta(const PoolMeta & m)
{
    std::vector<String> algos;
    for (const auto algo : m.algos_used)
        algos.push_back(jsonEscape(blobHashAlgoName(static_cast<BlobHashAlgo>(algo))));
    return JsonObj()
        .add("object", jsonEscape("pool_meta"))
        .add("pool_id", jsonHex(m.pool_id))
        .add("blob_header_len", jsonUInt(m.blob_header_len))
        .add("min_reader_generation", jsonUInt(m.min_reader_generation))
        .add("algos_used", jsonArray(algos))
        .str();
}

}

String caInspectToJson(const Layout & layout, const String & key, std::string_view bytes)
{
    if (key == layout.poolMetaKey())
        return renderPoolMeta(decodePoolMeta(bytes));
    if (key.starts_with(layout.blobsPrefix()))
        return renderEnvelopeHeader(decodeEnvelopeHeader(bytes, bytes.size(), ObjectKind::Blob));
    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
        "cas-inspect: unrecognized key layout '{}' (recognized: _pool_meta, blobs)", key);
}

}
