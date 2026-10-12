#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <limits>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LIMIT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

namespace RefCatalogWire
{
    constexpr WireKey kind{"kind"};
    constexpr WireKey ns{"ns"};
    constexpr WireKey state{"state"};
    constexpr WireKey life{"life"};
    constexpr WireKey remove_round{"remove_round"};
    constexpr WireKey creator{"creator"};
    constexpr WireKey creator_epoch{"creator_epoch"};
    constexpr WireKey creator_fence{"creator_fence"};
}

constexpr std::string_view kEntryTag = "entry";

constexpr EnumWireTable<NsState, 3> kNsStateWords{{{
    {NsState::Creating, "creating"},
    {NsState::Live, "live"},
    {NsState::Removing, "removing"},
}}};

static_assert(casEnumTableCoversEnum<kNsStateWords, NsState>());

/// `creator` is required iff `state == Creating`, forbidden otherwise -- one predicate, used by both
/// directions of the codec, so the writer's self-check and the reader's fail-close can never disagree.
bool creatorPairingOk(const CatalogEntry & e)
{
    return (e.state == NsState::Creating) == e.creator.has_value();
}

bool removalRoundPairingOk(const CatalogEntry & e)
{
    return (e.state == NsState::Removing) == e.removal_started_round.has_value();
}

/// Whether `entries` is already in the catalog's canonical shape: strictly ascending by namespace
/// bytes, no duplicate namespace. A duplicate namespace fails the SAME check as an out-of-order pair
/// (equal keys never compare strictly less), which is exactly right -- both are "not canonical".
bool isCanonicalCatalogOrder(const std::vector<CatalogEntry> & entries)
{
    for (size_t i = 1; i < entries.size(); ++i)
        if (!(entries[i - 1].ns.string() < entries[i].ns.string()))
            return false;
    return true;
}

}

std::string_view nsStateToWord(NsState s)
{
    return kNsStateWords.toWord(s, "CAS ref catalog");
}

NsState nsStateFromWord(std::string_view w)
{
    return kNsStateWords.fromWord(w, "CAS ref catalog ns state");
}

String encodeRefCatalog(const RefCatalog & catalog)
{
    const uint64_t line_cap = traitsFor(FormatId::RefCatalog).line_cap;
    CasJsonWriter out(256);

    /// EVERY line this encoder emits is measured against the LINE cap, on the bytes actually emitted
    /// -- escaping, framing and all -- with `LIMIT_EXCEEDED`, not `LOGICAL_ERROR`. A line that does not fit is
    /// not a large line, it is an UNREADABLE one: `readLine` refuses it, so the whole object is lost.
    /// This is a capacity refusal on otherwise well-formed input (an admitted namespace name or
    /// server_root_id near their own byte bounds, worst-case escaped, can reach ~4.7 KiB on its own --
    /// reachable, not theoretical), not a bug in this process, so a caller catching `LIMIT_EXCEEDED`
    /// for a capacity refusal must not see it misreported as a `LOGICAL_ERROR`. Refuse here, where
    /// nothing is durable yet.
    const auto checkLineBytes = [&](uint64_t bytes, std::string_view what)
    {
        if (bytes > line_cap)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "CAS ref catalog: the {} line encodes to {} bytes, over the {}-byte line cap; a longer "
                "line cannot be read back, so the catalog is refused before it is written",
                what, bytes, line_cap);
    };

    writeHeaderLine(out, FormatId::RefCatalog);   /// emits its own terminator
    checkLineBytes(out.size() - 1, "header");

    /// This is our own state, about to become durable: an out-of-order or duplicate-keyed vector is a
    /// bug in the writer, not corruption arriving from a store.
    if (!isCanonicalCatalogOrder(catalog.entries))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "CAS ref catalog: entries are not canonically ordered -- strictly ascending by namespace "
            "bytes with no duplicate namespace is required before a catalog may be encoded");

    size_t line_start = out.size();
    const auto closeLine = [&](std::string_view what)
    {
        checkLineBytes(out.size() - line_start, what);
        writeChar('\n', out);
        line_start = out.size();
    };

    for (const CatalogEntry & e : catalog.entries)
    {
        if (e.ns.string().empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS ref catalog: an entry's namespace must not be empty");
        if (e.ns.string().size() > kMaxNamespaceBytes)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS ref catalog: namespace '{}' is {} bytes, over the {}-byte admission bound",
                e.ns.string(), e.ns.string().size(), kMaxNamespaceBytes);
        if (e.incarnation == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS ref catalog: namespace '{}' has a zero incarnation -- 0 never names a life",
                e.ns.string());
        if (!creatorPairingOk(e))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS ref catalog: namespace '{}' is {} and {} a creator fence -- creator is required "
                "iff state == Creating and forbidden otherwise",
                e.ns.string(), nsStateToWord(e.state), e.creator ? "carries" : "lacks");
        if (!removalRoundPairingOk(e))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "CAS ref catalog: namespace '{}' is {} and {} removal_started_round -- the field is "
                "required iff state == Removing",
                e.ns.string(), nsStateToWord(e.state), e.removal_started_round ? "carries" : "lacks");

        bool first = true;
        writeStringField(out, RefCatalogWire::kind, kEntryTag, first);
        writeStringField(out, RefCatalogWire::ns, e.ns.string(), first);
        writeStringField(out, RefCatalogWire::state, nsStateToWord(e.state), first);
        writeHex128Field(out, RefCatalogWire::life, e.incarnation, first);
        if (e.removal_started_round)
            writeU64StringField(out, RefCatalogWire::remove_round, *e.removal_started_round, first);
        if (e.creator)
        {
            writeStringField(out, RefCatalogWire::creator, e.creator->server_root_id, first);
            writeU64StringField(out, RefCatalogWire::creator_epoch, e.creator->writer_epoch, first);
            writeU64StringField(out, RefCatalogWire::creator_fence, e.creator->fence_generation, first);
        }
        closeObject(out, first);
        closeLine("entry");
    }

    const size_t trailer_start = out.size();
    writeTrailerLine(out, catalog.entries.size());   /// emits its own terminator
    checkLineBytes(out.size() - trailer_start - 1, "trailer");

    return std::move(out).take();
}

RefCatalog decodeRefCatalog(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::RefCatalog);
    const uint64_t line_cap = traitsFor(FormatId::RefCatalog).line_cap;

    RefCatalog catalog;
    uint64_t seen = 0;
    /// One line scratch and one reader for the whole loop: a decoder that rebuilds them per
    /// row pays an allocation per row for the seen-key store and the line, which profiling put
    /// at about a fifth of the instructions executed inside a row.
    String row_line;
    JsonObjectReader row_reader;
    for (;;)
    {
        readLineInto(in, row_line, line_cap, "ref catalog");
        ReadBufferFromMemory l(row_line.data(), row_line.size());
        row_reader.reset(l, KeyStrictness::Strict, "ref catalog");
        JsonObjectReader & r = row_reader;
        String key;
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: empty line");

        if (key == "n")
        {
            const uint64_t n = r.readU64Number();
            if (r.nextKey(key))
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: trailer has extra keys");
            if (!l.eof() || !in.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: bytes after trailer");
            if (n != seen)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS ref catalog: trailer count {} != {} records", n, seen);
            return catalog;
        }
        if (key != RefCatalogWire::kind)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: record must start with \"kind\"");
        const String kind = r.readString();
        if (kind != kEntryTag)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: unknown record kind '{}'", kind);

        String ns_str;
        std::optional<String> st_word;
        std::optional<UInt128> inc;
        std::optional<String> csr;
        std::optional<uint64_t> cwe;
        std::optional<uint64_t> cfg;
        std::optional<uint64_t> removal_started_round;
        while (r.nextKey(key))
        {
            if (key == RefCatalogWire::ns) ns_str = r.readString();
            else if (key == RefCatalogWire::state) st_word = r.readString();
            else if (key == RefCatalogWire::life) inc = r.readHex128();
            else if (key == RefCatalogWire::creator) csr = r.readString();
            else if (key == RefCatalogWire::creator_epoch) cwe = r.readU64String();
            else if (key == RefCatalogWire::creator_fence) cfg = r.readU64String();
            else if (key == RefCatalogWire::remove_round) removal_started_round = r.readU64String();
            else throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: unknown entry key '{}'", key);
        }
        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: junk after record");

        if (!st_word)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: entry '{}' missing state", ns_str);
        const NsState state = nsStateFromWord(*st_word);   /// throws CORRUPTED_DATA on an unknown word

        /// A missing "ns" key reads as the same empty string a present-but-empty one would, and both
        /// are refused identically here -- an empty namespace would sort first (every non-empty
        /// namespace compares strictly greater than ""), passing the canonical-order check below, and
        /// then wedge every later catalog-driven pass that tries to build a ref/namespace-file key
        /// from it (`Layout::checkNamespace` refuses an empty namespace).
        if (ns_str.empty())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: an entry's namespace must not be empty");
        if (ns_str.size() > kMaxNamespaceBytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS ref catalog: namespace '{}' is {} bytes, over the {}-byte admission bound",
                ns_str, ns_str.size(), kMaxNamespaceBytes);

        if (!inc)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref catalog: entry '{}' missing life", ns_str);
        if (*inc == 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS ref catalog: namespace '{}' has a zero incarnation -- 0 never names a life", ns_str);

        const bool any_creator_field = csr || cwe || cfg;
        const bool every_creator_field = csr && cwe && cfg;
        std::optional<CreatorFence> creator;
        if (state == NsState::Creating)
        {
            if (!every_creator_field)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS ref catalog: namespace '{}' is Creating but its creator fence is incomplete -- "
                    "server_root_id, writer_epoch and fence_generation are all required", ns_str);
            creator = CreatorFence{.server_root_id = *csr, .writer_epoch = *cwe, .fence_generation = *cfg};
        }
        else if (any_creator_field)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS ref catalog: namespace '{}' carries a creator fence at state '{}' -- creator is "
                "forbidden on anything but Creating", ns_str, *st_word);

        if ((state == NsState::Removing) != removal_started_round.has_value())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS ref catalog: namespace '{}' is {} and {} removal_started_round -- the field is "
                "required iff state == Removing",
                ns_str, *st_word, removal_started_round ? "carries" : "lacks");

        /// Canonical order, checked incrementally as records stream in: a namespace that does not
        /// compare strictly greater than the previous one is either a duplicate or out of order --
        /// both are "not canonical", and this one check rejects either shape.
        if (!catalog.entries.empty() && !(catalog.entries.back().ns.string() < ns_str))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS ref catalog: namespace '{}' does not sort strictly after the previous entry "
                "'{}' -- entries must be canonically ordered with no duplicate namespace",
                ns_str, catalog.entries.back().ns.string());

        catalog.entries.push_back(CatalogEntry{.ns = RootNamespace{ns_str}, .state = state,
                                                .incarnation = *inc, .creator = creator,
                                                .removal_started_round = removal_started_round});
        ++seen;
    }
}

void checkCatalogObjectBytes(uint64_t encoded_bytes, const RootNamespace & ns)
{
    const uint64_t cap = traitsFor(FormatId::RefCatalog).object_cap;
    if (encoded_bytes > cap)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "CAS ref catalog: admitting '{}' would grow the catalog to {} bytes, over the {}-byte "
            "object cap (predicate 1: encoded_catalog_bytes <= catalog_object_cap) -- refused before "
            "the write", ns.string(), encoded_bytes, cap);
}

String checkCatalogAdmission(const RefCatalog & candidate, const RootNamespace & admitting_ns)
{
    const String encoded = encodeRefCatalog(candidate);   /// grammar-checked; LOGICAL_ERROR on our own bug
    checkCatalogObjectBytes(encoded.size(), admitting_ns);
    return encoded;
}

}
