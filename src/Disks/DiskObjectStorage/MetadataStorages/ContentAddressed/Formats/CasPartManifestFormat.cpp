#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPartManifestFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <algorithm>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

namespace PartManifestWire
{
    constexpr WireKey path{"path"};
    constexpr WireKey place{"place"};
    constexpr WireKey size{"size"};
}

namespace PartFilesWire
{
    constexpr WireKey pool{"pool"};
}

constexpr EnumWireTable<EntryPlacement, 2> kEntryPlacementWords{{{
    {EntryPlacement::Inline, "inline"},
    {EntryPlacement::Blob, "blob"},
}}};

static_assert(casEnumTableCoversEnum<kEntryPlacementWords, EntryPlacement>());

/// One entry-record line: `path`/`place`, followed by either `algo`/`digest`/`size` for a Blob or
/// `size` for Inline bytes.
void writeEntryRecord(CasJsonWriter & out, const ManifestEntry & e)
{
    bool first = true;
    writeStringField(out, PartManifestWire::path, e.path, first);
    writeWordField(out, PartManifestWire::place, entryPlacementToWireWord(e.placement), first);
    if (e.placement == EntryPlacement::Blob)
    {
        writeBlobRefFields(out, first, e.ref);   /// algo + digest
        writeNumberField(out, PartManifestWire::size, e.blob_size, first);
    }
    else
    {
        writeNumberField(out, PartManifestWire::size, e.inline_bytes.size(), first);
    }
    closeObject(out, first);
    writeChar('\n', out);
}

/// The path is written into the banner through the SAME escaper the entry-record line uses. It has to be
/// the same one: the decoder rebuilds this banner from the path it read out of the record line and
/// compares byte-wise, so any spelling difference between the two writers is an object the writer cannot
/// read back. Concatenating the path raw here is what made a part-file path containing a newline
/// undecodable -- the LF split this line, and no reader could match it again.
String bannerFor(std::string_view path, uint64_t n)
{
    CasJsonWriter w(path.size() + 32);
    w.append("==> ");
    w.stringValue(path);
    w.append(" size=");
    w.u64Number(n);
    w.append(" <==");
    return std::move(w).take();
}

/// Entry records, trailer, then the payload zone: one banner + raw bytes + '\n' per Inline entry, in
/// path order. Blob entries carry no payload-zone bytes (their bytes live in a separately addressed
/// CAS blob).
void writeEntriesAndPayload(CasJsonWriter & out, const std::vector<const ManifestEntry *> & sorted)
{
    for (const ManifestEntry * e : sorted)
        writeEntryRecord(out, *e);

    writeTrailerLine(out, sorted.size());

    for (const ManifestEntry * e : sorted)
    {
        if (e->placement != EntryPlacement::Inline)
            continue;
        const String banner = bannerFor(e->path, e->inline_bytes.size());
        out.append(banner);
        writeChar('\n', out);
        out.append(e->inline_bytes);
        writeChar('\n', out);
    }
}

/// The entry records up to and including the trailer, then the payload zone that fills every Inline
/// entry's bytes. Shared by every codec whose body is entry records plus a payload zone. `what` names the
/// object in errors; `context` is the line-reader label.
std::vector<ManifestEntry> readEntriesAndPayload(ReadBuffer & in, uint64_t line_cap, std::string_view what, std::string_view context)
{
    std::vector<ManifestEntry> entries;
    /// entry record lines, until the trailer. Inline entries remember their declared `size` so
    /// the payload zone below can read exactly that many raw bytes back into `inline_bytes`.
    /// Index-aligned with `entries` (Blob entries push an unused 0 placeholder).
    std::vector<uint64_t> inline_lens;
    String blob_ref_what;   /// reused across Blob entries so the error context does not allocate per row
    /// One line scratch and one reader for the whole loop: a decoder that rebuilds them per
    /// row pays an allocation per row for the seen-key store and the line, which profiling put
    /// at about a fifth of the instructions executed inside a row.
    String row_line;
    JsonObjectReader row_reader;
    while (true)
    {
        readLineInto(in, row_line, line_cap, context);
        ReadBufferFromMemory l(row_line.data(), row_line.size());
        row_reader.reset(l, KeyStrictness::Tolerant, context);
        JsonObjectReader & r = row_reader;
        String key;
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: empty line", what);

        if (key == "n")
        {
            const uint64_t declared_n = r.readU64Number();
            while (r.nextKey(key))
                r.skipUnknown(key);
            if (!l.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: junk after trailer", what);
            if (declared_n != entries.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "{}: trailer count {} != {} records", what, declared_n, entries.size());
            break;
        }

        if (key != PartManifestWire::path)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: record must start with \"path\"", what);
        ManifestEntry e;
        e.path = r.readString();

        /// A record's bytes come from Keeper, written by another server: enforce path hygiene so no
        /// consumer can inherit a traversal. Relative, no empty/'.'/'..' segments. (Syntactic only —
        /// legal projection subdirs pass.)
        if (e.path.empty() || e.path.front() == '/')
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: invalid entry path '{}'", what, e.path);
        for (std::string_view rest = e.path; !rest.empty();)
        {
            const size_t slash = rest.find('/');
            const std::string_view seg = rest.substr(0, slash);
            if (seg.empty() || seg == "." || seg == "..")
                throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: invalid entry path '{}'", what, e.path);
            rest = (slash == std::string_view::npos) ? std::string_view{} : rest.substr(slash + 1);
        }

        std::optional<String> pm;
        BlobRefFields blob_ref;
        std::optional<uint64_t> size;
        while (r.nextKey(key))
        {
            if (key == PartManifestWire::place) pm = r.readString();
            else if (matchBlobRefFields(key, r, blob_ref)) {}
            else if (key == PartManifestWire::size) size = r.readU64Number();
            else r.skipUnknown(key);
        }
        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: junk after record", what);
        if (!pm)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: entry '{}' missing place", what, e.path);
        e.placement = entryPlacementFromWireWord(*pm);

        if (e.placement == EntryPlacement::Blob)
        {
            if (!size)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: blob entry '{}' missing size", what, e.path);
            blob_ref_what.assign(what);
            blob_ref_what += " entry '";
            blob_ref_what += e.path;
            blob_ref_what += '\'';
            e.ref = blob_ref.build(blob_ref_what);
            e.blob_size = *size;
            inline_lens.push_back(0);   /// unused for Blob; keeps inline_lens index-aligned with entries
        }
        else
        {
            if (!size)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: inline entry '{}' missing size", what, e.path);
            inline_lens.push_back(*size);   /// bytes filled from the payload zone below
        }

        /// Canonical ascending-order and no-duplicate-path enforcement: compare only against the
        /// immediately preceding entry, requiring
        /// strict '<'. This is sufficient to catch a NON-adjacent duplicate too (e.g. forging entry
        /// c's path to equal entry a's path in an a<b<c stream): the forged c is compared against b,
        /// and since originally a<b, the forged c(=a) is NOT greater than b, so the strict check
        /// fires regardless of which earlier entry the duplicate collides with. This also keeps
        /// findEntry/entryRange's binary-search precondition (strictly ascending entries) sound.
        if (!entries.empty() && !(entries.back().path < e.path))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{}: entries not in canonical ascending order (prev '{}', got '{}')", what,
                entries.back().path, e.path);
        entries.push_back(std::move(e));
    }

    /// payload zone: for each Inline entry, in the same order it appeared above, a banner line then
    /// exactly `size` raw bytes then a terminating '\n'.
    for (size_t i = 0; i < entries.size(); ++i)
    {
        if (entries[i].placement != EntryPlacement::Inline)
            continue;
        const uint64_t n = inline_lens[i];
        const String banner_line = readLine(in, line_cap, fmt::format("{} payload zone", context));
        const String expected = bannerFor(entries[i].path, n);
        if (banner_line != expected)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{}: payload-zone banner mismatch, expected '{}', got '{}'", what, expected, banner_line);
        entries[i].inline_bytes = readFixedBytes(in, n);
        const String terminator = readFixedBytes(in, 1);
        if (terminator[0] != '\n')
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "{}: payload-zone chunk for '{}' missing terminating newline", what, entries[i].path);
    }

    return entries;
}

}

std::vector<const ManifestEntry *> sortedUniqueEntries(const std::vector<ManifestEntry> & entries, std::string_view what)
{
    std::vector<const ManifestEntry *> sorted;
    sorted.reserve(entries.size());
    for (const auto & e : entries)
        sorted.push_back(&e);
    std::sort(sorted.begin(), sorted.end(),
              [](const ManifestEntry * a, const ManifestEntry * b) { return a->path < b->path; });
    for (size_t i = 1; i < sorted.size(); ++i)
        if (sorted[i]->path == sorted[i - 1]->path)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "{}: duplicate path '{}'", what, sorted[i]->path);
    return sorted;
}

std::string_view entryPlacementToWireWord(EntryPlacement placement)
{
    return kEntryPlacementWords.toWord(placement, "PartManifest: EntryPlacement");
}

EntryPlacement entryPlacementFromWireWord(std::string_view w)
{
    return kEntryPlacementWords.fromWord(w, "PartManifest: EntryPlacement");
}

String encodePartFiles(const PartFiles & part_files)
{
    for (const auto & e : part_files.entries)
        if (e.placement != EntryPlacement::Inline)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PartFiles: entry '{}' is not Inline", e.path);
    const std::vector<const ManifestEntry *> sorted = sortedUniqueEntries(part_files.entries, "PartFiles");
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::PartFiles);
    {
        bool first = true;
        writeHex128Field(out, PartFilesWire::pool, part_files.pool_id, first);
        closeObject(out, first);
        writeChar('\n', out);
    }
    writeEntriesAndPayload(out, sorted);
    return std::move(out).take();
}

PartFiles decodePartFiles(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::PartFiles);
    const uint64_t line_cap = traitsFor(FormatId::PartFiles).line_cap;

    PartFiles part_files;
    {
        const String meta = readLine(in, line_cap, "cas_part_files");
        ReadBufferFromMemory mm(meta.data(), meta.size());
        JsonObjectReader r(mm, KeyStrictness::Tolerant, "cas_part_files");
        std::optional<UInt128> pool;
        String key;
        while (r.nextKey(key))
        {
            if (key == PartFilesWire::pool) pool = r.readHex128();
            else r.skipUnknown(key);
        }
        if (!pool)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartFiles: descriptor missing pool");
        if (!mm.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartFiles: junk after descriptor line");
        part_files.pool_id = *pool;
    }

    part_files.entries = readEntriesAndPayload(in, line_cap, "PartFiles", "cas_part_files");
    if (!in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "PartFiles: trailing bytes after the payload zone");
    for (const auto & e : part_files.entries)
        if (e.placement != EntryPlacement::Inline)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "PartFiles: entry '{}' is not Inline", e.path);
    return part_files;
}

const ManifestEntry * findEntry(const std::vector<ManifestEntry> & entries, std::string_view path)
{
    const auto it = std::lower_bound(entries.begin(), entries.end(), path,
        [](const ManifestEntry & e, std::string_view p) { return std::string_view(e.path) < p; });
    if (it == entries.end() || std::string_view(it->path) != path)
        return nullptr;
    return &*it;
}

std::pair<const ManifestEntry *, const ManifestEntry *>
entryRange(const std::vector<ManifestEntry> & entries, std::string_view dir_prefix)
{
    if (dir_prefix.empty())
        return {entries.data(), entries.data() + entries.size()};
    /// Every path starting with `dir_prefix` compares >= `dir_prefix`, and prefixed paths form a
    /// contiguous run from the first such position.
    const auto first = std::lower_bound(entries.begin(), entries.end(), dir_prefix,
        [](const ManifestEntry & e, std::string_view p) { return std::string_view(e.path) < p; });
    auto last = first;
    while (last != entries.end() && std::string_view(last->path).starts_with(dir_prefix))
        ++last;
    return {entries.data() + (first - entries.begin()), entries.data() + (last - entries.begin())};
}

}
