#pragma once
#include <cstdint>
#include <span>
#include <string_view>

namespace DB::Cas
{

/// Shared vocabulary and policy registry for persisted content-addressed objects. The registry
/// supplies the version gate and the per-object text-file traits used by the codecs in this
/// directory; it does not own object bytes or storage lifecycle state.

/// The highest pool-format generation this build understands. A build keeps every decoder for
/// generations 1..G_BUILD (new code always reads old); an object is readable iff its
/// compatibility_version <= G_BUILD. Bump this (and append a change-point in CasFormat.cpp) when a new
/// format generation is introduced.
///
/// The generation history was reset to this {1, 1} baseline: CAS is pre-release, carries no persisted
/// data, and so pays no compatibility cost for starting the count over. Every class's `changePoints`
/// begins at generation 1 until a future change appends a real entry.
constexpr uint32_t G_BUILD = 1;

/// Stable identifiers for every self-describing persisted object class. The text registry uses the
/// corresponding `type` string as the on-disk identity. Numeric values are part of the format history:
/// retired values remain unused so an old object can never be mistaken for a later class.
enum class FormatId : uint16_t
{
    Blob = 1,             /// cas_blob        -- the blob envelope
    PoolMeta = 8,         /// cas_pool_meta   -- `_pool_meta`, the pool's identity object
    PartFiles = 26,       /// cas_part_files  -- a part's Inline entries, held in Keeper (`<zookeeper_path>/cas_parts/<part>`)
    /// Every other value up to 26 belonged to a retired class (trees, GC state, ref logs/snapshots/
    /// checkpoints/catalog, part manifests, mount-safety objects, blob sidecars, the roster). Never reuse one.
};

/// Returns the writer generation stamped on newly written objects. The current pre-roster writer
/// always stamps `G_BUILD`.
uint32_t currentWriterVersion();

/// Returns the compatibility generation stamped on newly written objects. Until the roster and
/// write-down-to-floor policy exist, this is always `G_BUILD`; readers reject values above `G_BUILD`.
uint32_t currentCompatibilityVersion();

/// Applies the common fail-closed reader gate. If an object's `compatibility_version` exceeds
/// `G_BUILD`, throws `UNKNOWN_FORMAT_VERSION` before the caller interprets the body; `what` identifies
/// the object in the exception message.
void checkCompatibility(uint32_t compatibility_version, std::string_view what);

/// One append-only entry in a class's format history. At `generation`, the class's ENCODING or the
/// MEANING of what it encodes changed, and a reader must understand at least `min_reader` to read an
/// object written at that generation. Additive changes retain the previous reader floor; breaking
/// changes set the floor to the change generation itself — even when not one byte of the encoding
/// moves: a change that makes ids dense, for example, leaves the bytes readable but their MEANING
/// unreadable to an older build, so the floor is the change generation.
struct FormatChangePoint
{
    uint16_t generation;
    uint16_t min_reader;
};

/// Returns the append-only change-point history for `id`, oldest first. After the pre-release
/// generation reset every class carries the shared `{1, 1}` baseline. A class born LATER than the
/// current baseline must begin its history at its birth generation, not at 1 — claiming an earlier
/// entry would say an older reader could read an object kind that did not yet exist. Future changes
/// append entries without editing old ones.
std::span<const FormatChangePoint> changePoints(FormatId id);

/// The text-format registry has one row per decodable persisted object. Each row is the single source
/// for the header-line `type`, body family, unknown-key policy, and fail-closed
/// size caps. A format missing from this registry cannot be decoded.

/// The shape of a text object body: a materialized control object, or a descriptor followed by raw
/// payload bytes. Value 2 (the streamed record sequence) is retired with the GC source-edge run.
enum class TextFamily : uint8_t { Control = 1, PayloadHybrid = 3 };

/// Whether a decoder skips unknown ordinary keys or rejects them. Critical keys prefixed with `!`
/// are rejected by all families because they signal a required extension.
enum class KeyStrictness : uint8_t { Tolerant = 1, Strict = 2 };

/// Codec metadata for one registered text object. The byte caps apply to the object and to individual
/// text lines.
struct FormatTraits
{
    FormatId id;
    std::string_view type;      /// header-line "type" value
    TextFamily family;
    KeyStrictness strictness;
    uint64_t object_cap;        /// max DECOMPRESSED object bytes
    uint64_t line_cap;          /// max bytes of one text line
};

/// Returns the traits for `id`.
const FormatTraits & traitsFor(FormatId id);
/// Looks up a header-line `type` string. Returns nullptr for an unregistered type; it does not throw
/// because callers use this result to classify the input before decoding it.
const FormatTraits * traitsForType(std::string_view type);
std::span<const FormatId> allRegisteredFormatIds();

/// The largest number of bytes any materialized object may occupy: the largest whole-object cap in the
/// registry. A materialized read refuses anything larger rather than allocating it.
uint64_t casMaxStoredObjectBytes();

}
