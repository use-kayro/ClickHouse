#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <Common/Exception.h>


#include <algorithm>
#include <array>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

namespace
{

/// Generation-1 baseline for every class. A future format change appends to that class's own array and
/// bumps `G_BUILD`: additive changes use the previous reader floor, while breaking changes use the
/// new generation as the floor. Existing entries are immutable history. Every class currently shares
/// this baseline; a class that outgrows it gets its own named array again, the way the pre-reset
/// history once had.
constexpr FormatChangePoint BASELINE[] = {{1, 1}};

}

std::span<const FormatChangePoint> changePoints(FormatId id)
{
    switch (id)
    {
        case FormatId::PoolMeta:
        case FormatId::Blob:
        case FormatId::PartFiles:
            return BASELINE;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "CasFormat: unknown FormatId {}", static_cast<int>(id));
}

uint32_t currentWriterVersion()
{
    return G_BUILD;
}

uint32_t currentCompatibilityVersion()
{
    /// Until roster-based write-down is implemented, every object carries the current build as its
    /// compatibility floor.
    return G_BUILD;
}

void checkCompatibility(uint32_t compatibility_version, std::string_view what)
{
    if (compatibility_version > G_BUILD)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "CAS {}: object requires reader generation {} but this build supports at most {}",
            what, compatibility_version, G_BUILD);
}

namespace
{
constexpr uint64_t kKiB = 1024;
constexpr uint64_t kMiB = 1024 * 1024;

/// Caps are 100-1000x above realistic sizes; hitting one indicates a corrupt object or protocol bug.
constexpr FormatTraits TRAITS[] =
{
    {FormatId::Blob,         "cas_blob",          TextFamily::PayloadHybrid, KeyStrictness::Tolerant, 256,        256},
    {FormatId::PoolMeta,     "cas_pool_meta",     TextFamily::Control,       KeyStrictness::Tolerant, 1 * kMiB,   64 * kKiB},
    /// `cas_part_files` is a Keeper znode value, never an S3 object: raw. The caps only bound
    /// decoding; what bounds a record in practice is Keeper's request size.
    {FormatId::PartFiles,    "cas_part_files",    TextFamily::PayloadHybrid, KeyStrictness::Tolerant, 256 * kMiB, 64 * kKiB},
};
}

uint64_t casMaxStoredObjectBytes()
{
    static const uint64_t bound = []
    {
        uint64_t largest = 0;
        for (const FormatTraits & t : TRAITS)
            largest = std::max(largest, t.object_cap);
        return largest;
    }();
    return bound;
}

const FormatTraits & traitsFor(FormatId id)
{
    for (const FormatTraits & t : TRAITS)
        if (t.id == id)
            return t;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "CasFormat: no traits for FormatId {} (reserved?)", static_cast<uint16_t>(id));
}

const FormatTraits * traitsForType(std::string_view type)
{
    for (const FormatTraits & t : TRAITS)
        if (t.type == type)
            return &t;
    return nullptr;
}

std::span<const FormatId> allRegisteredFormatIds()
{
    static const auto ids = []
    {
        std::array<FormatId, std::size(TRAITS)> out{};
        for (size_t i = 0; i < std::size(TRAITS); ++i)
            out[i] = TRAITS[i].id;
        return out;
    }();
    return ids;
}


}
