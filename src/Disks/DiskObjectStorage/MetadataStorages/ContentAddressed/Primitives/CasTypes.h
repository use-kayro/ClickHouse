#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <base/defines.h>
#include <base/extended_types.h>
#include <base/hex.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <fmt/format.h>
#include <string>
#include <string_view>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}
}

namespace DB::Cas
{

/// Strongly typed value types and codecs shared by the content-addressed metadata and object paths.
///
/// These types deliberately keep protocol identity, content digests, backend tokens, and their
/// textual forms distinct. Their ordering and equality operators make them usable as ordered keys;
/// hash specializations below support unordered containers. Conversion to a storage key or wire
/// representation remains explicit at the boundary that owns that representation.
///
/// Opaque namespace under which root manifests live. The core never interprets its contents:
/// the wiring composes strings like "srv1/<table_uuid>" or "srv1/shadow/<backup>/<table_uuid>".
/// Layout only validates its shape (non-empty, no leading/trailing or empty segments,
/// no segment equal to the reserved "_files").
/// Construction from `String` is explicit, preventing unrelated identifier types from being mixed;
/// the underlying string is exposed only through `string` at object-storage boundaries.
class RootNamespace
{
public:
    RootNamespace() = default;
    explicit RootNamespace(String value_) : value(std::move(value_)) {}
    const String & string() const { return value; }
    auto operator<=>(const RootNamespace &) const = default;
    bool operator==(const RootNamespace &) const = default;

private:
    String value;
};

/// Returns 32 lowercase hex chars encoding `v`.
inline String u128ToHex(const UInt128 & v)
{
    return getHexUIntLowercase(v);
}

/// Parses 32-char lowercase (or uppercase) hex string to UInt128.
/// Throws BAD_ARGUMENTS on wrong length or non-hex characters.
inline UInt128 hexToU128(const String & hex)
{
    if (hex.size() != 32)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
            "hexToU128: expected 32 hex chars, got {}", hex.size());

    // Validate each character is a valid hex digit (unhex(c) returns 0xff for invalid chars).
    for (char c : hex)
    {
        if (::unhex(c) == 0xff)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                "hexToU128: invalid hex character '{}'", c);
    }

    return unhexUInt<UInt128>(hex.data());
}

}

namespace std
{

template <>
struct hash<DB::Cas::RootNamespace>
{
    size_t operator()(const DB::Cas::RootNamespace & value) const noexcept
    {
        return std::hash<std::string>{}(value.string());
    }
};

}

namespace DB::Cas
{

/// How a backend identifies one physical incarnation of an object key.
enum class Dialect : uint8_t
{
    ETag = 1,        /// S3-family / Azure
    Generation = 2,  /// GCS (binding deferred; fail-closed until probed)
    Emulated = 3,    /// test backends (in-memory fake, Local emulation)
};

}
