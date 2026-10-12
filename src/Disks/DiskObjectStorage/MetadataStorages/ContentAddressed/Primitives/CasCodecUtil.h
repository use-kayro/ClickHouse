#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasTypes.h>
#include <IO/ReadBuffer.h>
#include <Common/Exception.h>
#include <string_view>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

/// Shared low-level byte-encoding helpers for CAS: the big-endian `UInt128` wire form and exact reads.
/// They remain independent of any particular object format.

/// On-disk UInt128 wire form: the 16-byte big-endian representation used by raw CAS byte fields and
/// key components. It is FROZEN — changing the bytes breaks every object already written. (The
/// lowercase-hex form lives in `CasTypes.h` as `u128ToHex` / `hexToU128` and is out of scope here.)
///
/// Converts `v` to the frozen 16-byte big-endian representation used in raw byte fields and keys.
inline std::string u128ToBytesBE(const UInt128 & v)
{
    std::string out(16, '\0');
    for (int i = 0; i < 16; ++i)
        out[i] = static_cast<char>(static_cast<UInt8>(v >> (8 * (15 - i))));
    return out;
}

/// Read exactly `n` raw bytes. The bounds check MUST precede the allocation: `n` typically comes
/// from a length field just read off the wire, so on corrupted input it can be huge (a u32 field
/// admits 4 GiB) — allocating first would mean a multi-GiB transient allocation, which under a
/// memory tracker surfaces as MEMORY_LIMIT_EXCEEDED instead of the pinned CORRUPTED_DATA.
/// Comparing against `available` as the exact remainder is valid because all CAS codec decoding
/// reads from `ReadBufferFromMemory`: the whole object is in memory, so `available` is exactly
/// the number of bytes left.
/// Throws `CORRUPTED_DATA` before allocating when the encoded object cannot contain `n` bytes.
inline String readFixedBytes(ReadBuffer & in, size_t n)
{
    if (n > in.available())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS codec: truncated encoded data: need {} bytes, {} available", n, in.available());
    String s(n, '\0');
    in.readStrict(s.data(), n);
    return s;
}

}
