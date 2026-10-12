#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasLayout.h>
#include <base/types.h>
#include <string_view>

namespace DB::Cas
{

/// Read-only decode-to-JSON for `clickhouse-disks cas-inspect`: given a key of a content-addressed pool and the
/// raw bytes stored at it, renders `_pool_meta` or a blob's envelope header as JSON (no pool access: a pure
/// function of `(key, bytes)`). Throws `BAD_ARGUMENTS` for any other key; a decode failure propagates as-is.
String caInspectToJson(const Layout & layout, const String & key, std::string_view bytes);

}
