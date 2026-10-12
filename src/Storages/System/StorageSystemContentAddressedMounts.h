#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class Context;

/// system.cas_mounts: one row per content-addressed disk of this server and its GC's health, read from
/// process state only.
class StorageSystemContentAddressedMounts final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemContentAddressedMounts(const StorageID & table_id_);

    std::string getName() const override { return "SystemContentAddressedMounts"; }

    static VirtualColumnsDescription createVirtuals();

    using StorageWithCommonVirtualColumns::read;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }
};

}
