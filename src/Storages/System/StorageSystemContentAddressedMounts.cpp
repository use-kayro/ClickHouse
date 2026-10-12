#include <Storages/System/StorageSystemContentAddressedMounts.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/ContentAddressedMetadataStorage.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

StorageSystemContentAddressedMounts::StorageSystemContentAddressedMounts(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"disk", std::make_shared<DataTypeString>(), "Name of the content-addressed disk."},
        {"is_leader", std::make_shared<DataTypeUInt8>(), "1 if this server's last GC round on this disk ran as the writer (only the server that writes the pool's blobs runs one)."},
        {"pending_reclaim", std::make_shared<DataTypeInt64>(), "Blobs whose last record this server's GC removed and that it has not deleted yet (a part it holds still references them)."},
        {"last_success_age_seconds", std::make_shared<DataTypeUInt64>(), "Seconds since this server's GC last ran a round as the writer on this disk (0 if it never did)."},
    }));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemContentAddressedMounts::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

Pipe StorageSystemContentAddressedMounts::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_disk = ColumnString::create();
    MutableColumnPtr col_is_leader = ColumnUInt8::create();
    MutableColumnPtr col_pending = ColumnInt64::create();
    MutableColumnPtr col_last_success = ColumnUInt64::create();

    /// One row per content-addressed disk, from this process's state only: no object-store request.
    for (const auto & [disk_name, disk] : context->getDisksMap())
    {
        const auto * ca = ContentAddressedMetadataStorage::tryFromDisk(disk);
        if (!ca)
            continue;
        const auto health = ca->gcHealth().value_or(Cas::CasGcScheduler::GcHealth{});
        col_disk->insert(disk_name);
        col_is_leader->insert(static_cast<UInt8>(health.is_leader));
        col_pending->insert(health.pending_reclaim);
        col_last_success->insert(health.last_success_age_seconds);
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_disk));
    res_columns.emplace_back(std::move(col_is_leader));
    res_columns.emplace_back(std::move(col_pending));
    res_columns.emplace_back(std::move(col_last_success));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(storage_snapshot->metadata->getSampleBlock()), std::move(chunk)));
}

}
