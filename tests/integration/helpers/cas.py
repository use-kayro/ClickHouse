import os

CONFIG_D = "/etc/clickhouse-server/config.d/"


def join_pool_after_writer(node, test_dir, disk_configs):
    """Mount a brand-new CAS pool on a reader once the writer has minted its `_pool_meta`.

    Only the writer mints `_pool_meta` (TEC-851 step 3c), and a reader refuses to boot on a pool without it.
    The cluster starts every instance at once, so a reader is added without its CAS disk configs, and gets
    them here, after the writer is up, followed by a restart.
    """
    for config in disk_configs:
        node.copy_file_to_container(os.path.join(test_dir, config), CONFIG_D + os.path.basename(config))
    node.restart_clickhouse()
