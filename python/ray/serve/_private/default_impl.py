from ray.serve._private.cluster_node_info_cache import DefaultClusterNodeInfoCache


ClusterNodeInfoCacheImpl = DefaultClusterNodeInfoCache


def create_cluster_node_info_cache():
    return ClusterNodeInfoCacheImpl()
