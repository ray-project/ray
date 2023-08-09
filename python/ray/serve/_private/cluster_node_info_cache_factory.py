from ray.serve._private.cluster_node_info_cache import ClusterNodeInfoCache
from ray._raylet import GcsClient


def create_cluster_node_info_cache(gcs_client: GcsClient):
    return ClusterNodeInfoCache(gcs_client)
