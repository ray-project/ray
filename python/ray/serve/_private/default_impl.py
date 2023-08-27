from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import DefaultClusterNodeInfoCache


def create_cluster_node_info_cache(gcs_client: GcsClient):
    return DefaultClusterNodeInfoCache(gcs_client)
