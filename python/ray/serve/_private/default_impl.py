from ray.anyscale.serve._private.cluster_node_info_cache import (
    AnyscaleClusterNodeInfoCache,
)
from ray._raylet import GcsClient


def create_cluster_node_info_cache(gcs_client: GcsClient):
    return AnyscaleClusterNodeInfoCache(gcs_client)
