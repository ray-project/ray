from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import (
    ClusterNodeInfoCache,
    DefaultClusterNodeInfoCache,
)
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    DeploymentScheduler,
)


def create_cluster_node_info_cache(gcs_client: GcsClient) -> ClusterNodeInfoCache:
    return DefaultClusterNodeInfoCache(gcs_client)


def create_deployment_scheduler(
    cluster_node_info_cache: ClusterNodeInfoCache,
) -> DeploymentScheduler:
    return DefaultDeploymentScheduler(cluster_node_info_cache)
