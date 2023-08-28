from ray.serve._private.cluster_node_info_cache import (
    ClusterNodeInfoCache,
    DefaultClusterNodeInfoCache,
)
from ray.serve._private.deployment_scheduler import (
    DeploymentScheduler,
    DefaultDeploymentScheduler,
)


ClusterNodeInfoCacheImpl = DefaultClusterNodeInfoCache


def create_cluster_node_info_cache() -> ClusterNodeInfoCache:
    return ClusterNodeInfoCacheImpl()


def create_deployment_scheduler(
    cluster_node_info_cache: ClusterNodeInfoCache,
) -> DeploymentScheduler:
    return DefaultDeploymentScheduler(cluster_node_info_cache)
