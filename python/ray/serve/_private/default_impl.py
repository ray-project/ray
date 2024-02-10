from typing import Optional

from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import (
    ClusterNodeInfoCache,
    DefaultClusterNodeInfoCache,
)
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    DeploymentScheduler,
)
from ray.serve._private.utils import get_head_node_id

# NOTE: Please read carefully before changing!
#
# These methods are common extension points, therefore these should be
# changed as a Developer API, ie methods should not be renamed, have their
# API modified w/o substantial enough justification


def create_cluster_node_info_cache(gcs_client: GcsClient) -> ClusterNodeInfoCache:
    return DefaultClusterNodeInfoCache(gcs_client)


def create_deployment_scheduler(
    cluster_node_info_cache: ClusterNodeInfoCache,
    head_node_id_override: Optional[str] = None,
) -> DeploymentScheduler:
    head_node_id = head_node_id_override or get_head_node_id()
    return DefaultDeploymentScheduler(cluster_node_info_cache, head_node_id)


# Anyscale overrides


def create_cluster_node_info_cache(  # noqa: F811
    gcs_client: GcsClient,
) -> ClusterNodeInfoCache:
    from ray.anyscale.serve._private.cluster_node_info_cache import (
        AnyscaleClusterNodeInfoCache,
    )

    return AnyscaleClusterNodeInfoCache(gcs_client)


def create_deployment_scheduler(  # noqa: F811
    cluster_node_info_cache: ClusterNodeInfoCache,
) -> DeploymentScheduler:
    from ray.anyscale.serve._private.constants import (
        ANYSCALE_RAY_SERVE_ENABLE_PROPRIETARY_DEPLOYMENT_SCHEDULER,
    )

    if ANYSCALE_RAY_SERVE_ENABLE_PROPRIETARY_DEPLOYMENT_SCHEDULER:
        from ray.anyscale.serve._private.deployment_scheduler import (
            AnyscaleDeploymentScheduler,
        )

        return AnyscaleDeploymentScheduler(cluster_node_info_cache)
    else:
        head_node_id = head_node_id_override or get_head_node_id()
        return DefaultDeploymentScheduler(cluster_node_info_cache, head_node_id)