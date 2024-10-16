import os
from typing import Callable, Optional

import grpc

import ray
from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import (
    ClusterNodeInfoCache,
    DefaultClusterNodeInfoCache,
)
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    DeploymentScheduler,
)
from ray.serve._private.grpc_util import gRPCServer
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
    create_placement_group_fn_override: Optional[Callable] = None,
) -> DeploymentScheduler:
    head_node_id = head_node_id_override or get_head_node_id()
    return DefaultDeploymentScheduler(
        cluster_node_info_cache,
        head_node_id,
        create_placement_group_fn=create_placement_group_fn_override
        or ray.util.placement_group,
    )


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
    head_node_id_override: Optional[str] = None,
    create_placement_group_fn_override: Optional[Callable] = None,
) -> DeploymentScheduler:
    head_node_id = head_node_id_override or get_head_node_id()
    from ray.anyscale.serve._private.constants import (
        ANYSCALE_RAY_SERVE_ENABLE_PROPRIETARY_DEPLOYMENT_SCHEDULER,
    )

    if ANYSCALE_RAY_SERVE_ENABLE_PROPRIETARY_DEPLOYMENT_SCHEDULER:
        from ray.anyscale.serve._private.deployment_scheduler import (
            AnyscaleDeploymentScheduler,
        )

        deployment_scheduler_class = AnyscaleDeploymentScheduler
    else:
        deployment_scheduler_class = DefaultDeploymentScheduler
    return deployment_scheduler_class(
        cluster_node_info_cache,
        head_node_id,
        create_placement_group_fn=create_placement_group_fn_override
        or ray.util.placement_group,
    )


def add_grpc_address(grpc_server: gRPCServer, server_address: str):  # noqa: F811
    """Helper function to add a address to gRPC server.

    This overrides the original function to add secure port if the environment
    `RAY_SERVE_GRPC_SERVER_USE_SECURE_PORT` is set.
    """
    if os.environ.get("RAY_SERVE_GRPC_SERVER_USE_SECURE_PORT", "0") == "1":
        from ray._private.tls_utils import generate_self_signed_tls_certs

        cert_chain, private_key = generate_self_signed_tls_certs()
        server_creds = grpc.ssl_server_credentials(
            [(private_key.encode(), cert_chain.encode())]
        )
        grpc_server.add_secure_port(server_address, server_creds)
    else:
        grpc_server.add_insecure_port(server_address)
