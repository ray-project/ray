import asyncio
from typing import Callable, Optional

import ray
from ray._raylet import GcsClient
from ray.actor import ActorHandle
from ray.serve._private.cluster_node_info_cache import (
    ClusterNodeInfoCache,
    DefaultClusterNodeInfoCache,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    DeploymentScheduler,
)
from ray.serve._private.grpc_util import gRPCServer
from ray.serve._private.router import Router
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


def create_dynamic_handle_options(**kwargs):
    from ray.serve.handle import _DynamicHandleOptions

    return _DynamicHandleOptions(**kwargs)


def create_init_handle_options(**kwargs):
    from ray.serve.handle import _InitHandleOptions

    return _InitHandleOptions.create(**kwargs)


def create_router(
    controller_handle: ActorHandle,
    deployment_id: DeploymentID,
    handle_id: str,
    node_id: str,
    actor_id: str,
    availability_zone: Optional[str],
    event_loop: asyncio.BaseEventLoop,
    handle_options,
):
    return Router(
        controller_handle=controller_handle,
        deployment_id=deployment_id,
        handle_id=handle_id,
        self_node_id=node_id,
        self_actor_id=actor_id,
        self_availability_zone=availability_zone,
        handle_source=handle_options._source,
        event_loop=event_loop,
    )


def add_grpc_address(grpc_server: gRPCServer, server_address: str):
    """Helper function to add a address to gRPC server."""
    grpc_server.add_insecure_port(server_address)
