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
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE,
    RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
)
from ray.serve._private.deployment_scheduler import (
    DefaultDeploymentScheduler,
    DeploymentScheduler,
)
from ray.serve._private.grpc_util import gRPCServer
from ray.serve._private.handle_options import DynamicHandleOptions, InitHandleOptions
from ray.serve._private.replica_scheduler import (
    ActorReplicaWrapper,
    PowerOfTwoChoicesReplicaScheduler,
)
from ray.serve._private.router import Router
from ray.serve._private.utils import (
    get_head_node_id,
    inside_ray_client_context,
    resolve_request_args,
)

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
    return DynamicHandleOptions(**kwargs)


def create_init_handle_options(**kwargs):
    return InitHandleOptions.create(**kwargs)


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
    is_inside_ray_client_context = inside_ray_client_context()

    replica_scheduler = PowerOfTwoChoicesReplicaScheduler(
        event_loop,
        deployment_id,
        handle_options._source,
        handle_options._prefer_local_routing,
        RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
        node_id,
        actor_id,
        ray.get_runtime_context().current_actor
        if ray.get_runtime_context().get_actor_id()
        else None,
        availability_zone,
        # Streaming ObjectRefGenerators are not supported in Ray Client
        use_replica_queue_len_cache=(
            not is_inside_ray_client_context and RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE
        ),
        create_replica_wrapper_func=lambda r: ActorReplicaWrapper(r),
    )

    return Router(
        controller_handle=controller_handle,
        deployment_id=deployment_id,
        handle_id=handle_id,
        self_actor_id=actor_id,
        handle_source=handle_options._source,
        event_loop=event_loop,
        replica_scheduler=replica_scheduler,
        # Streaming ObjectRefGenerators are not supported in Ray Client
        enable_strict_max_ongoing_requests=(
            not is_inside_ray_client_context
            and RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS
        ),
        resolve_request_args_func=resolve_request_args,
    )


def add_grpc_address(grpc_server: gRPCServer, server_address: str):
    """Helper function to add a address to gRPC server."""
    grpc_server.add_insecure_port(server_address)
