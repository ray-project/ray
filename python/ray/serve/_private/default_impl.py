from typing import Callable, Optional, Tuple

import ray
from ray._raylet import GcsClient
from ray.serve._private.cluster_node_info_cache import (
    ClusterNodeInfoCache,
    DefaultClusterNodeInfoCache,
)
from ray.serve._private.common import DeploymentHandleSource, DeploymentID, EndpointInfo
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_QUEUE_LENGTH_CACHE,
    RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS,
    RAY_SERVE_PROXY_PREFER_LOCAL_AZ_ROUTING,
    RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING,
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
from ray.serve._private.router import Router, SingletonThreadRouter
from ray.serve._private.utils import (
    get_current_actor_id,
    get_head_node_id,
    inside_ray_client_context,
    resolve_deployment_response,
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


def create_replica_impl(**kwargs):
    from ray.serve._private.replica import Replica

    return Replica(**kwargs)


def create_dynamic_handle_options(**kwargs):
    return DynamicHandleOptions(**kwargs)


def create_init_handle_options(**kwargs):
    return InitHandleOptions.create(**kwargs)


def _get_node_id_and_az() -> Tuple[str, Optional[str]]:
    node_id = ray.get_runtime_context().get_node_id()
    try:
        cluster_node_info_cache = create_cluster_node_info_cache(
            GcsClient(address=ray.get_runtime_context().gcs_address)
        )
        cluster_node_info_cache.update()
        az = cluster_node_info_cache.get_node_az(node_id)
    except Exception:
        az = None

    return node_id, az


# Interface definition for create_router.
CreateRouterCallable = Callable[[str, DeploymentID, InitHandleOptions], Router]


def create_router(
    handle_id: str,
    deployment_id: DeploymentID,
    handle_options: InitHandleOptions,
) -> Router:
    # NOTE(edoakes): this is lazy due to a nasty circular import that should be fixed.
    from ray.serve.context import _get_global_client

    actor_id = get_current_actor_id()
    node_id, availability_zone = _get_node_id_and_az()
    controller_handle = _get_global_client()._controller
    is_inside_ray_client_context = inside_ray_client_context()

    replica_scheduler = PowerOfTwoChoicesReplicaScheduler(
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

    return SingletonThreadRouter(
        controller_handle=controller_handle,
        deployment_id=deployment_id,
        handle_id=handle_id,
        self_actor_id=actor_id,
        handle_source=handle_options._source,
        replica_scheduler=replica_scheduler,
        # Streaming ObjectRefGenerators are not supported in Ray Client
        enable_strict_max_ongoing_requests=(
            not is_inside_ray_client_context
            and RAY_SERVE_ENABLE_STRICT_MAX_ONGOING_REQUESTS
        ),
        resolve_request_arg_func=resolve_deployment_response,
    )


def add_grpc_address(grpc_server: gRPCServer, server_address: str):
    """Helper function to add a address to gRPC server."""
    grpc_server.add_insecure_port(server_address)


def get_proxy_handle(endpoint: DeploymentID, info: EndpointInfo):
    # NOTE(zcin): needs to be lazy import due to a circular dependency.
    # We should not be importing from application_state in context.
    from ray.serve.context import _get_global_client

    client = _get_global_client()
    handle = client.get_handle(endpoint.name, endpoint.app_name, check_exists=True)

    # NOTE(zcin): It's possible that a handle is already initialized
    # if a deployment with the same name and application name was
    # deleted, then redeployed later. However this is not an issue since
    # we initialize all handles with the same init options.
    if not handle.is_initialized:
        # NOTE(zcin): since the router is eagerly initialized here, the
        # proxy will receive the replica set from the controller early.
        handle._init(
            _prefer_local_routing=RAY_SERVE_PROXY_PREFER_LOCAL_NODE_ROUTING,
            _source=DeploymentHandleSource.PROXY,
        )

    return handle.options(stream=not info.app_is_cross_language)
