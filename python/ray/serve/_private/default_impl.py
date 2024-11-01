import os
from typing import Any, Callable, Optional, Tuple

import grpc

import ray
from ray._raylet import GcsClient
from ray.anyscale.serve.utils import asyncio_grpc_exception_handler
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
from ray.serve._private.replica_scheduler import (
    ActorReplicaWrapper,
    PowerOfTwoChoicesReplicaScheduler,
)
from ray.serve._private.router import Router, SingletonThreadRouter
from ray.serve._private.utils import (
    get_current_actor_id,
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
    from ray.serve.handle import _DynamicHandleOptions

    return _DynamicHandleOptions(**kwargs)


def create_init_handle_options(**kwargs):
    from ray.serve.handle import _InitHandleOptions

    return _InitHandleOptions.create(**kwargs)


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
CreateRouterCallable = Callable[[str, DeploymentID, Any], Router]


def create_router(
    handle_id: str,
    deployment_id: DeploymentID,
    handle_options: Any,
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
        resolve_request_args_func=resolve_request_args,
    )


def add_grpc_address(grpc_server: gRPCServer, server_address: str):
    """Helper function to add a address to gRPC server."""
    grpc_server.add_insecure_port(server_address)


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


def create_init_handle_options(**kwargs):  # noqa: F811
    from ray.anyscale.serve._private.handle_options import _AnyscaleInitHandleOptions

    return _AnyscaleInitHandleOptions.create(**kwargs)


def create_router(  # noqa: F811
    handle_id: str,
    deployment_id: DeploymentID,
    handle_options: Any,
):
    # NOTE(edoakes): this is lazy due to a nasty circular import that should be fixed.
    from ray.anyscale.serve._private.replica_scheduler.replica_wrapper import (
        gRPCReplicaWrapper,
    )
    from ray.serve.context import _get_global_client

    actor_id = get_current_actor_id()
    node_id, availability_zone = _get_node_id_and_az()
    controller_handle = _get_global_client()._controller
    is_inside_ray_client_context = inside_ray_client_context()

    def create_replica_wrapper_func(r):
        if handle_options._by_reference:
            return ActorReplicaWrapper(r)
        else:
            return gRPCReplicaWrapper(r)

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
        create_replica_wrapper_func=create_replica_wrapper_func,
    )

    router = SingletonThreadRouter(
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
        resolve_request_args_func=resolve_request_args,
    )
    router._get_singleton_asyncio_loop().set_exception_handler(
        asyncio_grpc_exception_handler
    )
    return router


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
