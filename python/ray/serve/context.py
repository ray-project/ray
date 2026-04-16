"""
This file stores global state for a Serve application. Deployment replicas
can use this state to access metadata or the Serve controller.
"""

import asyncio
import contextvars
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import ray
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.utils import get_deployment_actor_name
from ray.serve.exceptions import RayServeException
from ray.serve.gang import GangContext
from ray.serve.grpc_util import RayServegRPCContext
from ray.serve.schema import ReplicaRank
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)

_INTERNAL_REPLICA_CONTEXT: "ReplicaContext" = None
_global_client: ServeControllerClient = None
_SET_ASGI_APP_CALLBACK = None
_INC_NUM_ONGOING_REQUESTS_CALLBACK = None
_DEC_NUM_ONGOING_REQUESTS_CALLBACK = None


@DeveloperAPI
@dataclass
class ReplicaContext:
    """Stores runtime context info for replicas.

    Fields:
        - app_name: name of the application the replica is a part of.
        - deployment: name of the deployment the replica is a part of.
        - replica_tag: unique ID for the replica.
        - servable_object: instance of the user class/function this replica is running.
        - rank: the rank of the replica.
        - world_size: the number of replicas in the deployment.
        - gang_context: context information for the gang the replica is part of.
        - code_version: code version of the deployment (for get_deployment_actor).
    """

    replica_id: ReplicaID
    servable_object: Callable
    _deployment_config: DeploymentConfig
    rank: ReplicaRank
    world_size: int
    _handle_registration_callback: Optional[Callable[[DeploymentID], None]] = None
    gang_context: Optional[GangContext] = None
    code_version: Optional[str] = None

    @property
    def app_name(self) -> str:
        return self.replica_id.deployment_id.app_name

    @property
    def deployment(self) -> str:
        return self.replica_id.deployment_id.name

    @property
    def replica_tag(self) -> str:
        return self.replica_id.unique_id


def _get_global_client(
    raise_if_no_controller_running: bool = True,
) -> Optional[ServeControllerClient]:
    """Gets the global client, which stores the controller's handle.

    Args:
        raise_if_no_controller_running: Whether to raise an exception if
            there is no currently running Serve controller.

    Returns:
        ServeControllerClient to the running Serve controller. If there
        is no running controller and raise_if_no_controller_running is
        set to False, returns None.

    Raises:
        RayServeException: If there is no running Serve controller actor
            and raise_if_no_controller_running is set to True.
    """

    if _global_client is not None:
        return _global_client

    return _connect(raise_if_no_controller_running)


def _check_cached_client_alive() -> tuple:
    """Health-check the cached controller client.

    Returns:
        (client, had_cached) tuple.
        - ``(client, True)`` — cached client is alive.
        - ``(None, True)``  — cached client existed but is unreachable;
          the cache has been cleared.  Callers should **not** attempt to
          reconnect via ``_connect()`` because GCS is likely dead and
          ``ray.get_actor()`` would hang until the 60-second C++ GCS
          reconnection timeout kills the process.
        - ``(None, False)`` — no cached client.  Callers may safely call
          ``_get_global_client()`` to discover a running controller.
    """

    if _global_client is None:
        return None, False

    try:
        ray.get(_global_client._controller.check_alive.remote(), timeout=5)
        return _global_client, True
    except Exception as e:
        logger.info(f"The cached controller has died or is unreachable: {e}.")
        _set_global_client(None)
        return None, True


def _set_global_client(client):
    global _global_client
    _global_client = client


def _get_internal_replica_context():
    return _INTERNAL_REPLICA_CONTEXT


def _set_asgi_app_callback(callback):
    """Set by Replica to allow user callables to register a custom ASGI app."""
    global _SET_ASGI_APP_CALLBACK
    _SET_ASGI_APP_CALLBACK = callback


async def set_asgi_app(app):
    """Set a custom ASGI app for direct ingress HTTP serving.

    Call this from within a deployment's initialization to serve a custom
    ASGI app on the replica's direct ingress HTTP port. The app is stored
    and started after the replica's port allocation completes.

    This enables the "ingress bypass" pattern where HAProxy routes requests
    directly to the replica's custom app, bypassing the standard Ray Serve
    request handling pipeline.
    """
    if _SET_ASGI_APP_CALLBACK is None:
        raise RuntimeError(
            "set_asgi_app can only be called from within a running Serve replica"
        )
    await _SET_ASGI_APP_CALLBACK(app)


def _set_ongoing_requests_callbacks(inc_callback, dec_callback):
    """Set by Replica to allow custom ASGI apps to track ongoing requests."""
    global _INC_NUM_ONGOING_REQUESTS_CALLBACK, _DEC_NUM_ONGOING_REQUESTS_CALLBACK
    _INC_NUM_ONGOING_REQUESTS_CALLBACK = inc_callback
    _DEC_NUM_ONGOING_REQUESTS_CALLBACK = dec_callback


def inc_num_ongoing_requests():
    """Increment the ongoing request count for this replica.

    Call this from direct-ingress ASGI middleware when a request starts.
    The count is visible to the request router's `get_num_ongoing_requests()`
    probe, enabling load-aware routing decisions for bypassed requests.
    """
    if _INC_NUM_ONGOING_REQUESTS_CALLBACK is not None:
        _INC_NUM_ONGOING_REQUESTS_CALLBACK()


def dec_num_ongoing_requests():
    """Decrement the ongoing request count for this replica.

    Call this from direct-ingress ASGI middleware when a request completes.
    """
    if _DEC_NUM_ONGOING_REQUESTS_CALLBACK is not None:
        _DEC_NUM_ONGOING_REQUESTS_CALLBACK()


def _get_deployment_actor(actor_name: str):
    """Get a handle to a deployment-scoped actor by name.

    Thin wrapper around ``ray.get_actor`` with the Serve deployment-actor naming
    convention. See ``serve.get_deployment_actor`` docstring for behavior,
    ``ValueError``/``RayActorError`` expectations, and refresh patterns.
    """
    internal_context = _get_internal_replica_context()
    if internal_context is None:
        raise RayServeException(
            "`serve.get_deployment_actor()` may only be called from within "
            "a Ray Serve deployment replica."
        )
    deployment_id = internal_context.replica_id.deployment_id
    return ray.get_actor(
        get_deployment_actor_name(
            deployment_id,
            actor_name,
            code_version=internal_context.code_version,
        ),
        namespace=SERVE_NAMESPACE,
    )


def _set_internal_replica_context(
    *,
    replica_id: ReplicaID,
    servable_object: Callable,
    _deployment_config: DeploymentConfig,
    rank: ReplicaRank,
    world_size: int,
    handle_registration_callback: Optional[Callable[[str, str], None]] = None,
    gang_context: Optional[GangContext] = None,
    code_version: Optional[str] = None,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        replica_id=replica_id,
        servable_object=servable_object,
        _deployment_config=_deployment_config,
        rank=rank,
        world_size=world_size,
        _handle_registration_callback=handle_registration_callback,
        gang_context=gang_context,
        code_version=code_version,
    )


def _connect(raise_if_no_controller_running: bool = True) -> ServeControllerClient:
    """Connect to an existing Serve application on this Ray cluster.

    If called from within a replica, this will connect to the same Serve
    app that the replica is running in.

    Returns:
        ServeControllerClient that encapsulates a Ray actor handle to the
        existing Serve application's Serve Controller. None if there is
        no running Serve controller actor and raise_if_no_controller_running
        is set to False.

    Raises:
        RayServeException: If there is no running Serve controller actor
            and raise_if_no_controller_running is set to True.
    """

    # Initialize ray if needed.
    ray._private.worker.global_worker._filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace=SERVE_NAMESPACE)

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
    except ValueError:
        if raise_if_no_controller_running:
            raise RayServeException(
                "There is no Serve instance running on this Ray cluster."
            )
        return

    client = ServeControllerClient(
        controller,
    )
    _set_global_client(client)
    return client


# Serve request context var which is used for storing the internal
# request context information.
# route_prefix: http url route path, e.g. http://127.0.0.1:/app
#     the route is "/app". When you send requests by handle,
#     the route is empty.
# request_id: the request id is generated from http proxy, the value
#     shouldn't be changed when the variable is set.
#     This can be from the client and is used for logging.
# _internal_request_id: the request id is generated from the proxy. Used to track the
#     request objects in the system.
# note:
#   The request context is readonly to avoid potential
#       async task conflicts when using it concurrently.


@dataclass(frozen=True)
class _RequestContext:
    route: str = ""
    request_id: str = ""
    _internal_request_id: str = ""
    app_name: str = ""
    multiplexed_model_id: str = ""
    grpc_context: Optional[RayServegRPCContext] = None
    is_http_request: bool = False
    cancel_on_parent_request_cancel: bool = False
    # The client address in "host:port" format, if available.
    _client: str = ""
    # Ray tracing context for this request (if tracing is enabled)
    # This is extracted from _ray_trace_ctx kwarg at the replica entry point
    # Advanced users can access this to propagate tracing to external systems
    _ray_trace_ctx: Optional[dict] = None


_serve_request_context = contextvars.ContextVar(
    "Serve internal request context variable", default=None
)

_serve_batch_request_context = contextvars.ContextVar(
    "Serve internal batching request context variable", default=None
)


def _get_serve_request_context():
    """Get the current request context.

    Returns:
        The current request context
    """

    if _serve_request_context.get() is None:
        _serve_request_context.set(_RequestContext())
    return _serve_request_context.get()


def _get_serve_batch_request_context():
    """Get the list of request contexts for the current batch."""
    if _serve_batch_request_context.get() is None:
        _serve_batch_request_context.set([])
    return _serve_batch_request_context.get()


def _set_request_context(
    route: str = "",
    request_id: str = "",
    _internal_request_id: str = "",
    app_name: str = "",
    multiplexed_model_id: str = "",
):
    """Set the request context. If the value is not set,
    the current context value will be used."""

    current_request_context = _get_serve_request_context()

    _serve_request_context.set(
        _RequestContext(
            route=route or current_request_context.route,
            request_id=request_id or current_request_context.request_id,
            _internal_request_id=_internal_request_id
            or current_request_context._internal_request_id,
            app_name=app_name or current_request_context.app_name,
            multiplexed_model_id=multiplexed_model_id
            or current_request_context.multiplexed_model_id,
        )
    )


def _unset_request_context():
    """Unset the request context."""
    _serve_request_context.set(_RequestContext())


def _set_batch_request_context(request_contexts: List[_RequestContext]):
    """Add the request context to the batch request context."""
    _serve_batch_request_context.set(request_contexts)


# `_requests_pending_assignment` is a map from request ID to a
# dictionary of asyncio tasks.
# The request ID points to an ongoing request that is executing on the
# current replica, and the asyncio tasks are ongoing tasks started on
# the router to assign child requests to downstream replicas.

# A dictionary is used over a set to track the asyncio tasks for more
# efficient addition and deletion time complexity. A uniquely generated
# `response_id` is used to identify each task.

_requests_pending_assignment: Dict[str, Dict[str, asyncio.Task]] = defaultdict(dict)


# Note that the functions below that manipulate
# `_requests_pending_assignment` are NOT thread-safe. They are only
# expected to be called from the same thread/asyncio event-loop.


def _get_requests_pending_assignment(parent_request_id: str) -> Dict[str, asyncio.Task]:
    if parent_request_id in _requests_pending_assignment:
        return _requests_pending_assignment[parent_request_id]

    return {}


def _add_request_pending_assignment(parent_request_id: str, response_id: str, task):
    # NOTE: `parent_request_id` is the `internal_request_id` corresponding
    # to an ongoing Serve request, so it is always non-empty.
    _requests_pending_assignment[parent_request_id][response_id] = task


def _remove_request_pending_assignment(parent_request_id: str, response_id: str):
    if response_id in _requests_pending_assignment[parent_request_id]:
        del _requests_pending_assignment[parent_request_id][response_id]

    if len(_requests_pending_assignment[parent_request_id]) == 0:
        del _requests_pending_assignment[parent_request_id]


# `_in_flight_requests` is a map from request ID to a dictionary of replica results.
# The request ID points to an ongoing Serve request, and the replica results are
# in-flight child requests that have been assigned to a downstream replica.

# A dictionary is used over a set to track the replica results for more
# efficient addition and deletion time complexity. A uniquely generated
# `response_id` is used to identify each replica result.

_in_flight_requests: Dict[str, Dict[str, ReplicaResult]] = defaultdict(dict)

# Note that the functions below that manipulate `_in_flight_requests`
# are NOT thread-safe. They are only expected to be called from the
# same thread/asyncio event-loop.


def _get_in_flight_requests(parent_request_id):
    if parent_request_id in _in_flight_requests:
        return _in_flight_requests[parent_request_id]

    return {}


def _add_in_flight_request(parent_request_id, response_id, replica_result):
    _in_flight_requests[parent_request_id][response_id] = replica_result


def _remove_in_flight_request(parent_request_id, response_id):
    if response_id in _in_flight_requests[parent_request_id]:
        del _in_flight_requests[parent_request_id][response_id]

    if len(_in_flight_requests[parent_request_id]) == 0:
        del _in_flight_requests[parent_request_id]
