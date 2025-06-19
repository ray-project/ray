"""
This file stores global state for a Serve application. Deployment replicas
can use this state to access metadata or the Serve controller.
"""

import asyncio
import contextvars
import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable, Dict, Optional

import ray
from ray.exceptions import RayActorError
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ReplicaID
from ray.serve._private.config import DeploymentConfig
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve.exceptions import RayServeException
from ray.serve.grpc_util import RayServegRPCContext
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)

_INTERNAL_REPLICA_CONTEXT: "ReplicaContext" = None
_global_client: ServeControllerClient = None


@DeveloperAPI
@dataclass
class ReplicaContext:
    """Stores runtime context info for replicas.

    Fields:
        - app_name: name of the application the replica is a part of.
        - deployment: name of the deployment the replica is a part of.
        - replica_tag: unique ID for the replica.
        - servable_object: instance of the user class/function this replica is running.
    """

    replica_id: ReplicaID
    servable_object: Callable
    _deployment_config: DeploymentConfig

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
    _health_check_controller: bool = False, raise_if_no_controller_running: bool = True
) -> Optional[ServeControllerClient]:
    """Gets the global client, which stores the controller's handle.

    Args:
        _health_check_controller: If True, run a health check on the
            cached controller if it exists. If the check fails, try reconnecting
            to the controller.
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

    try:
        if _global_client is not None:
            if _health_check_controller:
                ray.get(_global_client._controller.check_alive.remote())
            return _global_client
    except RayActorError:
        logger.info("The cached controller has died. Reconnecting.")
        _set_global_client(None)

    return _connect(raise_if_no_controller_running)


def _set_global_client(client):
    global _global_client
    _global_client = client


def _get_internal_replica_context():
    return _INTERNAL_REPLICA_CONTEXT


def _set_internal_replica_context(
    *,
    replica_id: ReplicaID,
    servable_object: Callable,
    _deployment_config: DeploymentConfig,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        replica_id=replica_id,
        servable_object=servable_object,
        _deployment_config=_deployment_config,
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


_serve_request_context = contextvars.ContextVar(
    "Serve internal request context variable", default=None
)


def _get_serve_request_context():
    """Get the current request context.

    Returns:
        The current request context
    """

    if _serve_request_context.get() is None:
        _serve_request_context.set(_RequestContext())
    return _serve_request_context.get()


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
