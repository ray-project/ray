"""
This file stores global state for a Serve application. Deployment replicas
can use this state to access metadata or the Serve controller.
"""

import logging
from dataclasses import dataclass
from typing import Callable

import ray
from ray.exceptions import RayActorError
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ReplicaTag
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve.exceptions import RayServeException
from ray.util.annotations import PublicAPI, DeveloperAPI
import contextvars

logger = logging.getLogger(__file__)

_INTERNAL_REPLICA_CONTEXT: "ReplicaContext" = None
_global_client: ServeControllerClient = None


@PublicAPI(stability="alpha")
@dataclass
class ReplicaContext:
    """Stores data for Serve API calls from within deployments."""

    deployment: str
    replica_tag: ReplicaTag
    _internal_controller_name: str
    servable_object: Callable
    app_name: str


@PublicAPI(stability="alpha")
def get_global_client(_health_check_controller: bool = False) -> ServeControllerClient:
    """Gets the global client, which stores the controller's handle.

    Args:
        _health_check_controller: If True, run a health check on the
            cached controller if it exists. If the check fails, try reconnecting
            to the controller.

    Raises:
        RayServeException: if there is no running Serve controller actor.
    """

    try:
        if _global_client is not None:
            if _health_check_controller:
                ray.get(_global_client._controller.check_alive.remote())
            return _global_client
    except RayActorError:
        logger.info("The cached controller has died. Reconnecting.")
        _set_global_client(None)

    return _connect()


def _set_global_client(client):
    global _global_client
    _global_client = client


@PublicAPI(stability="alpha")
def get_internal_replica_context():
    return _INTERNAL_REPLICA_CONTEXT


def _set_internal_replica_context(
    deployment: str,
    replica_tag: ReplicaTag,
    controller_name: str,
    servable_object: Callable,
    app_name: str,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        deployment, replica_tag, controller_name, servable_object, app_name
    )


def _connect() -> ServeControllerClient:
    """Connect to an existing Serve application on this Ray cluster.

    If calling from the driver program, the Serve app on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a replica, this will connect to the same Serve
    app that the replica is running in.

    Returns:
        ServeControllerClient that encapsulates a Ray actor handle to the
        existing Serve application's Serve Controller.

    Raises:
        RayServeException: if there is no running Serve controller actor.
    """

    # Initialize ray if needed.
    ray._private.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace=SERVE_NAMESPACE)

    # When running inside of a replica, _INTERNAL_REPLICA_CONTEXT is set to
    # ensure that the correct instance is connected to.
    if _INTERNAL_REPLICA_CONTEXT is None:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = _INTERNAL_REPLICA_CONTEXT._internal_controller_name

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(controller_name, namespace=SERVE_NAMESPACE)
    except ValueError:
        raise RayServeException(
            "There is no "
            "instance running on this Ray cluster. Please "
            "call `serve.start(detached=True) to start "
            "one."
        )

    client = ServeControllerClient(
        controller,
        controller_name,
        detached=True,
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
# note:
#   The request context is readonly to avoid potential
#       async task conflicts when using it concurrently.


@DeveloperAPI
@dataclass(frozen=True)
class RequestContext:
    route: str = ""
    request_id: str = ""
    app_name: str = ""


_serve_request_context = contextvars.ContextVar(
    "Serve internal request context variable", default=RequestContext()
)
