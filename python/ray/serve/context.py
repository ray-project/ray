"""
This file stores global state for a Serve application. Deployment replicas
can use this state to access metadata or the Serve controller.
"""

import logging
from dataclasses import dataclass
from typing import Callable, Optional

import ray
from ray.exceptions import RayActorError
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.common import ReplicaTag
from ray.serve._private.constants import SERVE_CONTROLLER_NAME, SERVE_NAMESPACE
from ray.serve.exceptions import RayServeException
from ray.util.annotations import DeveloperAPI
import contextvars

logger = logging.getLogger(__file__)

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

    app_name: str
    deployment: str
    replica_tag: ReplicaTag
    servable_object: Callable
    _internal_controller_name: str


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
        RayServeException: if there is no running Serve controller actor
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
    app_name: str,
    deployment: str,
    replica_tag: ReplicaTag,
    servable_object: Callable,
    controller_name: str,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        app_name=app_name,
        deployment=deployment,
        replica_tag=replica_tag,
        servable_object=servable_object,
        _internal_controller_name=controller_name,
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
        RayServeException: if there is no running Serve controller actor
        and raise_if_no_controller_running is set to True.
    """

    # Initialize ray if needed.
    ray._private.worker.global_worker._filter_logs_by_job = False
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
        if raise_if_no_controller_running:
            raise RayServeException(
                "There is no Serve instance running on this Ray cluster."
            )
        return

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


@dataclass(frozen=True)
class _RequestContext:
    route: str = ""
    request_id: str = ""
    app_name: str = ""
    multiplexed_model_id: str = ""


_serve_request_context = contextvars.ContextVar(
    "Serve internal request context variable", default=_RequestContext()
)


def _set_request_context(
    route: str = "",
    request_id: str = "",
    app_name: str = "",
    multiplexed_model_id: str = "",
):
    """Set the request context. If the value is not set,
    the current context value will be used."""

    current_request_context = _serve_request_context.get()
    _serve_request_context.set(
        _RequestContext(
            route=route or current_request_context.route,
            request_id=request_id or current_request_context.request_id,
            app_name=app_name or current_request_context.app_name,
            multiplexed_model_id=multiplexed_model_id
            or current_request_context.multiplexed_model_id,
        )
    )
