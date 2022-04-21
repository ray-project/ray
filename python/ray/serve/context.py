"""
This file stores global state for a Serve application. Deployment replicas
can use this state to access metadata or the Serve controller.
"""

import logging
from dataclasses import dataclass
from typing import Callable, Optional

import ray
from ray.exceptions import RayActorError
from ray.serve.common import ReplicaTag
from ray.serve.constants import SERVE_CONTROLLER_NAME
from ray.serve.exceptions import RayServeException
from ray.serve.client import ServeControllerClient, get_controller_namespace

logger = logging.getLogger(__file__)

_INTERNAL_REPLICA_CONTEXT: "ReplicaContext" = None
_global_client: ServeControllerClient = None


@dataclass
class ReplicaContext:
    """Stores data for Serve API calls from within deployments."""

    deployment: str
    replica_tag: ReplicaTag
    _internal_controller_name: str
    _internal_controller_namespace: str
    servable_object: Callable


def get_global_client(
    _override_controller_namespace: Optional[str] = None,
    _health_check_controller: bool = False,
) -> ServeControllerClient:
    """Gets the global client, which stores the controller's handle.

    Args:
        _override_controller_namespace (Optional[str]): If None and there's no
            cached client, searches for the controller in this namespace.
        _health_check_controller (bool): If True, run a health check on the
            cached controller if it exists. If the check fails, try reconnecting
            to the controller.

    Raises:
        RayServeException: if there is no Serve controller actor in the
            expected namespace.
    """

    try:
        if _global_client is not None:
            if _health_check_controller:
                ray.get(_global_client._controller.check_alive.remote())
            return _global_client
    except RayActorError:
        logger.info("The cached controller has died. Reconnecting.")
        set_global_client(None)

    return _connect(_override_controller_namespace=_override_controller_namespace)


def set_global_client(client):
    global _global_client
    _global_client = client


def get_internal_replica_context():
    return _INTERNAL_REPLICA_CONTEXT


def set_internal_replica_context(
    deployment: str,
    replica_tag: ReplicaTag,
    controller_name: str,
    controller_namespace: str,
    servable_object: Callable,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        deployment, replica_tag, controller_name, controller_namespace, servable_object
    )


def _connect(
    _override_controller_namespace: Optional[str] = None,
) -> ServeControllerClient:
    """Connect to an existing Serve instance on this Ray cluster.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a replica, this will connect to the same Serve
    instance that the replica is running in.

    Args:
        _override_controller_namespace (Optional[str]): The namespace to use
            when looking for the controller. If None, Serve recalculates the
            controller's namespace using get_controller_namespace().

    Raises:
        RayServeException: if there is no Serve controller actor in the
            expected namespace.
    """

    # Initialize ray if needed.
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

    # When running inside of a replica, _INTERNAL_REPLICA_CONTEXT is set to
    # ensure that the correct instance is connected to.
    if _INTERNAL_REPLICA_CONTEXT is None:
        controller_name = SERVE_CONTROLLER_NAME
        controller_namespace = get_controller_namespace(
            detached=True, _override_controller_namespace=_override_controller_namespace
        )
    else:
        controller_name = _INTERNAL_REPLICA_CONTEXT._internal_controller_name
        controller_namespace = _INTERNAL_REPLICA_CONTEXT._internal_controller_namespace

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(controller_name, namespace=controller_namespace)
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
        _override_controller_namespace=_override_controller_namespace,
    )
    set_global_client(client)
    return client
