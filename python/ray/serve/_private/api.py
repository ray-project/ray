from typing import Dict, Optional, Union
import logging
import os

import ray
from ray._private.usage import usage_lib
from ray.serve.deployment import Deployment
from ray.serve.exceptions import RayServeException
from ray.serve.config import HTTPOptions
from ray.serve._private.constants import (
    CONTROLLER_MAX_CONCURRENCY,
    HTTP_PROXY_TIMEOUT,
    SERVE_CONTROLLER_NAME,
    SERVE_EXPERIMENTAL_DISABLE_HTTP_PROXY,
    SERVE_NAMESPACE,
    RAY_INTERNAL_SERVE_CONTROLLER_PIN_ON_NODE,
)
from ray.serve._private.client import ServeControllerClient

from ray.serve._private.utils import (
    format_actor_name,
    get_random_letters,
)
from ray.serve.controller import ServeController
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.serve.context import (
    get_global_client,
    _set_global_client,
)


logger = logging.getLogger(__file__)

FLAG_DISABLE_HTTP_PROXY = (
    os.environ.get(SERVE_EXPERIMENTAL_DISABLE_HTTP_PROXY, "0") == "1"
)


def get_deployment(name: str):
    """Dynamically fetch a handle to a Deployment object.

    Args:
        name: name of the deployment. This must have already been
        deployed.

    Returns:
        Deployment
    """
    try:
        (
            deployment_info,
            route_prefix,
        ) = get_global_client().get_deployment_info(name)
    except KeyError:
        raise KeyError(
            f"Deployment {name} was not found. Did you call Deployment.deploy()?"
        )
    return Deployment(
        deployment_info.replica_config.deployment_def,
        name,
        deployment_info.deployment_config,
        version=deployment_info.version,
        init_args=deployment_info.replica_config.init_args,
        init_kwargs=deployment_info.replica_config.init_kwargs,
        route_prefix=route_prefix,
        ray_actor_options=deployment_info.replica_config.ray_actor_options,
        _internal=True,
    )


def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = get_global_client().list_deployments()

    deployments = {}
    for name, (deployment_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            deployment_info.replica_config.deployment_def,
            name,
            deployment_info.deployment_config,
            version=deployment_info.version,
            init_args=deployment_info.replica_config.init_args,
            init_kwargs=deployment_info.replica_config.init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=deployment_info.replica_config.ray_actor_options,
            _internal=True,
        )

    return deployments


def _check_http_options(
    client: ServeControllerClient, http_options: Union[dict, HTTPOptions]
) -> None:
    if http_options:
        client_http_options = client.http_config
        new_http_options = (
            http_options
            if isinstance(http_options, HTTPOptions)
            else HTTPOptions.parse_obj(http_options)
        )
        different_fields = []
        all_http_option_fields = new_http_options.__dict__
        for field in all_http_option_fields:
            if getattr(new_http_options, field) != getattr(client_http_options, field):
                different_fields.append(field)

        if len(different_fields):
            logger.warning(
                "The new client HTTP config differs from the existing one "
                f"in the following fields: {different_fields}. "
                "The new HTTP config is ignored."
            )


def serve_start(
    detached: bool = False,
    http_options: Optional[Union[dict, HTTPOptions]] = None,
    dedicated_cpu: bool = False,
    **kwargs,
) -> ServeControllerClient:
    """Initialize a serve instance.

    By default, the instance will be scoped to the lifetime of the returned
    Client object (or when the script exits). If detached is set to True, the
    instance will instead persist until serve.shutdown() is called. This is
    only relevant if connecting to a long-running Ray cluster (e.g., with
    ray.init(address="auto") or ray.init("ray://<remote_addr>")).

    Args:
        detached: Whether not the instance should be detached from this
          script. If set, the instance will live on the Ray cluster until it is
          explicitly stopped with serve.shutdown().
        http_options (Optional[Dict, serve.HTTPOptions]): Configuration options
          for HTTP proxy. You can pass in a dictionary or HTTPOptions object
          with fields:

            - host(str, None): Host for HTTP servers to listen on. Defaults to
              "127.0.0.1". To expose Serve publicly, you probably want to set
              this to "0.0.0.0".
            - port(int): Port for HTTP server. Defaults to 8000.
            - root_path(str): Root path to mount the serve application
              (for example, "/serve"). All deployment routes will be prefixed
              with this path. Defaults to "".
            - middlewares(list): A list of Starlette middlewares that will be
              applied to the HTTP servers in the cluster. Defaults to [].
            - location(str, serve.config.DeploymentMode): The deployment
              location of HTTP servers:

                - "HeadOnly": start one HTTP server on the head node. Serve
                  assumes the head node is the node you executed serve.start
                  on. This is the default.
                - "EveryNode": start one HTTP server per node.
                - "NoServer" or None: disable HTTP server.
            - num_cpus (int): The number of CPU cores to reserve for each
              internal Serve HTTP proxy actor.  Defaults to 0.
        dedicated_cpu: Whether to reserve a CPU core for the internal
          Serve controller actor.  Defaults to False.
    """
    usage_lib.record_library_usage("serve")

    # Initialize ray if needed.
    ray._private.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace=SERVE_NAMESPACE)

    try:
        client = get_global_client(_health_check_controller=True)
        logger.info(
            f'Connecting to existing Serve app in namespace "{SERVE_NAMESPACE}".'
            " New http options will not be applied."
        )
        if http_options:
            _check_http_options(client, http_options)
        return client
    except RayServeException:
        pass

    if detached:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = format_actor_name(get_random_letters(), SERVE_CONTROLLER_NAME)

    # Used for scheduling things to the head node explicitly.
    # Assumes that `serve.start` runs on the head node.
    head_node_id = ray.get_runtime_context().node_id.hex()
    controller_actor_options = {
        "num_cpus": 1 if dedicated_cpu else 0,
        "name": controller_name,
        "lifetime": "detached" if detached else None,
        "max_restarts": -1,
        "max_task_retries": -1,
        # Schedule the controller on the head node with a soft constraint. This
        # prefers it to run on the head node in most cases, but allows it to be
        # restarted on other nodes in an HA cluster.
        "scheduling_strategy": NodeAffinitySchedulingStrategy(head_node_id, soft=True)
        if RAY_INTERNAL_SERVE_CONTROLLER_PIN_ON_NODE
        else None,
        "namespace": SERVE_NAMESPACE,
        "max_concurrency": CONTROLLER_MAX_CONCURRENCY,
    }

    if FLAG_DISABLE_HTTP_PROXY:
        controller = ServeController.options(**controller_actor_options).remote(
            controller_name,
            http_config=http_options,
            head_node_id=head_node_id,
            detached=detached,
            _disable_http_proxy=True,
        )
    else:
        # Legacy http proxy actor check
        http_deprecated_args = ["http_host", "http_port", "http_middlewares"]
        for key in http_deprecated_args:
            if key in kwargs:
                raise ValueError(
                    f"{key} is deprecated, please use serve.start(http_options="
                    f'{{"{key}": {kwargs[key]}}}) instead.'
                )

        if isinstance(http_options, dict):
            http_options = HTTPOptions.parse_obj(http_options)
        if http_options is None:
            http_options = HTTPOptions()

        controller = ServeController.options(**controller_actor_options).remote(
            controller_name,
            http_config=http_options,
            head_node_id=head_node_id,
            detached=detached,
        )

        proxy_handles = ray.get(controller.get_http_proxies.remote())
        if len(proxy_handles) > 0:
            try:
                ray.get(
                    [handle.ready.remote() for handle in proxy_handles.values()],
                    timeout=HTTP_PROXY_TIMEOUT,
                )
            except ray.exceptions.GetTimeoutError:
                raise TimeoutError(
                    f"HTTP proxies not available after {HTTP_PROXY_TIMEOUT}s."
                )

    client = ServeControllerClient(
        controller,
        controller_name,
        detached=detached,
    )
    _set_global_client(client)
    logger.info(
        f"Started{' detached ' if detached else ' '}Serve instance in "
        f'namespace "{SERVE_NAMESPACE}".'
    )
    return client
