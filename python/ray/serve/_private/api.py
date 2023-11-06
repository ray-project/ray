import inspect
import logging
from types import FunctionType
from typing import Any, Dict, Tuple, Union

import ray
from ray._private.pydantic_compat import BaseModel
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
from ray._private.usage import usage_lib
from ray.actor import ActorHandle
from ray.serve._private.client import ServeControllerClient
from ray.serve._private.constants import (
    CONTROLLER_MAX_CONCURRENCY,
    HTTP_PROXY_TIMEOUT,
    SERVE_CONTROLLER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.controller import ServeController
from ray.serve.config import HTTPOptions, gRPCOptions
from ray.serve.context import _get_global_client, _set_global_client
from ray.serve.deployment import Application, Deployment
from ray.serve.exceptions import RayServeException

logger = logging.getLogger(__file__)


def get_deployment(name: str, app_name: str = ""):
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
        ) = _get_global_client().get_deployment_info(name, app_name)
    except KeyError:
        if len(app_name) == 0:
            msg = (
                f"Deployment {name} was not found. Did you call Deployment.deploy()? "
                "Note that `serve.get_deployment()` can only be used to fetch a "
                "deployment that was deployed using the 1.x API `Deployment.deploy()`. "
                "If you want to fetch a handle to an application deployed through "
                "`serve.run` or through a Serve config, please use "
                "`serve.get_app_handle()` instead."
            )
        else:
            msg = f"Deployment {name} in application {app_name} was not found."
        raise KeyError(msg)
    return Deployment(
        name,
        deployment_info.deployment_config,
        deployment_info.replica_config,
        version=deployment_info.version,
        route_prefix=route_prefix,
        _internal=True,
    )


def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active 1.x deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = _get_global_client().list_deployments_v1()

    deployments = {}
    for name, (deployment_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            name,
            deployment_info.deployment_config,
            deployment_info.replica_config,
            version=deployment_info.version,
            route_prefix=route_prefix,
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


def _start_controller(
    http_options: Union[None, dict, HTTPOptions] = None,
    grpc_options: Union[None, dict, gRPCOptions] = None,
    **kwargs,
) -> Tuple[ActorHandle, str]:
    """Start Ray Serve controller.

    The function makes sure controller is ready to start deploying apps
    after it returns.

    Parameters are same as ray.serve._private.api.serve_start().

    Returns: A tuple with controller actor handle and controller name.
    """

    # Initialize ray if needed.
    ray._private.worker.global_worker._filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace=SERVE_NAMESPACE)

    controller_actor_options = {
        "num_cpus": 0,
        "name": SERVE_CONTROLLER_NAME,
        "lifetime": "detached",
        "max_restarts": -1,
        "max_task_retries": -1,
        "resources": {HEAD_NODE_RESOURCE_NAME: 0.001},
        "namespace": SERVE_NAMESPACE,
        "max_concurrency": CONTROLLER_MAX_CONCURRENCY,
    }

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

    if isinstance(grpc_options, dict):
        grpc_options = gRPCOptions(**grpc_options)

    controller = ServeController.options(**controller_actor_options).remote(
        SERVE_CONTROLLER_NAME,
        http_config=http_options,
        grpc_options=grpc_options,
    )

    proxy_handles = ray.get(controller.get_proxies.remote())
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
    return controller, SERVE_CONTROLLER_NAME


async def serve_start_async(
    http_options: Union[None, dict, HTTPOptions] = None,
    grpc_options: Union[None, dict, gRPCOptions] = None,
    **kwargs,
) -> ServeControllerClient:
    """Initialize a serve instance asynchronously.

    This function is not thread-safe. The caller should maintain the async lock in order
    to start the serve instance asynchronously.

    This function has the same functionality as ray.serve._private.api.serve_start().

    Parameters & Returns are same as ray.serve._private.api.serve_start().
    """

    usage_lib.record_library_usage("serve")

    try:
        client = _get_global_client(_health_check_controller=True)
        logger.info(
            f'Connecting to existing Serve app in namespace "{SERVE_NAMESPACE}".'
            " New http options will not be applied."
        )
        if http_options:
            _check_http_options(client, http_options)
        return client
    except RayServeException:
        pass

    controller, controller_name = (
        await ray.remote(_start_controller)
        .options(num_cpus=0)
        .remote(http_options, grpc_options, **kwargs)
    )

    client = ServeControllerClient(
        controller,
        controller_name,
    )
    _set_global_client(client)
    logger.info(f'Started Serve in namespace "{SERVE_NAMESPACE}".')
    return client


def serve_start(
    http_options: Union[None, dict, HTTPOptions] = None,
    grpc_options: Union[None, dict, gRPCOptions] = None,
    **kwargs,
) -> ServeControllerClient:
    """Initialize a serve instance.

    By default, the instance will be scoped to the lifetime of the returned
    Client object (or when the script exits). This is
    only relevant if connecting to a long-running Ray cluster (e.g., with
    ray.init(address="auto") or ray.init("ray://<remote_addr>")).

    Args:
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
        grpc_options: [Experimental] Configuration options for gRPC proxy.
          You can pass in a gRPCOptions object with fields:

            - port(int): Port for gRPC server. Defaults to 9000.
            - grpc_servicer_functions(list): List of import paths for gRPC
                `add_servicer_to_server` functions to add to Serve's gRPC proxy.
                Default empty list, meaning not to start the gRPC server.
    """

    usage_lib.record_library_usage("serve")

    try:
        client = _get_global_client(_health_check_controller=True)
        logger.info(
            f'Connecting to existing Serve app in namespace "{SERVE_NAMESPACE}".'
            " New http options will not be applied."
        )
        if http_options:
            _check_http_options(client, http_options)
        return client
    except RayServeException:
        pass

    controller, controller_name = _start_controller(
        http_options, grpc_options, **kwargs
    )

    client = ServeControllerClient(
        controller,
        controller_name,
    )
    _set_global_client(client)
    logger.info(f'Started Serve in namespace "{SERVE_NAMESPACE}".')
    return client


def call_app_builder_with_args_if_necessary(
    builder: Union[Application, FunctionType],
    args: Dict[str, Any],
) -> Application:
    """Builds a Serve application from an application builder function.

    If a pre-built application is passed, this is a no-op.

    Else, we validate the signature of the builder, convert the args dictionary to
    the user-annotated Pydantic model if provided, and call the builder function.

    The output of the function is returned (must be an Application).
    """
    if isinstance(builder, Application):
        if len(args) > 0:
            raise ValueError(
                "Arguments can only be passed to an application builder function, "
                "not an already built application."
            )
        return builder
    elif not isinstance(builder, FunctionType):
        raise TypeError(
            "Expected a built Serve application or an application builder function "
            f"but got: {type(builder)}."
        )

    # Check that the builder only takes a single argument.
    # TODO(edoakes): we may want to loosen this to allow optional kwargs in the future.
    signature = inspect.signature(builder)
    if len(signature.parameters) != 1:
        raise TypeError(
            "Application builder functions should take exactly one parameter, "
            "a dictionary containing the passed arguments."
        )

    # If the sole argument to the builder is a pydantic model, convert the args dict to
    # that model. This will perform standard pydantic validation (e.g., raise an
    # exception if required fields are missing).
    param = signature.parameters[list(signature.parameters.keys())[0]]
    if inspect.isclass(param.annotation) and issubclass(param.annotation, BaseModel):
        args = param.annotation.parse_obj(args)

    app = builder(args)
    if not isinstance(app, Application):
        raise TypeError(
            "Application builder functions must return an `Application` returned "
            f"`from `Deployment.bind()`, but got: {type(app)}."
        )

    return app
