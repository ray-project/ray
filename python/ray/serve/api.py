import collections
import inspect
import logging
from typing import Any, Callable, Dict, Optional, Tuple, Union, overload

from fastapi import APIRouter, FastAPI
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from starlette.requests import Request
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray import cloudpickle
from ray.dag import DAGNode
from ray.util.annotations import Deprecated, PublicAPI

from ray.serve.built_application import BuiltApplication
from ray.serve._private.client import ServeControllerClient
from ray.serve.config import AutoscalingConfig, DeploymentConfig, HTTPOptions
from ray.serve._private.constants import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    SERVE_DEFAULT_APP_NAME,
    MIGRATION_MESSAGE,
)
from ray.serve.context import (
    ReplicaContext,
    get_global_client,
    get_internal_replica_context,
    _set_global_client,
)
from ray.serve.deployment import Application, Deployment
from ray.serve._private.deployment_graph_build import build as pipeline_build
from ray.serve._private.deployment_graph_build import (
    get_and_validate_ingress_deployment,
)
from ray.serve.exceptions import RayServeException
from ray.serve.handle import RayServeSyncHandle
from ray.serve._private.http_util import ASGIHTTPSender, make_fastapi_class_based_view
from ray.serve._private.logging_utils import LoggingContext
from ray.serve._private.utils import (
    DEFAULT,
    Default,
    ensure_serialization_context,
    in_interactive_shell,
    install_serve_encoders_to_fastapi,
    guarded_deprecation_warning,
    record_serve_tag,
)

from ray.serve._private import api as _private_api

logger = logging.getLogger(__file__)


@guarded_deprecation_warning(instructions=MIGRATION_MESSAGE)
@Deprecated(message=MIGRATION_MESSAGE)
def start(
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

            - host: Host for HTTP servers to listen on. Defaults to
              "127.0.0.1". To expose Serve publicly, you probably want to set
              this to "0.0.0.0".
            - port: Port for HTTP server. Defaults to 8000.
            - root_path: Root path to mount the serve application
              (for example, "/serve"). All deployment routes will be prefixed
              with this path. Defaults to "".
            - middlewares: A list of Starlette middlewares that will be
              applied to the HTTP servers in the cluster. Defaults to [].
            - location(str, serve.config.DeploymentMode): The deployment
              location of HTTP servers:

                - "HeadOnly": start one HTTP server on the head node. Serve
                  assumes the head node is the node you executed serve.start
                  on. This is the default.
                - "EveryNode": start one HTTP server per node.
                - "NoServer" or None: disable HTTP server.
            - num_cpus: The number of CPU cores to reserve for each
              internal Serve HTTP proxy actor.  Defaults to 0.
        dedicated_cpu: Whether to reserve a CPU core for the internal
          Serve controller actor.  Defaults to False.
    """
    client = _private_api.serve_start(detached, http_options, dedicated_cpu, **kwargs)

    # Record after Ray has been started.
    record_extra_usage_tag(TagKey.SERVE_API_VERSION, "v1")

    return client


@PublicAPI(stability="stable")
def shutdown() -> None:
    """Completely shut down the connected Serve instance.

    Shuts down all processes and deletes all state associated with the
    instance.
    """

    try:
        client = get_global_client()
    except RayServeException:
        logger.info(
            "Nothing to shut down. There's no Serve application "
            "running on this Ray cluster."
        )
        return

    client.shutdown()
    _set_global_client(None)


@PublicAPI(stability="beta")
def get_replica_context() -> ReplicaContext:
    """If called from a deployment, returns the deployment and replica tag.

    A replica tag uniquely identifies a single replica for a Ray Serve
    deployment at runtime.  Replica tags are of the form
    `<deployment_name>#<random letters>`.

    Raises:
        RayServeException: if not called from within a Ray Serve deployment.

    Example:
        >>> from ray import serve
        >>> # deployment_name
        >>> serve.get_replica_context().deployment # doctest: +SKIP
        >>> # deployment_name#krcwoa
        >>> serve.get_replica_context().replica_tag # doctest: +SKIP
    """
    internal_replica_context = get_internal_replica_context()
    if internal_replica_context is None:
        raise RayServeException(
            "`serve.get_replica_context()` "
            "may only be called from within a "
            "Ray Serve deployment."
        )
    return internal_replica_context


@PublicAPI(stability="beta")
def ingress(app: Union["FastAPI", "APIRouter", Callable]):
    """Mark an ASGI application ingress for Serve.

    Args:
        app (FastAPI,APIRouter,Starlette,etc): the app or router object serve
            as ingress for this deployment. It can be any ASGI compatible
            object.

    Example:
        >>> from fastapi import FastAPI
        >>> from ray import serve
        >>> app = FastAPI() # doctest: +SKIP
        >>> app = FastAPI() # doctest: +SKIP
        >>> @serve.deployment # doctest: +SKIP
        ... @serve.ingress(app) # doctest: +SKIP
        ... class App: # doctest: +SKIP
        ...     pass # doctest: +SKIP
        >>> App.deploy() # doctest: +SKIP
    """

    def decorator(cls):
        if not inspect.isclass(cls):
            raise ValueError("@serve.ingress must be used with a class.")

        if issubclass(cls, collections.abc.Callable):
            raise ValueError(
                "Class passed to @serve.ingress may not have __call__ method."
            )

        # Sometimes there are decorators on the methods. We want to fix
        # the fast api routes here.
        if isinstance(app, (FastAPI, APIRouter)):
            make_fastapi_class_based_view(app, cls)

        # Free the state of the app so subsequent modification won't affect
        # this ingress deployment. We don't use copy.copy here to avoid
        # recursion issue.
        ensure_serialization_context()
        frozen_app = cloudpickle.loads(cloudpickle.dumps(app))

        class ASGIAppWrapper(cls):
            async def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

                record_serve_tag("SERVE_FASTAPI_USED", "1")
                install_serve_encoders_to_fastapi()

                self._serve_app = frozen_app

                # Use uvicorn's lifespan handling code to properly deal with
                # startup and shutdown event.
                self._serve_asgi_lifespan = LifespanOn(
                    Config(self._serve_app, lifespan="on")
                )
                # Replace uvicorn logger with our own.
                self._serve_asgi_lifespan.logger = logger
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                    self._serve_asgi_lifespan.logger, level=logging.WARNING
                ):
                    await self._serve_asgi_lifespan.startup()

            async def __call__(self, request: Request):
                sender = ASGIHTTPSender()
                await self._serve_app(
                    request.scope,
                    request.receive,
                    sender,
                )
                return sender.build_asgi_response()

            # NOTE: __del__ must be async so that we can run asgi shutdown
            # in the same event loop.
            async def __del__(self):
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                    self._serve_asgi_lifespan.logger, level=logging.WARNING
                ):
                    await self._serve_asgi_lifespan.shutdown()

                # Make sure to call user's del method as well.
                super_cls = super()
                if hasattr(super_cls, "__del__"):
                    super_cls.__del__()

        ASGIAppWrapper.__name__ = cls.__name__
        if hasattr(frozen_app, "docs_url"):
            ASGIAppWrapper.__fastapi_docs_path__ = frozen_app.docs_url
        return ASGIAppWrapper

    return decorator


@overload
def deployment(func_or_class: Callable) -> Deployment:
    pass


@overload
def deployment(
    name: Default[str] = DEFAULT.VALUE,
    version: Default[str] = DEFAULT.VALUE,
    num_replicas: Default[int] = DEFAULT.VALUE,
    init_args: Default[Tuple[Any]] = DEFAULT.VALUE,
    init_kwargs: Default[Dict[Any, Any]] = DEFAULT.VALUE,
    route_prefix: Default[Union[str, None]] = DEFAULT.VALUE,
    ray_actor_options: Default[Dict] = DEFAULT.VALUE,
    user_config: Default[Any] = DEFAULT.VALUE,
    max_concurrent_queries: Default[int] = DEFAULT.VALUE,
    autoscaling_config: Default[Union[Dict, AutoscalingConfig]] = DEFAULT.VALUE,
    graceful_shutdown_wait_loop_s: Default[float] = DEFAULT.VALUE,
    graceful_shutdown_timeout_s: Default[float] = DEFAULT.VALUE,
    health_check_period_s: Default[float] = DEFAULT.VALUE,
    health_check_timeout_s: Default[float] = DEFAULT.VALUE,
    is_driver_deployment: Optional[bool] = DEFAULT.VALUE,
) -> Callable[[Callable], Deployment]:
    pass


@PublicAPI(stability="beta")
def deployment(
    _func_or_class: Optional[Callable] = None,
    name: Default[str] = DEFAULT.VALUE,
    version: Default[str] = DEFAULT.VALUE,
    num_replicas: Default[Optional[int]] = DEFAULT.VALUE,
    init_args: Default[Tuple[Any]] = DEFAULT.VALUE,
    init_kwargs: Default[Dict[Any, Any]] = DEFAULT.VALUE,
    route_prefix: Default[Union[str, None]] = DEFAULT.VALUE,
    ray_actor_options: Default[Dict] = DEFAULT.VALUE,
    user_config: Default[Optional[Any]] = DEFAULT.VALUE,
    max_concurrent_queries: Default[int] = DEFAULT.VALUE,
    autoscaling_config: Default[Union[Dict, AutoscalingConfig, None]] = DEFAULT.VALUE,
    graceful_shutdown_wait_loop_s: Default[float] = DEFAULT.VALUE,
    graceful_shutdown_timeout_s: Default[float] = DEFAULT.VALUE,
    health_check_period_s: Default[float] = DEFAULT.VALUE,
    health_check_timeout_s: Default[float] = DEFAULT.VALUE,
    is_driver_deployment: Optional[bool] = DEFAULT.VALUE,
) -> Callable[[Callable], Deployment]:
    """Define a Serve deployment.

    Args:
        name (Default[str]): Globally-unique name identifying this
            deployment. If not provided, the name of the class or function will
            be used.
        version [DEPRECATED] (Default[str]): Version of the deployment.
            This is used to indicate a code change for the deployment; when it
            is re-deployed with a version change, a rolling update of the
            replicas will be performed. If not provided, every deployment will
            be treated as a new version.
        num_replicas (Default[Optional[int]]): The number of processes to start up that
            will handle requests to this deployment. Defaults to 1.
        init_args (Default[Tuple[Any]]): Positional args to be passed to the
            class constructor when starting up deployment replicas. These can
            also be passed when you call `.deploy()` on the returned Deployment.
        init_kwargs (Default[Dict[Any, Any]]): Keyword args to be passed to the
            class constructor when starting up deployment replicas. These can
            also be passed when you call `.deploy()` on the returned Deployment.
        route_prefix (Default[Union[str, None]]): Requests to paths under this
            HTTP path prefix will be routed to this deployment. Defaults to
            '/{name}'. When set to 'None', no HTTP endpoint will be created.
            Routing is done based on longest-prefix match, so if you have
            deployment A with a prefix of '/a' and deployment B with a prefix
            of '/a/b', requests to '/a', '/a/', and '/a/c' go to A and requests
            to '/a/b', '/a/b/', and '/a/b/c' go to B. Routes must not end with
            a '/' unless they're the root (just '/'), which acts as a
            catch-all.
        ray_actor_options (Default[Dict]): Options to be passed to the Ray
            actor constructor such as resource requirements. Valid options are
            `accelerator_type`, `memory`, `num_cpus`, `num_gpus`,
            `object_store_memory`, `resources`, and `runtime_env`.
        user_config (Default[Optional[Any]]): Config to pass to the
            reconfigure method of the deployment. This can be updated
            dynamically without changing the version of the deployment and
            restarting its replicas. The user_config must be json-serializable
            to keep track of updates, so it must only contain json-serializable
            types, or json-serializable types nested in lists and dictionaries.
        max_concurrent_queries (Default[int]): The maximum number of queries
            that will be sent to a replica of this deployment without receiving
            a response. Defaults to 100.
        is_driver_deployment (Optional[bool]): [Experiment] when set it as True, serve
            will deploy exact one deployment to every node.

    Example:
    >>> from ray import serve
    >>> @serve.deployment(name="deployment1") # doctest: +SKIP
    ... class MyDeployment: # doctest: +SKIP
    ...     pass # doctest: +SKIP

    >>> MyDeployment.bind(*init_args) # doctest: +SKIP
    >>> MyDeployment.options( # doctest: +SKIP
    ...     num_replicas=2, init_args=init_args).bind()

    Returns:
        Deployment
    """

    # NOTE: The user_configured_option_names should be the first thing that's
    # defined in this function. It depends on the locals() dictionary storing
    # only the function args/kwargs.
    # Create list of all user-configured options from keyword args
    user_configured_option_names = [
        option
        for option, value in locals().items()
        if option != "_func_or_class" and value is not DEFAULT.VALUE
    ]

    # Num of replicas should not be 0.
    # TODO(Sihan) seperate num_replicas attribute from internal and api
    if num_replicas == 0:
        raise ValueError("num_replicas is expected to larger than 0")

    if num_replicas not in [DEFAULT.VALUE, None] and autoscaling_config not in [
        DEFAULT.VALUE,
        None,
    ]:
        raise ValueError(
            "Manually setting num_replicas is not allowed when "
            "autoscaling_config is provided."
        )

    if version is not DEFAULT.VALUE:
        logger.warning(
            "DeprecationWarning: `version` in `@serve.deployment` has been deprecated. "
            "Explicitly specifying version will raise an error in the future!"
        )

    if is_driver_deployment is DEFAULT.VALUE:
        is_driver_deployment = False

    config = DeploymentConfig.from_default(
        num_replicas=num_replicas if num_replicas is not None else 1,
        user_config=user_config,
        max_concurrent_queries=max_concurrent_queries,
        autoscaling_config=autoscaling_config,
        graceful_shutdown_wait_loop_s=graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=graceful_shutdown_timeout_s,
        health_check_period_s=health_check_period_s,
        health_check_timeout_s=health_check_timeout_s,
    )
    config.user_configured_option_names = set(user_configured_option_names)

    def decorator(_func_or_class):
        return Deployment(
            _func_or_class,
            name if name is not DEFAULT.VALUE else _func_or_class.__name__,
            config,
            version=(version if version is not DEFAULT.VALUE else None),
            init_args=(init_args if init_args is not DEFAULT.VALUE else None),
            init_kwargs=(init_kwargs if init_kwargs is not DEFAULT.VALUE else None),
            route_prefix=route_prefix,
            ray_actor_options=(
                ray_actor_options if ray_actor_options is not DEFAULT.VALUE else None
            ),
            is_driver_deployment=is_driver_deployment,
            _internal=True,
        )

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator


@guarded_deprecation_warning(instructions=MIGRATION_MESSAGE)
@Deprecated(message=MIGRATION_MESSAGE)
def get_deployment(name: str) -> Deployment:
    """Dynamically fetch a handle to a Deployment object.

    This can be used to update and redeploy a deployment without access to
    the original definition.

    Example:
    >>> from ray import serve
    >>> MyDeployment = serve.get_deployment("name")  # doctest: +SKIP
    >>> MyDeployment.options(num_replicas=10).deploy()  # doctest: +SKIP

    Args:
        name: name of the deployment. This must have already been
        deployed.

    Returns:
        Deployment
    """
    record_extra_usage_tag(TagKey.SERVE_API_VERSION, "v1")
    return _private_api.get_deployment(name)


@guarded_deprecation_warning(instructions=MIGRATION_MESSAGE)
@Deprecated(message=MIGRATION_MESSAGE)
def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    record_extra_usage_tag(TagKey.SERVE_API_VERSION, "v1")
    return _private_api.list_deployments()


@PublicAPI(stability="beta")
def run(
    target: Union[Application, BuiltApplication],
    _blocking: bool = True,
    host: str = DEFAULT_HTTP_HOST,
    port: int = DEFAULT_HTTP_PORT,
    name: str = SERVE_DEFAULT_APP_NAME,
    route_prefix: str = DEFAULT.VALUE,
) -> Optional[RayServeSyncHandle]:
    """Run an application and return a handle to its ingress deployment.

    The application is returned by `Deployment.bind()` or `serve.build`.

    Args:
        target (Union[Application, BuiltApplication]):
            A Serve application returned from `Deployment.bind()` or a built application
            returned from `serve.build()`.
        host: Host for HTTP servers to listen on. Defaults to
            "127.0.0.1". To expose Serve publicly, you probably want to set
            this to "0.0.0.0".
        port: Port for HTTP server. Defaults to 8000.
        name: Application name. If not provided, this will be the only
            application running on the cluster (it will delete all others).
        route_prefix: Route prefix for HTTP requests. If not provided, it will use
            route_prefix of the ingress deployment. If specified neither as an argument
            nor in the ingress deployment, the route prefix will default to '/'.
    """
    client = _private_api.serve_start(
        detached=True,
        http_options={"host": host, "port": port, "location": "EveryNode"},
    )

    # Record after Ray has been started.
    record_extra_usage_tag(TagKey.SERVE_API_VERSION, "v2")

    if isinstance(target, Application):
        deployments = pipeline_build(target._get_internal_dag_node(), name)
        ingress = get_and_validate_ingress_deployment(deployments)
    elif isinstance(target, BuiltApplication):
        deployments = list(target.deployments.values())
        ingress = target.ingress
    else:
        msg = (
            "`serve.run` expects an `Application` returned by `Deployment.bind()` "
            "or a static `BuiltApplication` returned by `serve.build`."
        )
        if isinstance(target, DAGNode):
            msg += " If you are using the DAG API, you must bind the DAG node to a "
            "deployment like: `app = Deployment.bind(my_dag_output)`. "
        raise TypeError(msg)

    # when name provided, keep all existing applications
    # otherwise, delete all of them.
    remove_past_deployments = True
    if name:
        remove_past_deployments = False

    parameter_group = []

    for deployment in deployments:
        # Overwrite route prefix
        if route_prefix is not DEFAULT.VALUE and deployment._route_prefix is not None:
            deployment._route_prefix = route_prefix
        deployment_parameters = {
            "name": deployment._name,
            "func_or_class": deployment._func_or_class,
            "init_args": deployment.init_args,
            "init_kwargs": deployment.init_kwargs,
            "ray_actor_options": deployment._ray_actor_options,
            "config": deployment._config,
            "version": deployment._version,
            "route_prefix": deployment.route_prefix,
            "url": deployment.url,
            "is_driver_deployment": deployment._is_driver_deployment,
            "docs_path": deployment._docs_path,
        }
        parameter_group.append(deployment_parameters)
    client.deploy_application(
        name,
        parameter_group,
        _blocking=_blocking,
        remove_past_deployments=remove_past_deployments,
    )

    if ingress is not None:
        return ingress._get_handle()


@PublicAPI(stability="alpha")
def build(target: Application, name: str = None) -> BuiltApplication:
    """Builds a Serve application into a static, built application.

    Resolves the provided Application object into a list of deployments.
    This can be converted to a Serve config file that can be deployed via
    the Serve REST API or CLI.

    All of the deployments must be importable. That is, they cannot be
    defined in __main__ or inline defined. The deployments will be
    imported in the config file using the same import path they were here.

    Args:
        target: The Serve application to run consisting of one or more
            deployments.
        name: The name of the Serve application. When name is not provided, the
        deployment name won't be updated. (SINGLE_APP use case.)

    Returns:
        The static built Serve application.
    """
    if in_interactive_shell():
        raise RuntimeError(
            "build cannot be called from an interactive shell like "
            "IPython or Jupyter because it requires all deployments to be "
            "importable to run the app after building."
        )

    # TODO(edoakes): this should accept host and port, but we don't
    # currently support them in the REST API.
    return BuiltApplication(pipeline_build(target._get_internal_dag_node(), name))


@PublicAPI(stability="alpha")
def delete(name: str, _blocking: bool = True):
    """Delete an app by its name

    Deletes the app with all corresponding deployments.

    Args:
        name: the name of app to delete.
    """
    client = get_global_client()
    client.delete_apps([name], blocking=_blocking)
