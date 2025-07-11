import collections
import inspect
import logging
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Sequence, Type, Union

from attr import dataclass
from fastapi import APIRouter, FastAPI
from starlette.types import ASGIApp

import ray
from ray import cloudpickle
from ray._private.serialization import pickle_dumps
from ray.serve._private.build_app import build_app
from ray.serve._private.config import (
    DeploymentConfig,
    ReplicaConfig,
    handle_num_replicas_auto,
)
from ray.serve._private.constants import (
    RAY_SERVE_FORCE_LOCAL_TESTING_MODE,
    SERVE_DEFAULT_APP_NAME,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.http_util import (
    ASGIAppReplicaWrapper,
    make_fastapi_class_based_view,
)
from ray.serve._private.local_testing_mode import make_local_deployment_handle
from ray.serve._private.logging_utils import configure_component_logger
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    Default,
    ensure_serialization_context,
    extract_self_if_method_call,
    validate_route_prefix,
    wait_for_interrupt,
)
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentMode,
    HTTPOptions,
    ProxyLocation,
    RequestRouterConfig,
    gRPCOptions,
)
from ray.serve.context import (
    ReplicaContext,
    _get_global_client,
    _get_internal_replica_context,
    _set_global_client,
)
from ray.serve.deployment import Application, Deployment
from ray.serve.exceptions import RayServeException
from ray.serve.handle import DeploymentHandle
from ray.serve.multiplex import _ModelMultiplexWrapper
from ray.serve.schema import LoggingConfig, ServeInstanceDetails, ServeStatus
from ray.util.annotations import DeveloperAPI, PublicAPI

from ray.serve._private import api as _private_api  # isort:skip


logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="stable")
def start(
    proxy_location: Union[None, str, ProxyLocation] = None,
    http_options: Union[None, dict, HTTPOptions] = None,
    grpc_options: Union[None, dict, gRPCOptions] = None,
    logging_config: Union[None, dict, LoggingConfig] = None,
    **kwargs,
):
    """Start Serve on the cluster.

    Used to set cluster-scoped configurations such as HTTP options. In most cases, this
    does not need to be called manually and Serve will be started when an application is
    first deployed to the cluster.

    These cluster-scoped options cannot be updated dynamically. To update them, start a
    new cluster or shut down Serve on the cluster and start it again.

    These options can also be set in the config file deployed via REST API.

    Args:
        proxy_location: Where to run proxies that handle ingress traffic to the
          cluster (defaults to every node in the cluster with at least one replica on
          it). See `ProxyLocation` for supported options.
        http_options: HTTP config options for the proxies. These can be passed as an
          unstructured dictionary or the structured `HTTPOptions` class. See
          `HTTPOptions` for supported options.
        grpc_options: [EXPERIMENTAL] gRPC config options for the proxies. These can
          be passed as an unstructured dictionary or the structured `gRPCOptions`
          class See `gRPCOptions` for supported options.
        logging_config: logging config options for the serve component (
            controller & proxy).
    """
    if proxy_location is None:
        if http_options is None:
            http_options = HTTPOptions(location=DeploymentMode.EveryNode)
    else:
        if http_options is None:
            http_options = HTTPOptions()
        elif isinstance(http_options, dict):
            http_options = HTTPOptions(**http_options)

        if isinstance(proxy_location, str):
            proxy_location = ProxyLocation(proxy_location)

        http_options.location = ProxyLocation._to_deployment_mode(proxy_location)

    _private_api.serve_start(
        http_options=http_options,
        grpc_options=grpc_options,
        global_logging_config=logging_config,
        **kwargs,
    )


@PublicAPI(stability="stable")
def shutdown():
    """Completely shut down Serve on the cluster.

    Deletes all applications and shuts down Serve system actors.
    """

    try:
        client = _get_global_client()
    except RayServeException:
        logger.info(
            "Nothing to shut down. There's no Serve application "
            "running on this Ray cluster."
        )
        return

    client.shutdown()
    _set_global_client(None)


@DeveloperAPI
def get_replica_context() -> ReplicaContext:
    """Returns the deployment and replica tag from within a replica at runtime.

    A replica tag uniquely identifies a single replica for a Ray Serve
    deployment.

    Raises:
        RayServeException: if not called from within a Ray Serve deployment.

    Example:

        .. code-block:: python

            from ray import serve
            @serve.deployment
            class MyDeployment:
                def __init__(self):
                    # Prints "MyDeployment"
                    print(serve.get_replica_context().deployment)

    """
    internal_replica_context = _get_internal_replica_context()
    if internal_replica_context is None:
        raise RayServeException(
            "`serve.get_replica_context()` "
            "may only be called from within a "
            "Ray Serve deployment."
        )
    return internal_replica_context


@PublicAPI(stability="stable")
def ingress(app: Union[ASGIApp, Callable]) -> Callable:
    """Wrap a deployment class with an ASGI application for HTTP request parsing.
    There are a few different ways to use this functionality.

    Example:

    FastAPI app routes are defined inside the deployment class.

        .. code-block:: python

            from ray import serve
            from fastapi import FastAPI

            app = FastAPI()

            @serve.deployment
            @serve.ingress(app)
            class MyFastAPIDeployment:
                @app.get("/hi")
                def say_hi(self) -> str:
                    return "Hello world!"

            app = MyFastAPIDeployment.bind()

    You can also use a standalone FastAPI app without registering
    routes inside the deployment.

    .. code-block:: python

        from ray import serve
        from fastapi import FastAPI

        app = FastAPI()

        @app.get("/hi")
        def say_hi():
            return "Hello world!"

        deployment = serve.deployment(serve.ingress(app)())
        app = deployment.bind()

    You can also pass in a builder function that returns an ASGI app.
    The builder function is evaluated when the deployment is initialized on
    replicas. This example shows how to use a sub-deployment inside the routes
    defined outside the deployment class.

    .. code-block:: python

        from ray import serve

        @serve.deployment
        class SubDeployment:
            def __call__(self):
                return "Hello world!"

        def build_asgi_app():
            from fastapi import FastAPI

            app = FastAPI()

            def get_sub_deployment_handle():
                return serve.get_deployment_handle(SubDeployment.name, app_name="my_app")

            @app.get("/hi")
            async def say_hi(handle: Depends(get_sub_deployment_handle)):
                return await handle.remote()

            return app

        deployment = serve.deployment(serve.ingress(build_asgi_app)())
        app = deployment.bind(SubDeployment.bind(), name="my_app", route_prefix="/")

    Args:
        app: the FastAPI app to wrap this class with.
            Can be any ASGI-compatible callable.
            You can also pass in a builder function that returns an ASGI app.
    """

    def decorator(cls: Optional[Type[Any]] = None) -> Callable:
        if cls is None:

            class ASGIIngressDeployment:
                def __init__(self, *args, **kwargs):
                    self.args = args
                    self.kwargs = kwargs

            cls = ASGIIngressDeployment

        if not inspect.isclass(cls):
            raise ValueError("@serve.ingress must be used with a class.")
        if issubclass(cls, collections.abc.Callable):
            raise ValueError(
                "Classes passed to @serve.ingress may not have __call__ method."
            )

        # Sometimes there are decorators on the methods. We want to fix
        # the fast api routes here.
        if isinstance(app, (FastAPI, APIRouter)):
            make_fastapi_class_based_view(app, cls)

        frozen_app_or_func: Union[ASGIApp, Callable] = None

        if inspect.isfunction(app):
            frozen_app_or_func = app
        else:
            # Free the state of the app so subsequent modification won't affect
            # this ingress deployment. We don't use copy.copy here to avoid
            # recursion issue.
            ensure_serialization_context()
            frozen_app_or_func = cloudpickle.loads(
                pickle_dumps(app, error_msg="Failed to serialize the ASGI app.")
            )

        class ASGIIngressWrapper(cls, ASGIAppReplicaWrapper):
            def __init__(self, *args, **kwargs):
                # Call user-defined constructor.
                cls.__init__(self, *args, **kwargs)

                ServeUsageTag.FASTAPI_USED.record("1")
                ASGIAppReplicaWrapper.__init__(self, frozen_app_or_func)

            async def __del__(self):
                await ASGIAppReplicaWrapper.__del__(self)

                # Call user-defined destructor if defined.
                if hasattr(cls, "__del__"):
                    if inspect.iscoroutinefunction(cls.__del__):
                        await cls.__del__(self)
                    else:
                        cls.__del__(self)

        ASGIIngressWrapper.__name__ = cls.__name__

        return ASGIIngressWrapper

    return decorator


@PublicAPI(stability="stable")
def deployment(
    _func_or_class: Optional[Callable] = None,
    name: Default[str] = DEFAULT.VALUE,
    version: Default[str] = DEFAULT.VALUE,
    num_replicas: Default[Optional[Union[int, str]]] = DEFAULT.VALUE,
    route_prefix: Default[Union[str, None]] = DEFAULT.VALUE,
    ray_actor_options: Default[Dict] = DEFAULT.VALUE,
    placement_group_bundles: Default[List[Dict[str, float]]] = DEFAULT.VALUE,
    placement_group_strategy: Default[str] = DEFAULT.VALUE,
    max_replicas_per_node: Default[int] = DEFAULT.VALUE,
    user_config: Default[Optional[Any]] = DEFAULT.VALUE,
    max_ongoing_requests: Default[int] = DEFAULT.VALUE,
    max_queued_requests: Default[int] = DEFAULT.VALUE,
    autoscaling_config: Default[Union[Dict, AutoscalingConfig, None]] = DEFAULT.VALUE,
    graceful_shutdown_wait_loop_s: Default[float] = DEFAULT.VALUE,
    graceful_shutdown_timeout_s: Default[float] = DEFAULT.VALUE,
    health_check_period_s: Default[float] = DEFAULT.VALUE,
    health_check_timeout_s: Default[float] = DEFAULT.VALUE,
    logging_config: Default[Union[Dict, LoggingConfig, None]] = DEFAULT.VALUE,
    request_router_config: Default[
        Union[Dict, RequestRouterConfig, None]
    ] = DEFAULT.VALUE,
) -> Callable[[Callable], Deployment]:
    """Decorator that converts a Python class to a `Deployment`.

    Example:

    .. code-block:: python

        from ray import serve

        @serve.deployment(num_replicas=2)
        class MyDeployment:
            pass

        app = MyDeployment.bind()

    Args:
        _func_or_class: The class or function to be decorated.
        name: Name uniquely identifying this deployment within the application.
            If not provided, the name of the class or function is used.
        version: Version of the deployment. Deprecated.
        num_replicas: Number of replicas to run that handle requests to
            this deployment. Defaults to 1.
        route_prefix: Route prefix for HTTP requests. Defaults to '/'. Deprecated.
        ray_actor_options: Options to pass to the Ray Actor decorator, such as
            resource requirements. Valid options are: `accelerator_type`, `memory`,
            `num_cpus`, `num_gpus`, `resources`, and `runtime_env`.
        placement_group_bundles: Defines a set of placement group bundles to be
            scheduled *for each replica* of this deployment. The replica actor will
            be scheduled in the first bundle provided, so the resources specified in
            `ray_actor_options` must be a subset of the first bundle's resources. All
            actors and tasks created by the replica actor will be scheduled in the
            placement group by default (`placement_group_capture_child_tasks` is set
            to True).
            This cannot be set together with max_replicas_per_node.
        placement_group_strategy: Strategy to use for the replica placement group
            specified via `placement_group_bundles`. Defaults to `PACK`.
        max_replicas_per_node: The max number of replicas of this deployment that can
            run on a single node. Valid values are None (default, no limit)
            or an integer in the range of [1, 100].
            This cannot be set together with placement_group_bundles.
        user_config: Config to pass to the reconfigure method of the deployment. This
            can be updated dynamically without restarting the replicas of the
            deployment. The user_config must be fully JSON-serializable.
        max_ongoing_requests: Maximum number of requests that are sent to a
            replica of this deployment without receiving a response. Defaults to 5.
        max_queued_requests: [EXPERIMENTAL] Maximum number of requests to this
            deployment that will be queued at each *caller* (proxy or DeploymentHandle).
            Once this limit is reached, subsequent requests will raise a
            BackPressureError (for handles) or return an HTTP 503 status code (for HTTP
            requests). Defaults to -1 (no limit).
        autoscaling_config: Parameters to configure autoscaling behavior. If this
            is set, `num_replicas` should be "auto" or not set.
        graceful_shutdown_wait_loop_s: Duration that replicas wait until there is
            no more work to be done before shutting down. Defaults to 2s.
        graceful_shutdown_timeout_s: Duration to wait for a replica to gracefully
            shut down before being forcefully killed. Defaults to 20s.
        health_check_period_s: Duration between health check calls for the replica.
            Defaults to 10s. The health check is by default a no-op Actor call to the
            replica, but you can define your own health check using the "check_health"
            method in your deployment that raises an exception when unhealthy.
        health_check_timeout_s: Duration in seconds, that replicas wait for a health
            check method to return before considering it as failed. Defaults to 30s.
        logging_config: Logging config options for the deployment. If provided,
            the config will be used to set up the Serve logger on the deployment.
        request_router_config: Config for the request router used for this deployment.
    Returns:
        `Deployment`
    """
    if route_prefix is not DEFAULT.VALUE:
        raise ValueError(
            "`route_prefix` can no longer be specified at the deployment level. "
            "Pass it to `serve.run` or in the application config instead."
        )

    if max_ongoing_requests is None:
        raise ValueError("`max_ongoing_requests` must be non-null, got None.")

    if num_replicas == "auto":
        num_replicas = None
        max_ongoing_requests, autoscaling_config = handle_num_replicas_auto(
            max_ongoing_requests, autoscaling_config
        )

        ServeUsageTag.AUTO_NUM_REPLICAS_USED.record("1")

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
    # TODO(Sihan) separate num_replicas attribute from internal and api
    if num_replicas == 0:
        raise ValueError("num_replicas is expected to larger than 0")

    if num_replicas not in [DEFAULT.VALUE, None, "auto"] and autoscaling_config not in [
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

    if isinstance(logging_config, LoggingConfig):
        logging_config = logging_config.dict()

    deployment_config = DeploymentConfig.from_default(
        num_replicas=num_replicas if num_replicas is not None else 1,
        user_config=user_config,
        max_ongoing_requests=max_ongoing_requests,
        max_queued_requests=max_queued_requests,
        autoscaling_config=autoscaling_config,
        graceful_shutdown_wait_loop_s=graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=graceful_shutdown_timeout_s,
        health_check_period_s=health_check_period_s,
        health_check_timeout_s=health_check_timeout_s,
        logging_config=logging_config,
        request_router_config=request_router_config,
    )
    deployment_config.user_configured_option_names = set(user_configured_option_names)

    def decorator(_func_or_class):
        replica_config = ReplicaConfig.create(
            _func_or_class,
            init_args=None,
            init_kwargs=None,
            ray_actor_options=(
                ray_actor_options if ray_actor_options is not DEFAULT.VALUE else None
            ),
            placement_group_bundles=(
                placement_group_bundles
                if placement_group_bundles is not DEFAULT.VALUE
                else None
            ),
            placement_group_strategy=(
                placement_group_strategy
                if placement_group_strategy is not DEFAULT.VALUE
                else None
            ),
            max_replicas_per_node=(
                max_replicas_per_node
                if max_replicas_per_node is not DEFAULT.VALUE
                else None
            ),
        )

        return Deployment(
            name if name is not DEFAULT.VALUE else _func_or_class.__name__,
            deployment_config,
            replica_config,
            version=(version if version is not DEFAULT.VALUE else None),
            _internal=True,
        )

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator


@DeveloperAPI
@dataclass(frozen=True)
class RunTarget:
    """Represents a Serve application to run for `serve.run_many`."""

    target: Application
    name: str = SERVE_DEFAULT_APP_NAME
    route_prefix: Optional[str] = "/"
    logging_config: Optional[Union[Dict, LoggingConfig]] = None


@DeveloperAPI
def _run_many(
    targets: Sequence[RunTarget],
    wait_for_ingress_deployment_creation: bool = True,
    wait_for_applications_running: bool = True,
    _local_testing_mode: bool = False,
) -> List[DeploymentHandle]:
    """Run many applications and return the handles to their ingress deployments.

    This is only used internally with the _blocking not totally blocking the following
    code indefinitely until Ctrl-C'd.
    """
    if not targets:
        raise ValueError("No applications provided.")

    if RAY_SERVE_FORCE_LOCAL_TESTING_MODE:
        if not _local_testing_mode:
            logger.info("Overriding local_testing_mode=True from environment variable.")

        _local_testing_mode = True

    built_apps = []
    for t in targets:
        if len(t.name) == 0:
            raise RayServeException("Application name must a non-empty string.")

        if not isinstance(t.target, Application):
            raise TypeError(
                "`serve.run` expects an `Application` returned by `Deployment.bind()`."
            )

        validate_route_prefix(t.route_prefix)

        built_apps.append(
            build_app(
                t.target,
                name=t.name,
                route_prefix=t.route_prefix,
                logging_config=t.logging_config,
                make_deployment_handle=make_local_deployment_handle
                if _local_testing_mode
                else None,
                default_runtime_env=ray.get_runtime_context().runtime_env
                if not _local_testing_mode
                else None,
            )
        )

    if _local_testing_mode:
        # implicitly use the last target's logging config (if provided) in local testing mode
        logging_config = t.logging_config or LoggingConfig()
        if not isinstance(logging_config, LoggingConfig):
            logging_config = LoggingConfig(**(logging_config or {}))

        configure_component_logger(
            component_name="local_test",
            component_id="-",
            logging_config=logging_config,
            stream_handler_only=True,
        )
        return [b.deployment_handles[b.ingress_deployment_name] for b in built_apps]
    else:
        client = _private_api.serve_start(
            http_options={"location": "EveryNode"},
            global_logging_config=None,
        )

        # Record after Ray has been started.
        ServeUsageTag.API_VERSION.record("v2")

        return client.deploy_applications(
            built_apps,
            wait_for_ingress_deployment_creation=wait_for_ingress_deployment_creation,
            wait_for_applications_running=wait_for_applications_running,
        )


@PublicAPI(stability="stable")
def _run(
    target: Application,
    *,
    _blocking: bool = True,
    name: str = SERVE_DEFAULT_APP_NAME,
    route_prefix: Optional[str] = "/",
    logging_config: Optional[Union[Dict, LoggingConfig]] = None,
    _local_testing_mode: bool = False,
) -> DeploymentHandle:
    """Run an application and return a handle to its ingress deployment.

    This is only used internally with the _blocking not totally blocking the following
    code indefinitely until Ctrl-C'd.
    """
    return _run_many(
        [
            RunTarget(
                target=target,
                name=name,
                route_prefix=route_prefix,
                logging_config=logging_config,
            )
        ],
        wait_for_applications_running=_blocking,
        _local_testing_mode=_local_testing_mode,
    )[0]


@DeveloperAPI
def run_many(
    targets: Sequence[RunTarget],
    blocking: bool = False,
    wait_for_ingress_deployment_creation: bool = True,
    wait_for_applications_running: bool = True,
    _local_testing_mode: bool = False,
) -> List[DeploymentHandle]:
    """Run many applications and return the handles to their ingress deployments.

    Args:
        targets:
            A sequence of `RunTarget`,
            each containing information about an application to deploy.
        blocking: Whether this call should be blocking. If True, it
            will loop and log status until Ctrl-C'd.
        wait_for_ingress_deployment_creation: Whether to wait for the ingress
            deployments to be created.
        wait_for_applications_running: Whether to wait for the applications to be
            running. Note that this effectively implies
            `wait_for_ingress_deployment_creation=True`,
            because the ingress deployments must be created
            before the applications can be running.

    Returns:
        List[DeploymentHandle]: A list of handles that can be used
            to call the applications.
    """
    handles = _run_many(
        targets,
        wait_for_ingress_deployment_creation=wait_for_ingress_deployment_creation,
        wait_for_applications_running=wait_for_applications_running,
        _local_testing_mode=_local_testing_mode,
    )

    if blocking:
        wait_for_interrupt()

    return handles


@PublicAPI(stability="stable")
def run(
    target: Application,
    blocking: bool = False,
    name: str = SERVE_DEFAULT_APP_NAME,
    route_prefix: Optional[str] = "/",
    logging_config: Optional[Union[Dict, LoggingConfig]] = None,
    _local_testing_mode: bool = False,
) -> DeploymentHandle:
    """Run an application and return a handle to its ingress deployment.

    The application is returned by `Deployment.bind()`. Example:

    .. code-block:: python

        handle = serve.run(MyDeployment.bind())
        ray.get(handle.remote())

    Args:
        target:
            A Serve application returned by `Deployment.bind()`.
        blocking: Whether this call should be blocking. If True, it
            will loop and log status until Ctrl-C'd.
        name: Application name. If not provided, this will be the only
            application running on the cluster (it will delete all others).
        route_prefix: Route prefix for HTTP requests. Defaults to '/'.
            If `None` is passed, the application will not be exposed over HTTP
            (this may be useful if you only want the application to be exposed via
            gRPC or a `DeploymentHandle`).
        logging_config: Application logging config. If provided, the config will
            be applied to all deployments which doesn't have logging config.

    Returns:
        DeploymentHandle: A handle that can be used to call the application.
    """
    handle = _run(
        target=target,
        name=name,
        route_prefix=route_prefix,
        logging_config=logging_config,
        _local_testing_mode=_local_testing_mode,
    )

    if blocking:
        wait_for_interrupt()

    return handle


@PublicAPI(stability="stable")
def delete(name: str, _blocking: bool = True):
    """Delete an application by its name.

    Deletes the app with all corresponding deployments.
    """
    client = _get_global_client()
    client.delete_apps([name], blocking=_blocking)


@PublicAPI(stability="beta")
def multiplexed(
    func: Optional[Callable[..., Any]] = None, max_num_models_per_replica: int = 3
):
    """Wrap a callable or method used to load multiplexed models in a replica.

    The function can be standalone function or a method of a class. The
    function must have exactly one argument, the model id of type `str` for the
    model to be loaded.

    It is required to define the function with `async def` and the function must be
    an async function. It is recommended to define coroutines for long running
    IO tasks in the function to avoid blocking the event loop.

    The multiplexed function is called to load a model with the given model ID when
    necessary.

    When the number of models in one replica is larger than max_num_models_per_replica,
    the models will be unloaded using an LRU policy.

    If you want to release resources after the model is loaded, you can define
    a `__del__` method in your model class. The `__del__` method will be called when
    the model is unloaded.

    Example:

    .. code-block:: python

            from ray import serve

            @serve.deployment
            class MultiplexedDeployment:

                def __init__(self):
                    # Define s3 base path to load models.
                    self.s3_base_path = "s3://my_bucket/my_models"

                @serve.multiplexed(max_num_models_per_replica=5)
                async def load_model(self, model_id: str) -> Any:
                    # Load model with the given tag
                    # You can use any model loading library here
                    # and return the loaded model. load_from_s3 is
                    # a placeholder function.
                    return load_from_s3(model_id)

                async def __call__(self, request):
                    # Get the model_id from the request context.
                    model_id = serve.get_multiplexed_model_id()
                    # Load the model for the requested model_id.
                    # If the model is already cached locally,
                    # this will just be a dictionary lookup.
                    model = await self.load_model(model_id)
                    return model(request)


    Args:
        max_num_models_per_replica: the maximum number of models
            to be loaded on each replica. By default, it is 3, which
            means that each replica can cache up to 3 models. You can
            set it to a larger number if you have enough memory on
            the node resource, in opposite, you can set it to a smaller
            number if you want to save memory on the node resource.
    """

    if func is not None:
        if not callable(func):
            raise TypeError(
                "The `multiplexed` decorator must be used with a function or method."
            )

        # TODO(Sihan): Make the API accept the sync function as well.
        # https://github.com/ray-project/ray/issues/35356
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                "@serve.multiplexed can only be used to decorate async "
                "functions or methods."
            )
        signature = inspect.signature(func)
        if len(signature.parameters) == 0 or len(signature.parameters) > 2:
            raise TypeError(
                "@serve.multiplexed can only be used to decorate functions or methods "
                "with at least one 'model_id: str' argument."
            )

    if not isinstance(max_num_models_per_replica, int):
        raise TypeError("max_num_models_per_replica must be an integer.")

    if max_num_models_per_replica != -1 and max_num_models_per_replica <= 0:
        raise ValueError("max_num_models_per_replica must be positive.")

    def _multiplex_decorator(func: Callable):
        @wraps(func)
        async def _multiplex_wrapper(*args):
            args_check_error_msg = (
                "Functions decorated with `@serve.multiplexed` must take exactly one"
                "the multiplexed model ID (str), but got {}"
            )
            if not args:
                raise TypeError(
                    args_check_error_msg.format("no arguments are provided.")
                )
            self = extract_self_if_method_call(args, func)

            # User defined multiplexed function can be a standalone function or a
            # method of a class. If it is a method of a class, the first argument
            # is self.
            if self is None:
                if len(args) != 1:
                    raise TypeError(
                        args_check_error_msg.format("more than one arguments.")
                    )
                multiplex_object = func
                model_id = args[0]
            else:
                # count self as an argument
                if len(args) != 2:
                    raise TypeError(
                        args_check_error_msg.format("more than one arguments.")
                    )
                multiplex_object = self
                model_id = args[1]
            multiplex_attr = "__serve_multiplex_wrapper"
            # If the multiplexed function is called for the first time,
            # create a model multiplex wrapper and cache it in the multiplex object.
            if not hasattr(multiplex_object, multiplex_attr):
                model_multiplex_wrapper = _ModelMultiplexWrapper(
                    func, self, max_num_models_per_replica
                )
                setattr(multiplex_object, multiplex_attr, model_multiplex_wrapper)
            else:
                model_multiplex_wrapper = getattr(multiplex_object, multiplex_attr)
            return await model_multiplex_wrapper.load_model(model_id)

        return _multiplex_wrapper

    return _multiplex_decorator(func) if callable(func) else _multiplex_decorator


@PublicAPI(stability="beta")
def get_multiplexed_model_id() -> str:
    """Get the multiplexed model ID for the current request.

    This is used with a function decorated with `@serve.multiplexed`
    to retrieve the model ID for the current request.

    .. code-block:: python

            import ray
            from ray import serve
            import requests

            # Set the multiplexed model id with the key
            # "ray_serve_multiplexed_model_id" in the request
            # headers when sending requests to the http proxy.
            requests.get("http://localhost:8000",
                headers={"ray_serve_multiplexed_model_id": "model_1"})

            # This can also be set when using `DeploymentHandle`.
            handle.options(multiplexed_model_id="model_1").remote("blablabla")

            # In your deployment code, you can retrieve the model id from
            # `get_multiplexed_model_id()`.
            @serve.deployment
            def my_deployment_function(request):
                assert serve.get_multiplexed_model_id() == "model_1"
    """
    _request_context = ray.serve.context._get_serve_request_context()
    return _request_context.multiplexed_model_id


@PublicAPI(stability="alpha")
def status() -> ServeStatus:
    """Get the status of Serve on the cluster.

    Includes status of all HTTP Proxies, all active applications, and
    their deployments.

    .. code-block:: python

            @serve.deployment(num_replicas=2)
            class MyDeployment:
                pass

            serve.run(MyDeployment.bind())
            status = serve.status()
            assert status.applications["default"].status == "RUNNING"
    """

    client = _get_global_client(raise_if_no_controller_running=False)
    if client is None:
        # Serve has not started yet
        return ServeStatus()

    ServeUsageTag.SERVE_STATUS_API_USED.record("1")
    details = ServeInstanceDetails(**client.get_serve_details())
    return details._get_status()


@PublicAPI(stability="alpha")
def get_app_handle(name: str) -> DeploymentHandle:
    """Get a handle to the application's ingress deployment by name.

    Args:
        name: Name of application to get a handle to.

    Raises:
        RayServeException: If no Serve controller is running, or if the
            application does not exist.

    .. code-block:: python

            import ray
            from ray import serve

            @serve.deployment
            def f(val: int) -> int:
                return val * 2

            serve.run(f.bind(), name="my_app")
            handle = serve.get_app_handle("my_app")
            assert handle.remote(3).result() == 6
    """

    client = _get_global_client()
    ingress = ray.get(client._controller.get_ingress_deployment_name.remote(name))
    if ingress is None:
        raise RayServeException(f"Application '{name}' does not exist.")

    ServeUsageTag.SERVE_GET_APP_HANDLE_API_USED.record("1")
    # There is no need to check if the deployment exists since the
    # deployment name was just fetched from the controller
    return client.get_handle(ingress, name, check_exists=False)


@DeveloperAPI
def get_deployment_handle(
    deployment_name: str,
    app_name: Optional[str] = None,
    _check_exists: bool = True,
    _record_telemetry: bool = True,
) -> DeploymentHandle:
    """Get a handle to a deployment by name.

    This is a developer API and is for advanced Ray users and library developers.

    Args:
        deployment_name: Name of deployment to get a handle to.
        app_name: Application in which deployment resides. If calling
            from inside a Serve application and `app_name` is not
            specified, this will default to the application from which
            this API is called.

    Raises:
        RayServeException: If no Serve controller is running, or if
            calling from outside a Serve application and no application
            name is specified.

    The following example gets the handle to the ingress deployment of
    an application, which is equivalent to using `serve.get_app_handle`.

    .. testcode::

            import ray
            from ray import serve

            @serve.deployment
            def f(val: int) -> int:
                return val * 2

            serve.run(f.bind(), name="my_app")
            handle = serve.get_deployment_handle("f", app_name="my_app")
            assert handle.remote(3).result() == 6

            serve.shutdown()

    The following example demonstrates how you can use this API to get
    the handle to a non-ingress deployment in an application.

    .. testcode::

            import ray
            from ray import serve
            from ray.serve.handle import DeploymentHandle

            @serve.deployment
            class Multiplier:
                def __init__(self, multiple: int):
                    self._multiple = multiple

                def __call__(self, val: int) -> int:
                    return val * self._multiple

            @serve.deployment
            class Adder:
                def __init__(self, handle: DeploymentHandle, increment: int):
                    self._handle = handle
                    self._increment = increment

                async def __call__(self, val: int) -> int:
                    return await self._handle.remote(val) + self._increment


            # The app calculates 2 * x + 3
            serve.run(Adder.bind(Multiplier.bind(2), 3), name="math_app")
            handle = serve.get_app_handle("math_app")
            assert handle.remote(5).result() == 13

            # Get handle to Multiplier only
            handle = serve.get_deployment_handle("Multiplier", app_name="math_app")
            assert handle.remote(5).result() == 10

            serve.shutdown()
    """

    client = _get_global_client()

    internal_replica_context = _get_internal_replica_context()
    if app_name is None:
        if internal_replica_context is None:
            raise RayServeException(
                "Please specify an application name when getting a deployment handle "
                "outside of a Serve application."
            )
        else:
            app_name = internal_replica_context.app_name

    if _record_telemetry:
        ServeUsageTag.SERVE_GET_DEPLOYMENT_HANDLE_API_USED.record("1")

    return client.get_handle(deployment_name, app_name, check_exists=_check_exists)
