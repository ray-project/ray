import collections
import inspect
import logging
import warnings
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, FastAPI

import ray
from ray import cloudpickle
from ray.dag import DAGNode
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    SERVE_DEFAULT_APP_NAME,
)
from ray.serve._private.deployment_graph_build import build as pipeline_build
from ray.serve._private.deployment_graph_build import (
    get_and_validate_ingress_deployment,
)
from ray.serve._private.http_util import (
    ASGIAppReplicaWrapper,
    make_fastapi_class_based_view,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    DEFAULT,
    Default,
    ensure_serialization_context,
    extract_self_if_method_call,
    get_random_letters,
)
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentMode,
    HTTPOptions,
    ProxyLocation,
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
from ray.serve.handle import DeploymentHandle, RayServeSyncHandle
from ray.serve.multiplex import _ModelMultiplexWrapper
from ray.serve.schema import ServeInstanceDetails, ServeStatus
from ray.util.annotations import Deprecated, DeveloperAPI, PublicAPI

from ray.serve._private import api as _private_api  # isort:skip

logger = logging.getLogger(__file__)


@PublicAPI(stability="stable")
def start(
    detached: bool = True,
    proxy_location: Union[None, str, ProxyLocation] = None,
    http_options: Union[None, dict, HTTPOptions] = None,
    dedicated_cpu: bool = False,
    grpc_options: Union[None, dict, gRPCOptions] = None,
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
    """

    if detached is not True:
        raise ValueError(
            "`detached=False` is no longer supported. "
            "In a future release, it will be removed altogether."
        )

    if dedicated_cpu is not False:
        raise ValueError(
            "`dedicated_cpu=True` is no longer supported. "
            "In a future release, it will be removed altogether."
        )

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

                    # Prints "MyDeployment#<replica_tag>"
                    print(serve.get_replica_context().replica_tag)

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
def ingress(app: Union["FastAPI", "APIRouter", Callable]) -> Callable:
    """Wrap a deployment class with a FastAPI application for HTTP request parsing.

    Example:

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

    Args:
        app: the FastAPI app or router object to wrap this class with.
            Can be any ASGI-compatible callable.
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

        class ASGIIngressWrapper(cls, ASGIAppReplicaWrapper):
            def __init__(self, *args, **kwargs):
                # Call user-defined constructor.
                cls.__init__(self, *args, **kwargs)

                ServeUsageTag.FASTAPI_USED.record("1")
                ASGIAppReplicaWrapper.__init__(self, frozen_app)

            async def __del__(self):
                await ASGIAppReplicaWrapper.__del__(self)

                # Call user-defined destructor if defined.
                if hasattr(cls, "__del__"):
                    cls.__del__(self)

        ASGIIngressWrapper.__name__ = cls.__name__
        if hasattr(frozen_app, "docs_url"):
            ASGIIngressWrapper.__fastapi_docs_path__ = frozen_app.docs_url

        return ASGIIngressWrapper

    return decorator


@PublicAPI(stability="stable")
def deployment(
    _func_or_class: Optional[Callable] = None,
    name: Default[str] = DEFAULT.VALUE,
    version: Default[str] = DEFAULT.VALUE,
    num_replicas: Default[Optional[int]] = DEFAULT.VALUE,
    init_args: Default[Tuple[Any]] = DEFAULT.VALUE,
    init_kwargs: Default[Dict[Any, Any]] = DEFAULT.VALUE,
    route_prefix: Default[Union[str, None]] = DEFAULT.VALUE,
    ray_actor_options: Default[Dict] = DEFAULT.VALUE,
    placement_group_bundles: Optional[List[Dict[str, float]]] = DEFAULT.VALUE,
    placement_group_strategy: Optional[str] = DEFAULT.VALUE,
    max_replicas_per_node: Default[int] = DEFAULT.VALUE,
    user_config: Default[Optional[Any]] = DEFAULT.VALUE,
    max_concurrent_queries: Default[int] = DEFAULT.VALUE,
    autoscaling_config: Default[Union[Dict, AutoscalingConfig, None]] = DEFAULT.VALUE,
    graceful_shutdown_wait_loop_s: Default[float] = DEFAULT.VALUE,
    graceful_shutdown_timeout_s: Default[float] = DEFAULT.VALUE,
    health_check_period_s: Default[float] = DEFAULT.VALUE,
    health_check_timeout_s: Default[float] = DEFAULT.VALUE,
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
        name: Name uniquely identifying this deployment within the application.
            If not provided, the name of the class or function is used.
        num_replicas: Number of replicas to run that handle requests to
            this deployment. Defaults to 1.
        autoscaling_config: Parameters to configure autoscaling behavior. If this
            is set, `num_replicas` cannot be set.
        init_args: [DEPRECATED] These should be passed to `.bind()` instead.
        init_kwargs: [DEPRECATED] These should be passed to `.bind()` instead.
        route_prefix: [DEPRECATED] Route prefix should be set per-application
            through `serve.run()`.
        ray_actor_options: Options to pass to the Ray Actor decorator, such as
            resource requirements. Valid options are: `accelerator_type`, `memory`,
            `num_cpus`, `num_gpus`, `object_store_memory`, `resources`,
            and `runtime_env`.
        placement_group_bundles: Defines a set of placement group bundles to be
            scheduled *for each replica* of this deployment. The replica actor will
            be scheduled in the first bundle provided, so the resources specified in
            `ray_actor_options` must be a subset of the first bundle's resources. All
            actors and tasks created by the replica actor will be scheduled in the
            placement group by default (`placement_group_capture_child_tasks` is set
            to True).
        placement_group_strategy: Strategy to use for the replica placement group
            specified via `placement_group_bundles`. Defaults to `PACK`.
        user_config: Config to pass to the reconfigure method of the deployment. This
            can be updated dynamically without restarting the replicas of the
            deployment. The user_config must be fully JSON-serializable.
        max_concurrent_queries: Maximum number of queries that are sent to a
            replica of this deployment without receiving a response. Defaults to 100.
        health_check_period_s: Duration between health check calls for the replica.
            Defaults to 10s. The health check is by default a no-op Actor call to the
            replica, but you can define your own health check using the "check_health"
            method in your deployment that raises an exception when unhealthy.
        health_check_timeout_s: Duration in seconds, that replicas wait for a health
            check method to return before considering it as failed. Defaults to 30s.
        graceful_shutdown_wait_loop_s: Duration that replicas wait until there is
            no more work to be done before shutting down. Defaults to 2s.
        graceful_shutdown_timeout_s: Duration to wait for a replica to gracefully
            shut down before being forcefully killed. Defaults to 20s.
        max_replicas_per_node: [EXPERIMENTAL] The max number of deployment replicas can
            run on a single node. Valid values are None (no limitation)
            or an integer in the range of [1, 100].
            Defaults to no limitation.

    Returns:
        `Deployment`
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

    if route_prefix is not DEFAULT.VALUE:
        logger.warning(
            "DeprecationWarning: `route_prefix` in `@serve.deployment` has been "
            "deprecated. To specify a route prefix for an application, pass it into "
            "`serve.run` instead."
        )

    deployment_config = DeploymentConfig.from_default(
        num_replicas=num_replicas if num_replicas is not None else 1,
        user_config=user_config,
        max_concurrent_queries=max_concurrent_queries,
        autoscaling_config=autoscaling_config,
        graceful_shutdown_wait_loop_s=graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=graceful_shutdown_timeout_s,
        health_check_period_s=health_check_period_s,
        health_check_timeout_s=health_check_timeout_s,
    )
    deployment_config.user_configured_option_names = set(user_configured_option_names)

    def decorator(_func_or_class):
        replica_config = ReplicaConfig.create(
            _func_or_class,
            init_args=(init_args if init_args is not DEFAULT.VALUE else None),
            init_kwargs=(init_kwargs if init_kwargs is not DEFAULT.VALUE else None),
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
            route_prefix=route_prefix,
            _internal=True,
        )

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator


@Deprecated
def get_deployment(name: str) -> Deployment:
    raise ValueError(
        "serve.get_deployment is fully deprecated. Use serve.get_app_handle() to get a "
        "handle to a running Serve application."
    )


@Deprecated
def list_deployments() -> Dict[str, Deployment]:
    raise ValueError(
        "serve.list_deployments() is fully deprecated. Use serve.status() to get a "
        "list of all active applications and deployments."
    )


@PublicAPI(stability="stable")
def run(
    target: Application,
    _blocking: bool = True,
    host: str = DEFAULT_HTTP_HOST,
    port: int = DEFAULT_HTTP_PORT,
    name: str = SERVE_DEFAULT_APP_NAME,
    route_prefix: str = DEFAULT.VALUE,
) -> Optional[RayServeSyncHandle]:
    """Run an application and return a handle to its ingress deployment.

    The application is returned by `Deployment.bind()`. Example:

    .. code-block:: python

        handle = serve.run(MyDeployment.bind())
        ray.get(handle.remote())

    Args:
        target:
            A Serve application returned by `Deployment.bind()`.
        host: [DEPRECATED: use `serve.start` to set HTTP options]
            Host for HTTP servers to listen on. Defaults to
            "127.0.0.1". To expose Serve publicly, you probably want to set
            this to "0.0.0.0".
        port: [DEPRECATED: use `serve.start` to set HTTP options]
            Port for HTTP server. Defaults to 8000.
        name: Application name. If not provided, this will be the only
            application running on the cluster (it will delete all others).
        route_prefix: Route prefix for HTTP requests. If not provided, it will use
            route_prefix of the ingress deployment. If specified neither as an argument
            nor in the ingress deployment, the route prefix will default to '/'.

    Returns:
        RayServeSyncHandle: A handle that can be used to call the application.
    """

    if len(name) == 0:
        raise RayServeException("Application name must a non-empty string.")

    if host != DEFAULT_HTTP_HOST or port != DEFAULT_HTTP_PORT:
        warnings.warn(
            "Specifying host and port in `serve.run` is deprecated and will be "
            "removed in a future version. To specify custom HTTP options, use "
            "`serve.start`."
        )

    client = _private_api.serve_start(
        http_options={"host": host, "port": port, "location": "EveryNode"},
    )

    # Record after Ray has been started.
    ServeUsageTag.API_VERSION.record("v2")

    if isinstance(target, Application):
        deployments = pipeline_build(target._get_internal_dag_node(), name)
        ingress = get_and_validate_ingress_deployment(deployments)
    else:
        msg = "`serve.run` expects an `Application` returned by `Deployment.bind()`."
        if isinstance(target, DAGNode):
            msg += (
                " If you are using the DAG API, you must bind the DAG node to a "
                "deployment like: `app = Deployment.bind(my_dag_output)`. "
            )
        raise TypeError(msg)

    parameter_group = []

    for deployment in deployments:
        # Overwrite route prefix
        if route_prefix is not DEFAULT.VALUE and deployment._route_prefix is not None:
            if route_prefix is not None and not route_prefix.startswith("/"):
                raise ValueError(
                    "The route_prefix must start with a forward slash ('/')"
                )

            deployment._route_prefix = route_prefix
        deployment_parameters = {
            "name": deployment._name,
            "replica_config": deployment._replica_config,
            "deployment_config": deployment._deployment_config,
            "version": deployment._version or get_random_letters(),
            "route_prefix": deployment.route_prefix,
            "url": deployment.url,
            "docs_path": deployment._docs_path,
            "ingress": deployment._name == ingress._name,
        }
        parameter_group.append(deployment_parameters)
    client.deploy_application(
        name,
        parameter_group,
        _blocking=_blocking,
    )

    if ingress is not None:
        # The deployment state is not guaranteed to be created after
        # deploy_application returns; the application state manager will
        # need another reconcile iteration to create it.
        client._wait_for_deployment_created(ingress.name, name)
        handle = client.get_handle(ingress.name, name, missing_ok=True)
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
            # This can also be set when using `RayServeHandle`.
            handle.options(multiplexed_model_id="model_1").remote("blablabla")

            # In your deployment code, you can retrieve the model id from
            # `get_multiplexed_model_id()`.
            @serve.deployment
            def my_deployment_function(request):
                assert serve.get_multiplexed_model_id() == "model_1"
    """
    _request_context = ray.serve.context._serve_request_context.get()
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
    # Default to async within a deployment and sync outside a deployment.
    sync = _get_internal_replica_context() is None
    return client.get_handle(ingress, name, sync=sync, use_new_handle_api=True)


@DeveloperAPI
def get_deployment_handle(
    deployment_name: str,
    app_name: Optional[str] = None,
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

    ServeUsageTag.SERVE_GET_DEPLOYMENT_HANDLE_API_USED.record("1")
    # Default to async within a deployment and sync outside a deployment.
    sync = internal_replica_context is None
    return client.get_handle(
        deployment_name, app_name, sync=sync, use_new_handle_api=True
    )
