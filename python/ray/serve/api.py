import asyncio
import atexit
import collections
import inspect
import logging
import random
import re
import time
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union, overload

from fastapi import APIRouter, FastAPI
from starlette.requests import Request
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray.actor import ActorHandle
from ray.serve.common import DeploymentInfo, GoalId, ReplicaTag
from ray.serve.config import (AutoscalingConfig, DeploymentConfig, HTTPOptions,
                              ReplicaConfig)
from ray.serve.constants import (DEFAULT_CHECKPOINT_PATH, HTTP_PROXY_TIMEOUT,
                                 SERVE_CONTROLLER_NAME, MAX_CACHED_HANDLES,
                                 CONTROLLER_MAX_CONCURRENCY)
from ray.serve.controller import ServeController
from ray.serve.exceptions import RayServeException
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.http_util import ASGIHTTPSender, make_fastapi_class_based_view
from ray.serve.utils import (LoggingContext, ensure_serialization_context,
                             format_actor_name, get_current_node_resource_key,
                             get_random_letters, logger, DEFAULT)
from ray.util.annotations import PublicAPI
import ray
from ray import cloudpickle

_INTERNAL_REPLICA_CONTEXT = None
_global_client = None

_UUID_RE = re.compile(
    "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}")


def _get_controller_namespace(detached):
    controller_namespace = ray.get_runtime_context().namespace

    if not detached:
        return controller_namespace

    # Start controller in "serve" namespace if detached and currently
    # in anonymous namespace.
    if _UUID_RE.fullmatch(controller_namespace) is not None:
        controller_namespace = "serve"
    return controller_namespace


def _get_global_client():
    if _global_client is not None:
        return _global_client

    return _connect()


def _set_global_client(client):
    global _global_client
    _global_client = client


@dataclass
class ReplicaContext:
    """Stores data for Serve API calls from within deployments."""
    deployment: str
    replica_tag: ReplicaTag
    _internal_controller_name: str
    servable_object: Callable


def _set_internal_replica_context(
        deployment: str,
        replica_tag: ReplicaTag,
        controller_name: str,
        servable_object: Callable,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        deployment, replica_tag, controller_name, servable_object)


def _ensure_connected(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, **kwargs):
        if self._shutdown:
            raise RayServeException("Client has already been shut down.")
        return f(self, *args, **kwargs)

    return check


class Client:
    def __init__(self,
                 controller: ActorHandle,
                 controller_name: str,
                 detached: bool = False):
        self._controller = controller
        self._controller_name = controller_name
        self._detached = detached
        self._shutdown = False
        self._http_config: HTTPOptions = ray.get(
            controller.get_http_config.remote())
        self._root_url = ray.get(self._controller.get_root_url.remote())

        # Each handle has the overhead of long poll client, therefore cached.
        self.handle_cache = dict()
        self._evicted_handle_keys = set()

        # NOTE(edoakes): Need this because the shutdown order isn't guaranteed
        # when the interpreter is exiting so we can't rely on __del__ (it
        # throws a nasty stacktrace).
        if not self._detached:

            def shutdown_serve_client():
                self.shutdown()

            atexit.register(shutdown_serve_client)

    @property
    def root_url(self):
        return self._root_url

    def __del__(self):
        if not self._detached:
            logger.debug("Shutting down Ray Serve because client went out of "
                         "scope. To prevent this, either keep a reference to "
                         "the client or use serve.start(detached=True).")
            self.shutdown()

    def __reduce__(self):
        raise RayServeException(("Ray Serve client cannot be serialized."))

    def shutdown(self) -> None:
        """Completely shut down the connected Serve instance.

        Shuts down all processes and deletes all state associated with the
        instance.
        """
        if (not self._shutdown) and ray.is_initialized():
            for goal_id in ray.get(self._controller.shutdown.remote()):
                self._wait_for_goal(goal_id)

            ray.kill(self._controller, no_restart=True)

            # Wait for the named actor entry gets removed as well.
            started = time.time()
            while True:
                try:
                    controller_namespace = _get_controller_namespace(
                        self._detached)
                    ray.get_actor(
                        self._controller_name, namespace=controller_namespace)
                    if time.time() - started > 5:
                        logger.warning(
                            "Waited 5s for Serve to shutdown gracefully but "
                            "the controller is still not cleaned up. "
                            "You can ignore this warning if you are shutting "
                            "down the Ray cluster.")
                        break
                except ValueError:  # actor name is removed
                    break

            self._shutdown = True

    def _wait_for_goal(self,
                       goal_id: Optional[GoalId],
                       timeout: Optional[float] = None) -> bool:
        if goal_id is None:
            return True

        ready, _ = ray.wait(
            [self._controller.wait_for_goal.remote(goal_id)], timeout=timeout)
        # AsyncGoal could return exception if set, ray.get()
        # retrieves and throws it to user code explicitly.
        if len(ready) == 1:
            async_goal_exception = ray.get(ready)[0]
            if async_goal_exception is not None:
                raise async_goal_exception
            return True
        else:
            return False

    @_ensure_connected
    def deploy(
            self,
            name: str,
            deployment_def: Union[Callable, Type[Callable], str],
            init_args: Tuple[Any],
            init_kwargs: Dict[Any, Any],
            ray_actor_options: Optional[Dict] = None,
            config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
            version: Optional[str] = None,
            prev_version: Optional[str] = None,
            route_prefix: Optional[str] = None,
            url: Optional[str] = None,
            _blocking: Optional[bool] = True) -> Optional[GoalId]:
        if config is None:
            config = {}
        if ray_actor_options is None:
            ray_actor_options = {}

        curr_job_env = ray.get_runtime_context().runtime_env
        if "runtime_env" in ray_actor_options:
            ray_actor_options["runtime_env"].setdefault(
                "working_dir", curr_job_env.get("working_dir"))
        else:
            ray_actor_options["runtime_env"] = curr_job_env

        replica_config = ReplicaConfig(
            deployment_def,
            init_args=init_args,
            init_kwargs=init_kwargs,
            ray_actor_options=ray_actor_options)

        if isinstance(config, dict):
            deployment_config = DeploymentConfig.parse_obj(config)
        elif isinstance(config, DeploymentConfig):
            deployment_config = config
        else:
            raise TypeError(
                "config must be a DeploymentConfig or a dictionary.")

        if deployment_config.autoscaling_config is not None and \
            deployment_config.max_concurrent_queries < deployment_config. \
                autoscaling_config.target_num_ongoing_requests_per_replica:
            logger.warning("Autoscaling will never happen, "
                           "because 'max_concurrent_queries' is less than "
                           "'target_num_ongoing_requests_per_replica' now.")

        goal_id, updating = ray.get(
            self._controller.deploy.remote(name,
                                           deployment_config.to_proto_bytes(),
                                           replica_config, version,
                                           prev_version, route_prefix,
                                           ray.get_runtime_context().job_id))

        tag = f"component=serve deployment={name}"

        if updating:
            msg = f"Updating deployment '{name}'"
            if version is not None:
                msg += f" to version '{version}'"
            logger.info(f"{msg}. {tag}")
        else:
            logger.info(f"Deployment '{name}' is already at version "
                        f"'{version}', not updating. {tag}")

        if _blocking:
            self._wait_for_goal(goal_id)

            if url is not None:
                url_part = f" at `{url}`"
            else:
                url_part = ""
            logger.info(
                f"Deployment '{name}{':'+version if version else ''}' is ready"
                f"{url_part}. {tag}")
        else:
            return goal_id

    @_ensure_connected
    def delete_deployment(self, name: str) -> None:
        self._wait_for_goal(
            ray.get(self._controller.delete_deployment.remote(name)))

    @_ensure_connected
    def get_deployment_info(self, name: str) -> Tuple[DeploymentInfo, str]:
        return ray.get(self._controller.get_deployment_info.remote(name))

    @_ensure_connected
    def list_deployments(self) -> Dict[str, Tuple[DeploymentInfo, str]]:
        return ray.get(self._controller.list_deployments.remote())

    @_ensure_connected
    def get_handle(
            self,
            deployment_name: str,
            missing_ok: Optional[bool] = False,
            sync: bool = True,
            _internal_pickled_http_request: bool = False,
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Retrieve RayServeHandle for service deployment to invoke it from Python.

        Args:
            deployment_name (str): A registered service deployment.
            missing_ok (bool): If true, then Serve won't check the deployment
                is registered. False by default.
            sync (bool): If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return a ServeHandle
                that's only usable in asyncio loop.

        Returns:
            RayServeHandle
        """
        cache_key = (deployment_name, missing_ok, sync)
        if cache_key in self.handle_cache:
            cached_handle = self.handle_cache[cache_key]
            if cached_handle.is_polling and cached_handle.is_same_loop:
                return cached_handle

        all_endpoints = ray.get(self._controller.get_all_endpoints.remote())
        if not missing_ok and deployment_name not in all_endpoints:
            raise KeyError(f"Deployment '{deployment_name}' does not exist.")

        try:
            asyncio_loop_running = asyncio.get_event_loop().is_running()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                asyncio_loop_running = False
            else:
                raise ex

        if asyncio_loop_running and sync:
            logger.warning(
                "You are retrieving a sync handle inside an asyncio loop. "
                "Try getting client.get_handle(.., sync=False) to get better "
                "performance. Learn more at https://docs.ray.io/en/master/"
                "serve/http-servehandle.html#sync-and-async-handles")

        if not asyncio_loop_running and not sync:
            logger.warning(
                "You are retrieving an async handle outside an asyncio loop. "
                "You should make sure client.get_handle is called inside a "
                "running event loop. Or call client.get_handle(.., sync=True) "
                "to create sync handle. Learn more at https://docs.ray.io/en/"
                "master/serve/http-servehandle.html#sync-and-async-handles")

        if sync:
            handle = RayServeSyncHandle(
                self._controller,
                deployment_name,
                _internal_pickled_http_request=_internal_pickled_http_request,
            )
        else:
            handle = RayServeHandle(
                self._controller,
                deployment_name,
                _internal_pickled_http_request=_internal_pickled_http_request,
            )

        self.handle_cache[cache_key] = handle
        if cache_key in self._evicted_handle_keys:
            logger.warning(
                "You just got a ServeHandle that was evicted from internal "
                "cache. This means you are getting too many ServeHandles in "
                "the same process, this will bring down Serve's performance. "
                "Please post a github issue at "
                "https://github.com/ray-project/ray/issues to let the Serve "
                "team to find workaround for your use case.")

        if len(self.handle_cache) > MAX_CACHED_HANDLES:
            # Perform random eviction to keep the handle cache from growing
            # infinitely. We used use WeakValueDictionary but hit
            # https://github.com/ray-project/ray/issues/18980.
            evict_key = random.choice(list(self.handle_cache.keys()))
            self._evicted_handle_keys.add(evict_key)
            self.handle_cache.pop(evict_key)

        return handle


@PublicAPI(stability="beta")
def start(
        detached: bool = False,
        http_options: Optional[Union[dict, HTTPOptions]] = None,
        dedicated_cpu: bool = False,
        _checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
        **kwargs,
) -> Client:
    """Initialize a serve instance.

    By default, the instance will be scoped to the lifetime of the returned
    Client object (or when the script exits). If detached is set to True, the
    instance will instead persist until serve.shutdown() is called. This is
    only relevant if connecting to a long-running Ray cluster (e.g., with
    ray.init(address="auto") or ray.init("ray://<remote_addr>")).

    Args:
        detached (bool): Whether not the instance should be detached from this
          script. If set, the instance will live on the Ray cluster until it is
          explicitly stopped with serve.shutdown().
        http_options (Optional[Dict, serve.HTTPOptions]): Configuration options
          for HTTP proxy. You can pass in a dictionary or HTTPOptions object
          with fields:

            - host(str, None): Host for HTTP servers to listen on. Defaults to
              "127.0.0.1". To expose Serve publicly, you probably want to set
              this to "0.0.0.0".
            - port(int): Port for HTTP server. Defaults to 8000.
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
        dedicated_cpu (bool): Whether to reserve a CPU core for the internal
          Serve controller actor.  Defaults to False.
    """
    http_deprecated_args = ["http_host", "http_port", "http_middlewares"]
    for key in http_deprecated_args:
        if key in kwargs:
            raise ValueError(
                f"{key} is deprecated, please use serve.start(http_options="
                f'{{"{key}": {kwargs[key]}}}) instead.')
    # Initialize ray if needed.
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

    controller_namespace = _get_controller_namespace(detached)

    try:
        client = _get_global_client()
        logger.info("Connecting to existing Serve instance in namespace "
                    f"'{controller_namespace}'.")
        return client
    except RayServeException:
        pass

    if detached:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = format_actor_name(get_random_letters(),
                                            SERVE_CONTROLLER_NAME)

    if isinstance(http_options, dict):
        http_options = HTTPOptions.parse_obj(http_options)
    if http_options is None:
        http_options = HTTPOptions()

    controller = ServeController.options(
        num_cpus=(1 if dedicated_cpu else 0),
        name=controller_name,
        lifetime="detached" if detached else None,
        max_restarts=-1,
        max_task_retries=-1,
        # Pin Serve controller on the head node.
        resources={
            get_current_node_resource_key(): 0.01
        },
        namespace=controller_namespace,
        max_concurrency=CONTROLLER_MAX_CONCURRENCY,
    ).remote(
        controller_name,
        http_options,
        _checkpoint_path,
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
                "HTTP proxies not available after {HTTP_PROXY_TIMEOUT}s.")

    client = Client(controller, controller_name, detached=detached)
    _set_global_client(client)
    logger.info(f"Started{' detached ' if detached else ' '}Serve instance in "
                f"namespace '{controller_namespace}'.")
    return client


def _connect() -> Client:
    """Connect to an existing Serve instance on this Ray cluster.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a replica, this will connect to the same Serve
    instance that the replica is running in.
    """

    # Initialize ray if needed.
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

    # When running inside of a replica, _INTERNAL_REPLICA_CONTEXT is set to
    # ensure that the correct instance is connected to.
    if _INTERNAL_REPLICA_CONTEXT is None:
        controller_name = SERVE_CONTROLLER_NAME
        controller_namespace = _get_controller_namespace(detached=True)
    else:
        controller_name = _INTERNAL_REPLICA_CONTEXT._internal_controller_name
        controller_namespace = _get_controller_namespace(detached=False)

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(
            controller_name, namespace=controller_namespace)
    except ValueError:
        raise RayServeException("There is no "
                                "instance running on this Ray cluster. Please "
                                "call `serve.start(detached=True) to start "
                                "one.")

    client = Client(controller, controller_name, detached=True)
    _set_global_client(client)
    return client


@PublicAPI
def shutdown() -> None:
    """Completely shut down the connected Serve instance.

    Shuts down all processes and deletes all state associated with the
    instance.
    """
    if _global_client is None:
        return

    _get_global_client().shutdown()
    _set_global_client(None)


@PublicAPI
def get_replica_context() -> ReplicaContext:
    """If called from a deployment, returns the deployment and replica tag.

    A replica tag uniquely identifies a single replica for a Ray Serve
    deployment at runtime.  Replica tags are of the form
    `<deployment_name>#<random letters>`.

    Raises:
        RayServeException: if not called from within a Ray Serve deployment.
    Example:
        >>> serve.get_replica_context().deployment # deployment_name
        >>> serve.get_replica_context().replica_tag # deployment_name#krcwoa
    """
    if _INTERNAL_REPLICA_CONTEXT is None:
        raise RayServeException("`serve.get_replica_context()` "
                                "may only be called from within a "
                                "Ray Serve deployment.")
    return _INTERNAL_REPLICA_CONTEXT


@PublicAPI(stability="beta")
def ingress(app: Union["FastAPI", "APIRouter", Callable]):
    """Mark an ASGI application ingress for Serve.

    Args:
        app (FastAPI,APIRouter,Starlette,etc): the app or router object serve
            as ingress for this deployment. It can be any ASGI compatible
            object.

    Example:
    >>> app = FastAPI()
    >>> @serve.deployment
        @serve.ingress(app)
        class App:
            pass
    >>> App.deploy()
    """

    def decorator(cls):
        if not inspect.isclass(cls):
            raise ValueError("@serve.ingress must be used with a class.")

        if issubclass(cls, collections.abc.Callable):
            raise ValueError(
                "Class passed to @serve.ingress may not have __call__ method.")

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

                self._serve_app = frozen_app

                # Use uvicorn's lifespan handling code to properly deal with
                # startup and shutdown event.
                self._serve_asgi_lifespan = LifespanOn(
                    Config(self._serve_app, lifespan="on"))
                # Replace uvicorn logger with our own.
                self._serve_asgi_lifespan.logger = logger
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                        self._serve_asgi_lifespan.logger,
                        level=logging.WARNING):
                    await self._serve_asgi_lifespan.startup()

            async def __call__(self, request: Request):
                sender = ASGIHTTPSender()
                await self._serve_app(
                    request.scope,
                    request._receive,
                    sender,
                )
                return sender.build_starlette_response()

            # NOTE: __del__ must be async so that we can run asgi shutdown
            # in the same event loop.
            async def __del__(self):
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                        self._serve_asgi_lifespan.logger,
                        level=logging.WARNING):
                    await self._serve_asgi_lifespan.shutdown()

                # Make sure to call user's del method as well.
                super_cls = super()
                if hasattr(super_cls, "__del__"):
                    super_cls.__del__()

        ASGIAppWrapper.__name__ = cls.__name__
        return ASGIAppWrapper

    return decorator


@PublicAPI
class Deployment:
    def __init__(self,
                 func_or_class: Callable,
                 name: str,
                 config: DeploymentConfig,
                 version: Optional[str] = None,
                 prev_version: Optional[str] = None,
                 init_args: Optional[Tuple[Any]] = None,
                 init_kwargs: Optional[Tuple[Any]] = None,
                 route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
                 ray_actor_options: Optional[Dict] = None,
                 _internal=False) -> None:
        """Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.

        Deployments should be created, retrieved, and updated using
        `@serve.deployment`, `serve.get_deployment`, and `Deployment.options`,
        respectively.
        """

        if not _internal:
            raise RuntimeError(
                "The Deployment constructor should not be called "
                "directly. Use `@serve.deployment` instead.")
        if not callable(func_or_class):
            raise TypeError(
                "@serve.deployment must be called on a class or function.")
        if not isinstance(name, str):
            raise TypeError("name must be a string.")
        if not (version is None or isinstance(version, str)):
            raise TypeError("version must be a string.")
        if not (prev_version is None or isinstance(prev_version, str)):
            raise TypeError("prev_version must be a string.")
        if not (init_args is None or isinstance(init_args, (tuple, list))):
            raise TypeError("init_args must be a tuple.")
        if not (init_kwargs is None or isinstance(init_kwargs, dict)):
            raise TypeError("init_kwargs must be a dict.")
        if route_prefix is not DEFAULT.VALUE and route_prefix is not None:
            if not isinstance(route_prefix, str):
                raise TypeError("route_prefix must be a string.")
            if not route_prefix.startswith("/"):
                raise ValueError("route_prefix must start with '/'.")
            if route_prefix != "/" and route_prefix.endswith("/"):
                raise ValueError(
                    "route_prefix must not end with '/' unless it's the root.")
            if "{" in route_prefix or "}" in route_prefix:
                raise ValueError("route_prefix may not contain wildcards.")
        if not (ray_actor_options is None
                or isinstance(ray_actor_options, dict)):
            raise TypeError("ray_actor_options must be a dict.")

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        # TODO(architkulkarni): Enforce that autoscaling_config and
        # user-provided num_replicas should be mutually exclusive.
        if version is None and config.autoscaling_config is not None:
            # TODO(architkulkarni): Remove this restriction.
            raise ValueError(
                "Currently autoscaling is only supported for "
                "versioned deployments. Try @serve.deployment(version=...).")

        self._func_or_class = func_or_class
        self._name = name
        self._version = version
        self._prev_version = prev_version
        self._config = config
        self._init_args = init_args
        self._init_kwargs = init_kwargs
        self._route_prefix = route_prefix
        self._ray_actor_options = ray_actor_options

    @property
    def name(self) -> str:
        """Unique name of this deployment."""
        return self._name

    @property
    def version(self) -> Optional[str]:
        """Version of this deployment.

        If None, will be redeployed every time `.deploy()` is called.
        """
        return self._version

    @property
    def prev_version(self) -> Optional[str]:
        """Existing version of deployment to target.

        If prev_version does not match with existing deployment
        version, the deployment will fail to be deployed.
        """
        return self._prev_version

    @property
    def func_or_class(self) -> Callable:
        """Underlying class or function that this deployment wraps."""
        return self._func_or_class

    @property
    def num_replicas(self) -> int:
        """Current target number of replicas."""
        return self._config.num_replicas

    @property
    def user_config(self) -> Any:
        """Current dynamic user-provided config options."""
        return self._config.user_config

    @property
    def max_concurrent_queries(self) -> int:
        """Current max outstanding queries from each handle."""
        return self._config.max_concurrent_queries

    @property
    def route_prefix(self) -> Optional[str]:
        """HTTP route prefix that this deployment is exposed under."""
        if self._route_prefix is DEFAULT.VALUE:
            return f"/{self._name}"
        return self._route_prefix

    @property
    def ray_actor_options(self) -> Optional[Dict]:
        """Actor options such as resources required for each replica."""
        return self._ray_actor_options

    @property
    def init_args(self) -> Tuple[Any]:
        """Positional args passed to the underlying class's constructor."""
        return self._init_args

    @property
    def init_kwargs(self) -> Tuple[Any]:
        """Keyword args passed to the underlying class's constructor."""
        return self._init_args

    @property
    def url(self) -> Optional[str]:
        """Full HTTP url for this deployment."""
        if self._route_prefix is None:
            # this deployment is not exposed over HTTP
            return None

        return _get_global_client().root_url + self.route_prefix

    def __call__(self):
        raise RuntimeError("Deployments cannot be constructed directly. "
                           "Use `deployment.deploy() instead.`")

    @PublicAPI
    def deploy(self, *init_args, _blocking=True, **init_kwargs):
        """Deploy or update this deployment.

        Args:
            init_args (optional): args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
            init_kwargs (optional): kwargs to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        if len(init_args) == 0 and self._init_args is not None:
            init_args = self._init_args
        if len(init_kwargs) == 0 and self._init_kwargs is not None:
            init_kwargs = self._init_kwargs

        return _get_global_client().deploy(
            self._name,
            self._func_or_class,
            init_args,
            init_kwargs,
            ray_actor_options=self._ray_actor_options,
            config=self._config,
            version=self._version,
            prev_version=self._prev_version,
            route_prefix=self.route_prefix,
            url=self.url,
            _blocking=_blocking)

    @PublicAPI
    def delete(self):
        """Delete this deployment."""
        return _get_global_client().delete_deployment(self._name)

    @PublicAPI
    def get_handle(self, sync: Optional[bool] = True
                   ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Get a ServeHandle to this deployment to invoke it from Python.

        Args:
            sync (bool): If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return an
                asyncio-optimized ServeHandle that's only usable in an asyncio
                loop.

        Returns:
            ServeHandle
        """
        return _get_global_client().get_handle(
            self._name, missing_ok=True, sync=sync)

    @PublicAPI
    def options(self,
                func_or_class: Optional[Callable] = None,
                name: Optional[str] = None,
                version: Optional[str] = None,
                prev_version: Optional[str] = None,
                init_args: Optional[Tuple[Any]] = None,
                init_kwargs: Optional[Dict[Any, Any]] = None,
                route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
                num_replicas: Optional[int] = None,
                ray_actor_options: Optional[Dict] = None,
                user_config: Optional[Any] = None,
                max_concurrent_queries: Optional[int] = None,
                _autoscaling_config: Optional[Union[Dict,
                                                    AutoscalingConfig]] = None,
                _graceful_shutdown_wait_loop_s: Optional[float] = None,
                _graceful_shutdown_timeout_s: Optional[float] = None
                ) -> "Deployment":
        """Return a copy of this deployment with updated options.

        Only those options passed in will be updated, all others will remain
        unchanged from the existing deployment.
        """
        new_config = self._config.copy()
        if num_replicas is not None:
            new_config.num_replicas = num_replicas
        if user_config is not None:
            new_config.user_config = user_config
        if max_concurrent_queries is not None:
            new_config.max_concurrent_queries = max_concurrent_queries

        if func_or_class is None:
            func_or_class = self._func_or_class

        if name is None:
            name = self._name

        if version is None:
            version = self._version

        if init_args is None:
            init_args = self._init_args

        if init_kwargs is None:
            init_kwargs = self._init_kwargs

        if route_prefix is DEFAULT.VALUE:
            # Default is to keep the previous value
            route_prefix = self._route_prefix

        if ray_actor_options is None:
            ray_actor_options = self._ray_actor_options

        if _autoscaling_config is not None:
            new_config.autoscaling_config = _autoscaling_config

        if _graceful_shutdown_wait_loop_s is not None:
            new_config.graceful_shutdown_wait_loop_s = (
                _graceful_shutdown_wait_loop_s)

        if _graceful_shutdown_timeout_s is not None:
            new_config.graceful_shutdown_timeout_s = (
                _graceful_shutdown_timeout_s)

        return Deployment(
            func_or_class,
            name,
            new_config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    def __eq__(self, other):
        return all([
            self._name == other._name,
            self._version == other._version,
            self._config == other._config,
            self._init_args == other._init_args,
            self._init_kwargs == other._init_kwargs,
            # compare route prefix with default value resolved
            self.route_prefix == other.route_prefix,
            self._ray_actor_options == self._ray_actor_options,
        ])

    def __str__(self):
        return (f"Deployment(name={self._name},"
                f"version={self._version},"
                f"route_prefix={self.route_prefix})")

    def __repr__(self):
        return str(self)


@overload
def deployment(func_or_class: Callable) -> Deployment:
    pass


@overload
def deployment(
        name: Optional[str] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        num_replicas: Optional[int] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        _graceful_shutdown_wait_loop_s: Optional[float] = None,
        _graceful_shutdown_timeout_s: Optional[float] = None
) -> Callable[[Callable], Deployment]:
    pass


@PublicAPI
def deployment(
        _func_or_class: Optional[Callable] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        num_replicas: Optional[int] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        _graceful_shutdown_wait_loop_s: Optional[float] = None,
        _graceful_shutdown_timeout_s: Optional[float] = None
) -> Callable[[Callable], Deployment]:
    """Define a Serve deployment.

    Args:
        name (Optional[str]): Globally-unique name identifying this deployment.
            If not provided, the name of the class or function will be used.
        version (Optional[str]): Version of the deployment. This is used to
            indicate a code change for the deployment; when it is re-deployed
            with a version change, a rolling update of the replicas will be
            performed. If not provided, every deployment will be treated as a
            new version.
        prev_version (Optional[str]): Version of the existing deployment which
            is used as a precondition for the next deployment. If prev_version
            does not match with the existing deployment's version, the
            deployment will fail. If not provided, deployment procedure will
            not check the existing deployment's version.
        num_replicas (Optional[int]): The number of processes to start up that
            will handle requests to this deployment. Defaults to 1.
        init_args (Optional[Tuple]): Positional args to be passed to the class
            constructor when starting up deployment replicas. These can also be
            passed when you call `.deploy()` on the returned Deployment.
        init_kwargs (Optional[Dict]): Keyword args to be passed to the class
            constructor when starting up deployment replicas. These can also be
            passed when you call `.deploy()` on the returned Deployment.
        route_prefix (Optional[str]): Requests to paths under this HTTP path
            prefix will be routed to this deployment. Defaults to '/{name}'.
            When set to 'None', no HTTP endpoint will be created.
            Routing is done based on longest-prefix match, so if you have
            deployment A with a prefix of '/a' and deployment B with a prefix
            of '/a/b', requests to '/a', '/a/', and '/a/c' go to A and requests
            to '/a/b', '/a/b/', and '/a/b/c' go to B. Routes must not end with
            a '/' unless they're the root (just '/'), which acts as a
            catch-all.
        ray_actor_options (dict): Options to be passed to the Ray actor
            constructor such as resource requirements.
        user_config (Optional[Any]): [experimental] Config to pass to the
            reconfigure method of the deployment. This can be updated
            dynamically without changing the version of the deployment and
            restarting its replicas. The user_config needs to be hashable to
            keep track of updates, so it must only contain hashable types, or
            hashable types nested in lists and dictionaries.
        max_concurrent_queries (Optional[int]): The maximum number of queries
            that will be sent to a replica of this deployment without receiving
            a response. Defaults to 100.

    Example:

    >>> @serve.deployment(name="deployment1", version="v1")
        class MyDeployment:
            pass

    >>> MyDeployment.deploy(*init_args)
    >>> MyDeployment.options(num_replicas=2, init_args=init_args).deploy()

    Returns:
        Deployment
    """

    if num_replicas is not None \
            and _autoscaling_config is not None:
        raise ValueError("Manually setting num_replicas is not allowed when "
                         "_autoscaling_config is provided.")

    config = DeploymentConfig()
    if num_replicas is not None:
        config.num_replicas = num_replicas

    if user_config is not None:
        config.user_config = user_config

    if max_concurrent_queries is not None:
        config.max_concurrent_queries = max_concurrent_queries

    if _autoscaling_config is not None:
        config.autoscaling_config = _autoscaling_config

    if _graceful_shutdown_wait_loop_s is not None:
        config.graceful_shutdown_wait_loop_s = _graceful_shutdown_wait_loop_s

    if _graceful_shutdown_timeout_s is not None:
        config.graceful_shutdown_timeout_s = _graceful_shutdown_timeout_s

    def decorator(_func_or_class):
        return Deployment(
            _func_or_class,
            name if name is not None else _func_or_class.__name__,
            config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_func_or_class) if callable(_func_or_class) else decorator


@PublicAPI
def get_deployment(name: str) -> Deployment:
    """Dynamically fetch a handle to a Deployment object.

    This can be used to update and redeploy a deployment without access to
    the original definition.

    Example:

    >>> MyDeployment = serve.get_deployment("name")
    >>> MyDeployment.options(num_replicas=10).deploy()

    Args:
        name(str): name of the deployment. This must have already been
        deployed.

    Returns:
        Deployment
    """
    try:
        deployment_info, route_prefix = _get_global_client(
        ).get_deployment_info(name)
    except KeyError:
        raise KeyError(f"Deployment {name} was not found. "
                       "Did you call Deployment.deploy()?")
    return Deployment(
        cloudpickle.loads(
            deployment_info.replica_config.serialized_deployment_def),
        name,
        deployment_info.deployment_config,
        version=deployment_info.version,
        init_args=deployment_info.replica_config.init_args,
        init_kwargs=deployment_info.replica_config.init_kwargs,
        route_prefix=route_prefix,
        ray_actor_options=deployment_info.replica_config.ray_actor_options,
        _internal=True,
    )


@PublicAPI
def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = _get_global_client().list_deployments()

    deployments = {}
    for name, (deployment_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            cloudpickle.loads(
                deployment_info.replica_config.serialized_deployment_def),
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
