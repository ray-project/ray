import asyncio
import atexit
import time
from functools import wraps
import os
from uuid import UUID
import threading
from typing import Any, Callable, Coroutine, Dict, List, Optional, Type, Union
from dataclasses import dataclass
from warnings import warn

import ray
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 SERVE_CONTROLLER_NAME, HTTP_PROXY_TIMEOUT)
from ray.serve.controller import ServeController, BackendTag, ReplicaTag
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.utils import (block_until_http_ready, format_actor_name,
                             get_random_letters, logger, get_conda_env_dir,
                             get_current_node_resource_key)
from ray.serve.exceptions import RayServeException
from ray.serve.config import (BackendConfig, ReplicaConfig, BackendMetadata,
                              HTTPOptions)
from ray.serve.env import CondaEnv
from ray.serve.router import RequestMetadata, Router
from ray.actor import ActorHandle

_INTERNAL_REPLICA_CONTEXT = None
global_async_loop = None


@dataclass
class InternalReplicaContext:
    """Stores data for Serve API calls from within the user's backend code."""
    backend_tag: BackendTag
    replica_tag: ReplicaTag
    controller_name: str


def create_or_get_async_loop_in_thread():
    global global_async_loop
    if global_async_loop is None:
        global_async_loop = asyncio.new_event_loop()
        thread = threading.Thread(
            daemon=True,
            target=global_async_loop.run_forever,
        )
        thread.start()
    return global_async_loop


def _set_internal_replica_context(backend_tag, replica_tag, controller_name):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = InternalReplicaContext(
        backend_tag, replica_tag, controller_name)


def _ensure_connected(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, **kwargs):
        if self._shutdown:
            raise RayServeException("Client has already been shut down.")
        return f(self, *args, **kwargs)

    return check


class ThreadProxiedRouter:
    def __init__(self, controller_handle, sync: bool):
        self.router = Router(controller_handle)

        if sync:
            self.async_loop = create_or_get_async_loop_in_thread()
            asyncio.run_coroutine_threadsafe(
                self.router.setup_in_async_loop(),
                self.async_loop,
            )
        else:
            self.async_loop = asyncio.get_event_loop()
            self.async_loop.create_task(self.router.setup_in_async_loop())

    def _remote(self, endpoint_name, handle_options, request_data,
                kwargs) -> Coroutine:
        request_metadata = RequestMetadata(
            get_random_letters(10),  # Used for debugging.
            endpoint_name,
            call_method=handle_options.method_name,
            shard_key=handle_options.shard_key,
            http_method=handle_options.http_method,
            http_headers=handle_options.http_headers,
        )
        coro = self.router.assign_request(request_metadata, request_data,
                                          **kwargs)
        return coro


class Client:
    def __init__(self,
                 controller: ActorHandle,
                 controller_name: str,
                 detached: bool = False):
        self._controller = controller
        self._controller_name = controller_name
        self._detached = detached
        self._shutdown = False
        self._http_config = ray.get(controller.get_http_config.remote())

        self._sync_proxied_router = None
        self._async_proxied_router = None

        # NOTE(edoakes): Need this because the shutdown order isn't guaranteed
        # when the interpreter is exiting so we can't rely on __del__ (it
        # throws a nasty stacktrace).
        if not self._detached:

            def shutdown_serve_client():
                self.shutdown()

            atexit.register(shutdown_serve_client)

    def _get_proxied_router(self, sync: bool):
        if sync:
            if self._sync_proxied_router is None:
                self._sync_proxied_router = ThreadProxiedRouter(
                    self._controller, sync=True)
            return self._sync_proxied_router
        else:
            if self._async_proxied_router is None:
                self._async_proxied_router = ThreadProxiedRouter(
                    self._controller, sync=False)
            return self._async_proxied_router

    def __del__(self):
        if not self._detached:
            logger.debug("Shutting down Ray Serve because client went out of "
                         "scope. To prevent this, either keep a reference to "
                         "the client or use serve.start(detached=True).")
            self.shutdown()

    def __reduce__(self):
        raise RayServeException(
            ("Ray Serve client cannot be serialized. Please use "
             "serve.connect() to get a client from within a backend."))

    def shutdown(self) -> None:
        """Completely shut down the connected Serve instance.

        Shuts down all processes and deletes all state associated with the
        instance.
        """
        if (not self._shutdown) and ray.is_initialized():
            ray.get(self._controller.shutdown.remote())
            ray.kill(self._controller, no_restart=True)

            # Wait for the named actor entry gets removed as well.
            started = time.time()
            while True:
                try:
                    ray.get_actor(self._controller_name)
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

    @_ensure_connected
    def _wait_for_goal(self, result_object_id: ray.ObjectRef) -> bool:
        goal_id: Optional[UUID] = ray.get(result_object_id)
        if goal_id is not None:
            ray.get(self._controller.wait_for_goal.remote(goal_id))
            logger.debug(f"Goal {goal_id} completed.")

    @_ensure_connected
    def create_endpoint(self,
                        endpoint_name: str,
                        *,
                        backend: str = None,
                        route: Optional[str] = None,
                        methods: List[str] = ["GET"]) -> None:
        """Create a service endpoint given route_expression.

        Args:
            endpoint_name (str): A name to associate to with the endpoint.
            backend (str, required): The backend that will serve requests to
                this endpoint. To change this or split traffic among backends,
                use `serve.set_traffic`.
            route (str, optional): A string begin with "/". HTTP server will
                use the string to match the path.
            methods(List[str], optional): The HTTP methods that are valid for
                this endpoint.
        """
        if backend is None:
            raise TypeError("backend must be specified when creating "
                            "an endpoint.")
        elif not isinstance(backend, str):
            raise TypeError("backend must be a string, got {}.".format(
                type(backend)))

        if route is not None:
            if not isinstance(route, str) or not route.startswith("/"):
                raise TypeError("route must be a string starting with '/'.")

        if not isinstance(methods, list):
            raise TypeError(
                "methods must be a list of strings, but got type {}".format(
                    type(methods)))

        endpoints = self.list_endpoints()
        if endpoint_name in endpoints:
            methods_old = endpoints[endpoint_name]["methods"]
            route_old = endpoints[endpoint_name]["route"]
            if sorted(methods_old) == sorted(methods) and route_old == route:
                raise ValueError(
                    "Route '{}' is already registered to endpoint '{}' "
                    "with methods '{}'.  To set the backend for this "
                    "endpoint, please use serve.set_traffic().".format(
                        route, endpoint_name, methods))

        upper_methods = []
        for method in methods:
            if not isinstance(method, str):
                raise TypeError(
                    "methods must be a list of strings, but contained "
                    "an element of type {}".format(type(method)))
            upper_methods.append(method.upper())

        self._wait_for_goal(
            self._controller.create_endpoint.remote(
                endpoint_name, {backend: 1.0}, route, upper_methods))

        # Block until the route table has been propagated to all HTTP proxies.
        if route is not None:

            def check_ready(http_response):
                return route in http_response.json()

            futures = []
            for node_id in ray.state.node_ids():
                future = block_until_http_ready.options(
                    num_cpus=0, resources={
                        node_id: 0.01
                    }).remote(
                        "http://{}:{}/-/routes".format(self._http_config.host,
                                                       self._http_config.port),
                        check_ready=check_ready,
                        timeout=HTTP_PROXY_TIMEOUT)
                futures.append(future)
            try:
                ray.get(futures)
            except ray.exceptions.RayTaskError:
                raise TimeoutError("Route not available at HTTP proxies "
                                   "after {HTTP_PROXY_TIMEOUT}s.")

    @_ensure_connected
    def delete_endpoint(self, endpoint: str) -> None:
        """Delete the given endpoint.

        Does not delete any associated backends.
        """
        ray.get(self._controller.delete_endpoint.remote(endpoint))

    @_ensure_connected
    def list_endpoints(self) -> Dict[str, Dict[str, Any]]:
        """Returns a dictionary of all registered endpoints.

        The dictionary keys are endpoint names and values are dictionaries
        of the form: {"methods": List[str], "traffic": Dict[str, float]}.
        """
        return ray.get(self._controller.get_all_endpoints.remote())

    @_ensure_connected
    def update_backend_config(
            self, backend_tag: str,
            config_options: Union[BackendConfig, Dict[str, Any]]) -> None:
        """Update a backend configuration for a backend tag.

        Keys not specified in the passed will be left unchanged.

        Args:
            backend_tag(str): A registered backend.
            config_options(dict, serve.BackendConfig): Backend config options
                to update. Either a BackendConfig object or a dict mapping
                strings to values for the following supported options:
                - "num_replicas": number of processes to start up that
                will handle requests to this backend.
                - "max_batch_size": the maximum number of requests that will
                be processed in one batch by this backend.
                - "batch_wait_timeout": time in seconds that backend replicas
                will wait for a full batch of requests before
                processing a partial batch.
                - "max_concurrent_queries": the maximum number of queries
                that will be sent to a replica of this backend
                without receiving a response.
                - "user_config" (experimental): Arguments to pass to the
                reconfigure method of the backend. The reconfigure method is
                called if "user_config" is not None.
        """

        if not isinstance(config_options, (BackendConfig, dict)):
            raise TypeError(
                "config_options must be a BackendConfig or dictionary.")
        if isinstance(config_options, dict):
            config_options = BackendConfig.parse_obj(config_options)
        self._wait_for_goal(
            self._controller.update_backend_config.remote(
                backend_tag, config_options))

    @_ensure_connected
    def get_backend_config(self, backend_tag: str) -> BackendConfig:
        """Get the backend configuration for a backend tag.

        Args:
            backend_tag(str): A registered backend.
        """
        return ray.get(self._controller.get_backend_config.remote(backend_tag))

    @_ensure_connected
    def create_backend(
            self,
            backend_tag: str,
            func_or_class: Union[Callable, Type[Callable]],
            *actor_init_args: Any,
            ray_actor_options: Optional[Dict] = None,
            config: Optional[Union[BackendConfig, Dict[str, Any]]] = None,
            env: Optional[CondaEnv] = None) -> None:
        """Create a backend with the provided tag.

        The backend will serve requests with func_or_class.

        Args:
            backend_tag (str): a unique tag assign to identify this backend.
            func_or_class (callable, class): a function or a class implementing
                __call__, returning a JSON-serializable object or a
                Starlette Response object.
            *actor_init_args (optional): the arguments to pass to the class
                initialization method.
            ray_actor_options (optional): options to be passed into the
                @ray.remote decorator for the backend actor.
            config (dict, serve.BackendConfig, optional): configuration options
                for this backend. Either a BackendConfig, or a dictionary
                mapping strings to values for the following supported options:
                - "num_replicas": number of processes to start up that
                will handle requests to this backend.
                - "max_batch_size": the maximum number of requests that will
                be processed in one batch by this backend.
                - "batch_wait_timeout": time in seconds that backend replicas
                will wait for a full batch of requests before processing a
                partial batch.
                - "max_concurrent_queries": the maximum number of queries that
                will be sent to a replica of this backend without receiving a
                response.
                - "user_config" (experimental): Arguments to pass to the
                reconfigure method of the backend. The reconfigure method is
                called if "user_config" is not None.
            env (serve.CondaEnv, optional): conda environment to run this
                backend in.  Requires the caller to be running in an activated
                conda environment (not necessarily ``env``), and requires
                ``env`` to be an existing conda environment on all nodes.  If
                ``env`` is not provided but conda is activated, the backend
                will run in the conda environment of the caller.
        """
        if backend_tag in self.list_backends().keys():
            raise ValueError(
                "Cannot create backend. "
                "Backend '{}' is already registered.".format(backend_tag))

        if config is None:
            config = {}
        if ray_actor_options is None:
            ray_actor_options = {}
        if env is None:
            # If conda is activated, default to conda env of this process.
            if os.environ.get("CONDA_PREFIX"):
                if "override_environment_variables" not in ray_actor_options:
                    ray_actor_options["override_environment_variables"] = {}
                ray_actor_options["override_environment_variables"].update({
                    "PYTHONHOME": os.environ.get("CONDA_PREFIX")
                })
        else:
            conda_env_dir = get_conda_env_dir(env.name)
            ray_actor_options.update(
                override_environment_variables={"PYTHONHOME": conda_env_dir})
        replica_config = ReplicaConfig(
            func_or_class,
            *actor_init_args,
            ray_actor_options=ray_actor_options)
        metadata = BackendMetadata(
            accepts_batches=replica_config.accepts_batches,
            is_blocking=replica_config.is_blocking)

        if isinstance(config, dict):
            backend_config = BackendConfig.parse_obj({
                **config, "internal_metadata": metadata
            })
        elif isinstance(config, BackendConfig):
            backend_config = config.copy(
                update={"internal_metadata": metadata})
        else:
            raise TypeError("config must be a BackendConfig or a dictionary.")

        backend_config._validate_complete()
        self._wait_for_goal(
            self._controller.create_backend.remote(backend_tag, backend_config,
                                                   replica_config))

    @_ensure_connected
    def list_backends(self) -> Dict[str, BackendConfig]:
        """Returns a dictionary of all registered backends.

        Dictionary maps backend tags to backend config objects.
        """
        return ray.get(self._controller.get_all_backends.remote())

    @_ensure_connected
    def delete_backend(self, backend_tag: str, force: bool = False) -> None:
        """Delete the given backend.

        The backend must not currently be used by any endpoints.

        Args:
            backend_tag (str): The backend tag to be deleted.
            force (bool): Whether or not to force the deletion, without waiting
              for graceful shutdown. Default to false.
        """
        self._wait_for_goal(
            self._controller.delete_backend.remote(backend_tag, force))

    @_ensure_connected
    def set_traffic(self, endpoint_name: str,
                    traffic_policy_dictionary: Dict[str, float]) -> None:
        """Associate a service endpoint with traffic policy.

        Example:

        >>> serve.set_traffic("service-name", {
            "backend:v1": 0.5,
            "backend:v2": 0.5
        })

        Args:
            endpoint_name (str): A registered service endpoint.
            traffic_policy_dictionary (dict): a dictionary maps backend names
                to their traffic weights. The weights must sum to 1.
        """
        ray.get(
            self._controller.set_traffic.remote(endpoint_name,
                                                traffic_policy_dictionary))

    @_ensure_connected
    def shadow_traffic(self, endpoint_name: str, backend_tag: str,
                       proportion: float) -> None:
        """Shadow traffic from an endpoint to a backend.

        The specified proportion of requests will be duplicated and sent to the
        backend. Responses of the duplicated traffic will be ignored.
        The backend must not already be in use.

        To stop shadowing traffic to a backend, call `shadow_traffic` with
        proportion equal to 0.

        Args:
            endpoint_name (str): A registered service endpoint.
            backend_tag (str): A registered backend.
            proportion (float): The proportion of traffic from 0 to 1.
        """

        if not isinstance(proportion,
                          (float, int)) or not 0 <= proportion <= 1:
            raise TypeError("proportion must be a float from 0 to 1.")

        ray.get(
            self._controller.shadow_traffic.remote(endpoint_name, backend_tag,
                                                   proportion))

    @_ensure_connected
    def get_handle(
            self,
            endpoint_name: str,
            missing_ok: Optional[bool] = False,
            sync: bool = True) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Retrieve RayServeHandle for service endpoint to invoke it from Python.

        Args:
            endpoint_name (str): A registered service endpoint.
            missing_ok (bool): If true, then Serve won't check the endpoint is
                registered. False by default.
            sync (bool): If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return a ServeHandle
                that's only usable in asyncio loop.

        Returns:
            RayServeHandle
        """
        if not missing_ok and endpoint_name not in ray.get(
                self._controller.get_all_endpoints.remote()):
            raise KeyError(f"Endpoint '{endpoint_name}' does not exist.")

        if asyncio.get_event_loop().is_running() and sync:
            logger.warning(
                "You are retrieving a sync handle inside an asyncio loop. "
                "Try getting client.get_handle(.., sync=False) to get better "
                "performance. Learn more at https://docs.ray.io/en/master/"
                "serve/advanced.html#sync-and-async-handles")

        if not asyncio.get_event_loop().is_running() and not sync:
            logger.warning(
                "You are retrieving an async handle outside an asyncio loop. "
                "You should make sure client.get_handle is called inside a "
                "running event loop. Or call client.get_handle(.., sync=True) "
                "to create sync handle. Learn more at https://docs.ray.io/en/"
                "master/serve/advanced.html#sync-and-async-handles")

        if sync:
            handle = RayServeSyncHandle(
                self._get_proxied_router(sync=sync), endpoint_name)
        else:
            handle = RayServeHandle(
                self._get_proxied_router(sync=sync), endpoint_name)
        return handle


def start(
        detached: bool = False,
        http_host: Optional[str] = DEFAULT_HTTP_HOST,
        http_port: int = DEFAULT_HTTP_PORT,
        http_middlewares: List[Any] = [],
        http_options: Optional[Union[dict, HTTPOptions]] = None,
) -> Client:
    """Initialize a serve instance.

    By default, the instance will be scoped to the lifetime of the returned
    Client object (or when the script exits). If detached is set to True, the
    instance will instead persist until client.shutdown() is called and clients
    to it can be connected using serve.connect(). This is only relevant if
    connecting to a long-running Ray cluster (e.g., with address="auto").

    Args:
        detached (bool): Whether not the instance should be detached from this
          script.
        http_host (Optional[str]): Deprecated, use http_options instead.
        http_port (int): Deprecated, use http_options instead.
        http_middlewares (list): Deprecated, use http_options instead.
        http_options (Optional[Dict, serve.HTTPOptions]): Configuration options
          for HTTP proxy. You can pass in a dictionary or HTTPOptions object
          with fields:

            - host(str, None): Host for HTTP servers to listen on. Defaults to
              "127.0.0.1". To expose Serve publicly, you probably want to set
              this to "0.0.0.0".
            - port(int): Port for HTTP server. Defaults to 8000.
            - middlewares(list): A list of Starlette middlewares that will be
              applied to the HTTP servers in the cluster.
            - location(str, serve.config.DeploymentMode): The deployment
              location of HTTP servers:

                - "HeadOnly": start one HTTP server on the head node. Serve
                  assumes the head node is the node you executed serve.start
                  on. This is the default.
                - "EveryNode": start one HTTP server per node.
                - "NoServer" or None: disable HTTP server.
    """
    if ((http_host != DEFAULT_HTTP_HOST) or (http_port != DEFAULT_HTTP_PORT)
            or (len(http_middlewares) != 0)):
        if http_options is not None:
            raise ValueError(
                "You cannot specify both `http_options` and any of the "
                "`http_host`, `http_port`, and `http_middlewares` arguments. "
                "`http_options` is preferred.")
        else:
            warn(
                "`http_host`, `http_port`, `http_middlewares` are deprecated. "
                "Please use serve.start(http_options={'host': ..., "
                "'port': ..., middlewares': ...}) instead.",
                DeprecationWarning,
            )

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init()

    # Try to get serve controller if it exists
    if detached:
        controller_name = SERVE_CONTROLLER_NAME
        try:
            ray.get_actor(controller_name)
            raise RayServeException("Called serve.start(detached=True) but a "
                                    "detached instance is already running. "
                                    "Please use serve.connect() to connect to "
                                    "the running instance instead.")
        except ValueError:
            pass
    else:
        controller_name = format_actor_name(SERVE_CONTROLLER_NAME,
                                            get_random_letters())

    if isinstance(http_options, dict):
        http_options = HTTPOptions.parse_obj(http_options)
    if http_options is None:
        http_options = HTTPOptions(
            host=http_host, port=http_port, middlewares=http_middlewares)

    controller = ServeController.options(
        name=controller_name,
        lifetime="detached" if detached else None,
        max_restarts=-1,
        max_task_retries=-1,
        # Pin Serve controller on the head node.
        resources={
            get_current_node_resource_key(): 0.01
        },
    ).remote(
        controller_name,
        http_options,
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

    return Client(controller, controller_name, detached=detached)


def connect() -> Client:
    """Connect to an existing Serve instance on this Ray cluster.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a backend, will connect to the same Serve instance
    that the backend is running in.
    """

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init()

    # When running inside of a backend, _INTERNAL_REPLICA_CONTEXT is set to
    # ensure that the correct instance is connected to.
    if _INTERNAL_REPLICA_CONTEXT is None:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = _INTERNAL_REPLICA_CONTEXT.controller_name

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(controller_name)
    except ValueError:
        raise RayServeException("Called `serve.connect()` but there is no "
                                "instance running on this Ray cluster. Please "
                                "call `serve.start(detached=True) to start "
                                "one.")

    return Client(controller, controller_name, detached=True)


def get_current_backend_tag() -> BackendTag:
    """When called from within a backend, return its backend tag.

    Raises:
        RayServeException if not called from within a Ray Serve backend.
    """
    if _INTERNAL_REPLICA_CONTEXT is None:
        raise RayServeException("`serve.get_current_backend_tag()`"
                                "may only be called from within a"
                                "Ray Serve backend.")
    else:
        return _INTERNAL_REPLICA_CONTEXT.backend_tag


def get_current_replica_tag() -> ReplicaTag:
    """When called from within a backend, return its replica tag.

    A replica tag uniquely identifies a single replica (a process)
    for a Ray Serve backend.

    Raises:
        RayServeException if not called from within a Ray Serve backend.
    """
    if _INTERNAL_REPLICA_CONTEXT is None:
        raise RayServeException("`serve.get_current_replica_tag()`"
                                "may only be called from within a"
                                "Ray Serve backend.")
    else:
        return _INTERNAL_REPLICA_CONTEXT.replica_tag


def accept_batch(f: Callable) -> Callable:
    """Annotation to mark that a serving function accepts batches of requests.

    In order to accept batches of requests as input, the implementation must
    handle a list of requests being passed in rather than just a single
    request.

    This must be set on any backend implementation that will have
    max_batch_size set to greater than 1.

    Example:

    >>> @serve.accept_batch
        def serving_func(requests):
            assert isinstance(requests, list)
            ...

    >>> class ServingActor:
            @serve.accept_batch
            def __call__(self, requests):
                assert isinstance(requests, list)
    """
    f._serve_accept_batch = True
    return f
