from abc import ABC
import asyncio
import atexit
import inspect
import os
import threading
import time
from dataclasses import dataclass
from functools import wraps
from typing import (TYPE_CHECKING, Any, Callable, Coroutine, Dict, List,
                    Optional, Type, Union)
from uuid import UUID
from warnings import warn

from ray.actor import ActorHandle
from ray.serve.common import EndpointTag
from ray.serve.config import (BackendConfig, BackendMetadata, HTTPOptions,
                              ReplicaConfig)
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 HTTP_PROXY_TIMEOUT, SERVE_CONTROLLER_NAME)
from ray.serve.controller import BackendTag, ReplicaTag, ServeController
from ray.serve.exceptions import RayServeException
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.router import RequestMetadata, Router
from ray.serve.utils import (block_until_http_ready, format_actor_name,
                             get_current_node_resource_key, get_random_letters,
                             logger, register_custom_serializers)

import ray

if TYPE_CHECKING:
    from fastapi import APIRouter, FastAPI  # noqa: F401

_INTERNAL_REPLICA_CONTEXT = None
_global_async_loop = None
_global_client = None


def _get_global_client():
    if _global_client is not None:
        return _global_client

    return connect()


def _set_global_client(client):
    global _global_client
    _global_client = client


@dataclass
class ReplicaContext:
    """Stores data for Serve API calls from within the user's backend code."""
    backend_tag: BackendTag
    replica_tag: ReplicaTag
    _internal_controller_name: str


def create_or_get_async_loop_in_thread():
    global _global_async_loop
    if _global_async_loop is None:
        _global_async_loop = asyncio.new_event_loop()
        thread = threading.Thread(
            daemon=True,
            target=_global_async_loop.run_forever,
        )
        thread.start()
    return _global_async_loop


def _set_internal_replica_context(backend_tag, replica_tag, controller_name):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(backend_tag, replica_tag,
                                               controller_name)


def _ensure_connected(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, _internal=False, **kwargs):
        if self._shutdown:
            raise RayServeException("Client has already been shut down.")
        if not _internal:
            logger.warning(
                "The client-based API is being deprecated in favor of global "
                "API calls (e.g., `serve.create_backend()`). Please replace "
                "all instances of `client.api_call()` with "
                "`serve.api_call()`.")
        return f(self, *args, **kwargs)

    return check


class ThreadProxiedRouter:
    def __init__(self, controller_handle, sync: bool,
                 endpoint_tag: EndpointTag):
        self.controller_handle = controller_handle
        self.sync = sync
        self.endpoint_tag = endpoint_tag
        if sync:
            self._async_loop = create_or_get_async_loop_in_thread()
        else:
            self._async_loop = asyncio.get_event_loop()
        self.router = Router(controller_handle, endpoint_tag, self._async_loop)

    @property
    def async_loop(self):
        # called by handles
        return self._async_loop

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

    def __reduce__(self):
        deserializer = ThreadProxiedRouter
        serialized_data = (
            self.controller_handle,
            self.sync,
            self.endpoint_tag,
        )
        return deserializer, serialized_data


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

        # TODO(simon): remove this when dropping router object and making
        # ServeHandle sync only.
        self._cached_routers = dict()

        # NOTE(edoakes): Need this because the shutdown order isn't guaranteed
        # when the interpreter is exiting so we can't rely on __del__ (it
        # throws a nasty stacktrace).
        if not self._detached:

            def shutdown_serve_client():
                self.shutdown()

            atexit.register(shutdown_serve_client)

    def _get_proxied_router(self, sync: bool, endpoint: EndpointTag):
        key = (sync, endpoint)
        if key not in self._cached_routers:
            self._cached_routers[key] = ThreadProxiedRouter(
                self._controller,
                sync,
                endpoint,
            )
        return self._cached_routers[key]

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

        endpoints = self.list_endpoints(_internal=True)
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
            backend_def: Union[Callable, Type[Callable], str],
            *init_args: Any,
            ray_actor_options: Optional[Dict] = None,
            config: Optional[Union[BackendConfig, Dict[str, Any]]] = None
    ) -> None:
        """Create a backend with the provided tag.

        Args:
            backend_tag (str): a unique tag assign to identify this backend.
            backend_def (callable, class, str): a function or class
                implementing __call__ and returning a JSON-serializable object
                or a Starlette Response object. A string import path can also
                be provided (e.g., "my_module.MyClass"), in which case the
                underlying function or class will be imported dynamically in
                the worker replicas.
            *init_args (optional): the arguments to pass to the class
                initialization method. Not valid if backend_def is a function.
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
        """
        if backend_tag in self.list_backends(_internal=True).keys():
            raise ValueError(
                "Cannot create backend. "
                "Backend '{}' is already registered.".format(backend_tag))

        if config is None:
            config = {}
        if ray_actor_options is None:
            ray_actor_options = {}

        # If conda is activated and a conda env is not specified in runtime_env
        # in ray_actor_options, default to conda env of this process (client).
        # Without this code, the backend would run in the controller's conda
        # env, which is likely different from that of the client.
        # If using Ray client, skip this convenience feature because the local
        # client env doesn't create the Ray cluster (so the client env is
        # likely not present on the cluster.)
        if not ray.util.client.ray.is_connected():
            if ray_actor_options.get("runtime_env") is None:
                ray_actor_options["runtime_env"] = {}
            if ray_actor_options["runtime_env"].get("conda") is None:
                current_env = os.environ.get("CONDA_DEFAULT_ENV")
                if current_env is not None and current_env != "":
                    ray_actor_options["runtime_env"]["conda"] = current_env

        replica_config = ReplicaConfig(
            backend_def, *init_args, ray_actor_options=ray_actor_options)
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
    def deploy(self,
               name: str,
               backend_def: Union[Callable, Type[Callable], str],
               *init_args: Any,
               ray_actor_options: Optional[Dict] = None,
               config: Optional[Union[BackendConfig, Dict[str, Any]]] = None,
               version: Optional[str] = None) -> None:
        if config is None:
            config = {}
        if ray_actor_options is None:
            ray_actor_options = {}

        # If conda is activated and a conda env is not specified in runtime_env
        # in ray_actor_options, default to conda env of this process (client).
        # Without this code, the backend would run in the controller's conda
        # env, which is likely different from that of the client.
        # If using Ray client, skip this convenience feature because the local
        # client env doesn't create the Ray cluster (so the client env is
        # likely not present on the cluster.)
        if not ray.util.client.ray.is_connected():
            if ray_actor_options.get("runtime_env") is None:
                ray_actor_options["runtime_env"] = {}
            if ray_actor_options["runtime_env"].get("conda") is None:
                current_env = os.environ.get("CONDA_DEFAULT_ENV")
                if current_env is not None and current_env != "":
                    ray_actor_options["runtime_env"]["conda"] = current_env

        replica_config = ReplicaConfig(
            backend_def, *init_args, ray_actor_options=ray_actor_options)
        metadata = BackendMetadata(
            accepts_batches=replica_config.accepts_batches,
            is_blocking=replica_config.is_blocking,
            is_asgi_app=replica_config.is_asgi_app,
            path_prefix=replica_config.path_prefix,
        )

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
            self._controller.deploy.remote(name, backend_config,
                                           replica_config, version))

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

        # NOTE(simon): this extra layer of router seems unnecessary
        # BUT it's needed still because of the shared asyncio thread.
        router = self._get_proxied_router(sync=sync, endpoint=endpoint_name)
        if sync:
            handle = RayServeSyncHandle(router, endpoint_name)
        else:
            handle = RayServeHandle(router, endpoint_name)
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

    register_custom_serializers()

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

    client = Client(controller, controller_name, detached=detached)
    _set_global_client(client)
    return client


def connect() -> Client:
    """Connect to an existing Serve instance on this Ray cluster.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a backend, this will connect to the same Serve
    instance that the backend is running in.
    """

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init()

    register_custom_serializers()

    # When running inside of a backend, _INTERNAL_REPLICA_CONTEXT is set to
    # ensure that the correct instance is connected to.
    if _INTERNAL_REPLICA_CONTEXT is None:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = _INTERNAL_REPLICA_CONTEXT._internal_controller_name

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(controller_name)
    except ValueError:
        raise RayServeException("Called `serve.connect()` but there is no "
                                "instance running on this Ray cluster. Please "
                                "call `serve.start(detached=True) to start "
                                "one.")

    client = Client(controller, controller_name, detached=True)
    _set_global_client(client)
    return client


def shutdown() -> None:
    """Completely shut down the connected Serve instance.

    Shuts down all processes and deletes all state associated with the
    instance.
    """
    if _global_client is None:
        return

    _get_global_client().shutdown()
    _set_global_client(None)


def create_endpoint(endpoint_name: str,
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
    return _get_global_client().create_endpoint(
        endpoint_name,
        backend=backend,
        route=route,
        methods=methods,
        _internal=True)


def delete_endpoint(endpoint: str) -> None:
    """Delete the given endpoint.

    Does not delete any associated backends.
    """
    return _get_global_client().delete_endpoint(endpoint, _internal=True)


def list_endpoints() -> Dict[str, Dict[str, Any]]:
    """Returns a dictionary of all registered endpoints.

    The dictionary keys are endpoint names and values are dictionaries
    of the form: {"methods": List[str], "traffic": Dict[str, float]}.
    """
    return _get_global_client().list_endpoints(_internal=True)


def update_backend_config(
        backend_tag: str,
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
    return _get_global_client().update_backend_config(
        backend_tag, config_options, _internal=True)


def get_backend_config(backend_tag: str) -> BackendConfig:
    """Get the backend configuration for a backend tag.

    Args:
        backend_tag(str): A registered backend.
    """
    return _get_global_client().get_backend_config(backend_tag, _internal=True)


def create_backend(
        backend_tag: str,
        backend_def: Union[Callable, Type[Callable], str],
        *init_args: Any,
        ray_actor_options: Optional[Dict] = None,
        config: Optional[Union[BackendConfig, Dict[str, Any]]] = None) -> None:
    """Create a backend with the provided tag.

    Args:
        backend_tag (str): a unique tag assign to identify this backend.
        backend_def (callable, class, str): a function or class
            implementing __call__ and returning a JSON-serializable object
            or a Starlette Response object. A string import path can also
            be provided (e.g., "my_module.MyClass"), in which case the
            underlying function or class will be imported dynamically in
            the worker replicas.
        *init_args (optional): the arguments to pass to the class
            initialization method. Not valid if backend_def is a function.
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
    """
    return _get_global_client().create_backend(
        backend_tag,
        backend_def,
        *init_args,
        ray_actor_options=ray_actor_options,
        config=config,
        _internal=True)


def list_backends() -> Dict[str, BackendConfig]:
    """Returns a dictionary of all registered backends.

    Dictionary maps backend tags to backend config objects.
    """
    return _get_global_client().list_backends(_internal=True)


def delete_backend(backend_tag: str, force: bool = False) -> None:
    """Delete the given backend.

    The backend must not currently be used by any endpoints.

    Args:
        backend_tag (str): The backend tag to be deleted.
        force (bool): Whether or not to force the deletion, without waiting
          for graceful shutdown. Default to false.
    """
    return _get_global_client().delete_backend(
        backend_tag, force=force, _internal=True)


def set_traffic(endpoint_name: str,
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
    return _get_global_client().set_traffic(
        endpoint_name, traffic_policy_dictionary, _internal=True)


def shadow_traffic(endpoint_name: str, backend_tag: str,
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
    return _get_global_client().shadow_traffic(
        endpoint_name, backend_tag, proportion, _internal=True)


def get_handle(endpoint_name: str,
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
    return _get_global_client().get_handle(
        endpoint_name, missing_ok=missing_ok, sync=sync, _internal=True)


def get_replica_context() -> ReplicaContext:
    """When called from a backend, returns the backend tag and replica tag.

    When not called from a backend, returns None.

    A replica tag uniquely identifies a single replica for a Ray Serve
    backend at runtime.  Replica tags are of the form
    `<backend tag>#<random letters>`.

    Raises:
        RayServeException: if not called from within a Ray Serve backend
    Example:
        >>> serve.get_replica_context().backend_tag # my_backend
        >>> serve.get_replica_context().replica_tag # my_backend#krcwoa
    """
    if _INTERNAL_REPLICA_CONTEXT is None:
        raise RayServeException("`serve.get_replica_context()` "
                                "may only be called from within a "
                                "Ray Serve backend.")
    return _INTERNAL_REPLICA_CONTEXT


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


def ingress(
        app: Union["FastAPI", "APIRouter", None] = None,
        path_prefix: Optional[str] = None,
):
    """Mark a FastAPI application ingress for Serve.

    Args:
        app(FastAPI,APIRouter,None): the app or router object serve as ingress
            for this backend.
        path_prefix(str,None): The path prefix for the ingress. For example,
            `/api`. Serve uses the deployment name as path_prefix by default.

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

        if app is not None:
            cls._serve_asgi_app = app
        if path_prefix is not None:
            cls._serve_path_prefix = path_prefix

        return cls

    return decorator


class ServeDeployment(ABC):
    @classmethod
    def deploy(self, *init_args) -> None:
        """Deploy this deployment.

        Args:
            *init_args (optional): the arguments to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        # TODO(edoakes): how to avoid copy-pasting the docstrings here?
        raise NotImplementedError()

    @classmethod
    def delete(self) -> None:
        """Delete this deployment."""
        raise NotImplementedError()

    @classmethod
    def get_handle(self, sync: Optional[bool] = True
                   ) -> Union[RayServeHandle, RayServeSyncHandle]:
        raise NotImplementedError()


def make_deployment_cls(
        backend_def: Callable,
        name: str,
        version: str,
        ray_actor_options: Optional[Dict] = None,
        config: Optional[BackendConfig] = None) -> ServeDeployment:
    class Deployment(ServeDeployment):
        _backend_def = backend_def
        _name = name
        _version = version
        _ray_actor_options = ray_actor_options
        _config = config

        @classmethod
        def deploy(self, *init_args):
            """Deploy this deployment.

            Args:
                *init_args (optional): args to pass to the class __init__
                    method. Not valid if this deployment wraps a function.
            """
            return _get_global_client().deploy(
                Deployment._name,
                Deployment._backend_def,
                *init_args,
                ray_actor_options=Deployment._ray_actor_options,
                config=Deployment._config,
                version=Deployment._version,
                _internal=True)

        @classmethod
        def delete(self):
            """Delete this deployment."""
            raise NotImplementedError()

        @classmethod
        def get_handle(self, sync: Optional[bool] = True
                       ) -> Union[RayServeHandle, RayServeSyncHandle]:
            return _get_global_client().get_handle(
                Deployment._name, missing_ok=False, sync=sync, _internal=True)

        @classmethod
        def options(self,
                    backend_def: Optional[Callable] = None,
                    name: Optional[str] = None,
                    version: Optional[str] = None,
                    ray_actor_options: Optional[Dict] = None,
                    config: Optional[BackendConfig] = None) -> "Deployment":
            """Return a new deployment with the specified options set."""
            return make_deployment_cls(
                backend_def or Deployment._backend_def,
                name or Deployment._name,
                version or Deployment._version,
                ray_actor_options=ray_actor_options
                or Deployment._ray_actor_options,
                config=config or Deployment._config,
            )

    return Deployment


# TODO(edoakes): better typing on the return value of the decorator.
def deployment(name: str,
               version: Optional[str] = None,
               ray_actor_options: Optional[Dict] = None,
               config: Optional[Union[BackendConfig, Dict[str, Any]]] = None
               ) -> Callable[[Callable], ServeDeployment]:
    """Define a Serve deployment.

    Args:
        name (str): Globally-unique name identifying this deployment.
        version (str): Version of the deployment. This is used to indicate a
            code change for the deployment; when it is re-deployed with a
            version change, a rolling update of the replicas will be performed.
        ray_actor_options (dict): Options to be passed to the Ray actor
            constructor such as resource requirements.
        config (dict, serve.BackendConfig, optional): Configuration options
            for this backend. Either a BackendConfig, or a dictionary
            mapping strings to values for the following supported options:
            - "num_replicas": number of processes to start up that
            will handle requests to this backend.
            - "max_concurrent_queries": the maximum number of queries that
            will be sent to a replica of this backend without receiving a
            response.
            - "user_config" (experimental): Arguments to pass to the
            reconfigure method of the backend. The reconfigure method is
            called if "user_config" is not None.

    Example:
    >>> @serve.deployment("deployment1", version="v1")
        class MyDeployment:
            pass

    >>> MyDeployment.deploy(*init_args)
    """

    def decorator(backend_def):
        return make_deployment_cls(backend_def, name, version,
                                   ray_actor_options, config)

    return decorator
