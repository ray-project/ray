import asyncio
import atexit
import collections
import inspect
import os
import time
from dataclasses import dataclass
from functools import wraps
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple,
                    Type, Union, overload)
from warnings import warn
from weakref import WeakValueDictionary

from starlette.requests import Request

from ray import cloudpickle
from ray.actor import ActorHandle
from ray.serve.common import BackendInfo, GoalId
from ray.serve.config import (BackendConfig, HTTPOptions, ReplicaConfig)
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 HTTP_PROXY_TIMEOUT, SERVE_CONTROLLER_NAME)
from ray.serve.controller import BackendTag, ReplicaTag, ServeController
from ray.serve.exceptions import RayServeException
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.http_util import (ASGIHTTPSender, make_fastapi_class_based_view,
                                 make_startup_shutdown_hooks)
from ray.serve.utils import (ensure_serialization_context, format_actor_name,
                             get_current_node_resource_key, get_random_letters,
                             logger)

import ray

if TYPE_CHECKING:
    from fastapi import APIRouter, FastAPI  # noqa: F401

_INTERNAL_REPLICA_CONTEXT = None
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
    servable_object: Callable


def _set_internal_replica_context(
        backend_tag: BackendTag,
        replica_tag: ReplicaTag,
        controller_name: str,
        servable_object: Callable,
):
    global _INTERNAL_REPLICA_CONTEXT
    _INTERNAL_REPLICA_CONTEXT = ReplicaContext(
        backend_tag, replica_tag, controller_name, servable_object)


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

        # Each handle has the overhead of long poll client, therefore cached.
        self.handle_cache = WeakValueDictionary()

        # NOTE(edoakes): Need this because the shutdown order isn't guaranteed
        # when the interpreter is exiting so we can't rely on __del__ (it
        # throws a nasty stacktrace).
        if not self._detached:

            def shutdown_serve_client():
                self.shutdown()

            atexit.register(shutdown_serve_client)

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

    def _wait_for_goal(self,
                       result_object_id: ray.ObjectRef,
                       timeout: Optional[float] = None) -> bool:
        goal_id: Optional[GoalId] = ray.get(result_object_id)
        if goal_id is None:
            return True

        ready, _ = ray.wait(
            [self._controller.wait_for_goal.remote(goal_id)], timeout=timeout)
        return len(ready) == 1

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

        if isinstance(config, dict):
            backend_config = BackendConfig.parse_obj(config)
        elif isinstance(config, BackendConfig):
            backend_config = config
        else:
            raise TypeError("config must be a BackendConfig or a dictionary.")

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
               version: Optional[str] = None,
               route_prefix: Optional[str] = None,
               _blocking: Optional[bool] = True) -> Optional[GoalId]:
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

        if isinstance(config, dict):
            backend_config = BackendConfig.parse_obj(config)
        elif isinstance(config, BackendConfig):
            backend_config = config
        else:
            raise TypeError("config must be a BackendConfig or a dictionary.")

        goal_ref = self._controller.deploy.remote(
            name, backend_config, replica_config, version, route_prefix)

        if _blocking:
            self._wait_for_goal(goal_ref)
        else:
            return goal_ref

    @_ensure_connected
    def delete_deployment(self, name: str) -> None:
        self._wait_for_goal(self._controller.delete_deployment.remote(name))

    @_ensure_connected
    def get_deployment_info(self, name: str) -> Tuple[BackendInfo, str]:
        return ray.get(self._controller.get_deployment_info.remote(name))

    @_ensure_connected
    def list_deployments(self) -> Dict[str, Tuple[BackendInfo, str]]:
        return ray.get(self._controller.list_deployments.remote())

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
    def get_handle(self,
                   endpoint_name: str,
                   missing_ok: Optional[bool] = False,
                   sync: bool = True,
                   _internal_use_serve_request: Optional[bool] = True
                   ) -> Union[RayServeHandle, RayServeSyncHandle]:
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
        cache_key = (endpoint_name, missing_ok, sync)
        if cache_key in self.handle_cache:
            return self.handle_cache[cache_key]

        all_endpoints = ray.get(self._controller.get_all_endpoints.remote())
        if not missing_ok and endpoint_name not in all_endpoints:
            raise KeyError(f"Endpoint '{endpoint_name}' does not exist.")

        if asyncio.get_event_loop().is_running() and sync:
            logger.warning(
                "You are retrieving a sync handle inside an asyncio loop. "
                "Try getting client.get_handle(.., sync=False) to get better "
                "performance. Learn more at https://docs.ray.io/en/master/"
                "serve/http-servehandle.html#sync-and-async-handles")

        if not asyncio.get_event_loop().is_running() and not sync:
            logger.warning(
                "You are retrieving an async handle outside an asyncio loop. "
                "You should make sure client.get_handle is called inside a "
                "running event loop. Or call client.get_handle(.., sync=True) "
                "to create sync handle. Learn more at https://docs.ray.io/en/"
                "master/serve/http-servehandle.html#sync-and-async-handles")

        if endpoint_name in all_endpoints:
            this_endpoint = all_endpoints[endpoint_name]
            python_methods: List[str] = this_endpoint["python_methods"]
        else:
            # This can happen in the missing_ok=True case.
            # handle.method_name.remote won't work and user must
            # use the legacy handle.options(method).remote().
            python_methods: List[str] = []

        if sync:
            handle = RayServeSyncHandle(
                self._controller,
                endpoint_name,
                known_python_methods=python_methods,
                _internal_use_serve_request=_internal_use_serve_request)
        else:
            handle = RayServeHandle(
                self._controller,
                endpoint_name,
                known_python_methods=python_methods,
                _internal_use_serve_request=_internal_use_serve_request)

        self.handle_cache[cache_key] = handle
        return handle


def start(
        detached: bool = False,
        http_host: Optional[str] = DEFAULT_HTTP_HOST,
        http_port: int = DEFAULT_HTTP_PORT,
        http_middlewares: List[Any] = [],
        http_options: Optional[Union[dict, HTTPOptions]] = None,
        dedicated_cpu: bool = False,
) -> Client:
    """Initialize a serve instance.

    By default, the instance will be scoped to the lifetime of the returned
    Client object (or when the script exits). If detached is set to True, the
    instance will instead persist until serve.shutdown() is called. This is
    only relevant if connecting to a long-running Ray cluster (e.g., with
    ray.init(address="auto") or ray.util.connect("<remote_addr>")).

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

    try:
        _get_global_client()
        logger.info("Connecting to existing Serve instance.")
        return
    except RayServeException:
        pass

    # Try to get serve controller if it exists
    if detached:
        controller_name = SERVE_CONTROLLER_NAME
    else:
        controller_name = format_actor_name(SERVE_CONTROLLER_NAME,
                                            get_random_letters())

    if isinstance(http_options, dict):
        http_options = HTTPOptions.parse_obj(http_options)
    if http_options is None:
        http_options = HTTPOptions(
            host=http_host, port=http_port, middlewares=http_middlewares)

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a backend, this will connect to the same Serve
    instance that the backend is running in.
    """

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init()

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

    Does not delete any associated backends.
    """
    return _get_global_client().delete_endpoint(endpoint, _internal=True)


def list_endpoints() -> Dict[str, Dict[str, Any]]:
    """Returns a dictionary of all registered endpoints.

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

    The dictionary keys are endpoint names and values are dictionaries
    of the form: {"methods": List[str], "traffic": Dict[str, float]}.
    """
    return _get_global_client().list_endpoints(_internal=True)


def update_backend_config(
        backend_tag: str,
        config_options: Union[BackendConfig, Dict[str, Any]]) -> None:
    """Update a backend configuration for a backend tag.

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

    Keys not specified in the passed will be left unchanged.

    Args:
        backend_tag(str): A registered backend.
        config_options(dict, serve.BackendConfig): Backend config options
            to update. Either a BackendConfig object or a dict mapping
            strings to values for the following supported options:
            - "num_replicas": number of processes to start up that
            will handle requests to this backend.
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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

    Dictionary maps backend tags to backend config objects.
    """
    return _get_global_client().list_backends(_internal=True)


def delete_backend(backend_tag: str, force: bool = False) -> None:
    """Delete the given backend.

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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
               sync: Optional[bool] = True,
               _internal_use_serve_request: Optional[bool] = True
               ) -> Union[RayServeHandle, RayServeSyncHandle]:
    """Retrieve RayServeHandle for service endpoint to invoke it from Python.

    DEPRECATED. Will be removed in Ray 1.5. See docs for details.

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
        endpoint_name,
        missing_ok=missing_ok,
        sync=sync,
        _internal_use_serve_request=_internal_use_serve_request,
        _internal=True)


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


def ingress(app: Union["FastAPI", "APIRouter"]):
    """Mark a FastAPI application ingress for Serve.

    Args:
        app(FastAPI,APIRouter): the app or router object serve as ingress
            for this backend.

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
        make_fastapi_class_based_view(app, cls)

        # Free the state of the app so subsequent modification won't affect
        # this ingress deployment. We don't use copy.copy here to avoid
        # recursion issue.
        ensure_serialization_context()
        frozen_app = cloudpickle.loads(cloudpickle.dumps(app))

        startup_hook, shutdown_hook = make_startup_shutdown_hooks(frozen_app)

        class FastAPIWrapper(cls):
            async def __init__(self, *args, **kwargs):
                # TODO(edoakes): should the startup_hook run before or after
                # the constructor?
                await startup_hook()
                super().__init__(*args, **kwargs)

            async def __call__(self, request: Request):
                sender = ASGIHTTPSender()
                await frozen_app(
                    request.scope,
                    request._receive,
                    sender,
                )
                return sender.build_starlette_response()

            def __del__(self):
                asyncio.get_event_loop().run_until_complete(shutdown_hook())

        FastAPIWrapper.__name__ = cls.__name__
        return FastAPIWrapper

    return decorator


class Deployment:
    def __init__(self,
                 backend_def: Callable,
                 name: str,
                 config: BackendConfig,
                 version: Optional[str] = None,
                 init_args: Optional[Tuple[Any]] = None,
                 route_prefix: Optional[str] = None,
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
        if not callable(backend_def):
            raise TypeError(
                "@serve.deployment must be called on a class or function.")
        if not isinstance(name, str):
            raise TypeError("name must be a string.")
        if not (version is None or isinstance(version, str)):
            raise TypeError("version must be a string.")
        if not (init_args is None or isinstance(init_args, tuple)):
            raise TypeError("init_args must be a tuple.")
        if route_prefix is not None:
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

        self.backend_def = backend_def
        self.name = name
        self.version = version
        self.config = config
        self.init_args = init_args
        self.route_prefix = route_prefix
        self.ray_actor_options = ray_actor_options

    def __call__(self):
        raise RuntimeError("Deployments cannot be constructed directly. "
                           "Use `deployment.deploy() instead.`")

    def deploy(self, *init_args, _blocking=True):
        """Deploy this deployment.

        Args:
            *init_args (optional): args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        if len(init_args) == 0 and self.init_args is not None:
            init_args = self.init_args

        return _get_global_client().deploy(
            self.name,
            self.backend_def,
            *init_args,
            ray_actor_options=self.ray_actor_options,
            config=self.config,
            version=self.version,
            route_prefix=self.route_prefix,
            _blocking=_blocking,
            _internal=True)

    def delete(self):
        """Delete this deployment."""
        return _get_global_client().delete_deployment(
            self.name, _internal=True)

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
            self.name,
            missing_ok=True,
            sync=sync,
            _internal_use_serve_request=False,
            _internal=True)

    def options(
            self,
            backend_def: Optional[Callable] = None,
            name: Optional[str] = None,
            version: Optional[str] = None,
            init_args: Optional[Tuple[Any]] = None,
            route_prefix: Optional[str] = None,
            num_replicas: Optional[int] = None,
            ray_actor_options: Optional[Dict] = None,
            user_config: Optional[Any] = None,
            max_concurrent_queries: Optional[int] = None,
    ) -> "Deployment":
        """Return a copy of this deployment with updated options.

        Only those options passed in will be updated, all others will remain
        unchanged from the existing deployment.
        """
        new_config = self.config.copy()
        if num_replicas is not None:
            new_config.num_replicas = num_replicas
        if user_config is not None:
            new_config.user_config = user_config
        if max_concurrent_queries is not None:
            new_config.max_concurrent_queries = max_concurrent_queries

        if backend_def is None:
            backend_def = self.backend_def

        if name is None:
            name = self.name

        if version is None:
            version = self.version

        if init_args is None:
            init_args = self.init_args

        if route_prefix is None:
            if self.route_prefix == f"/{self.name}":
                route_prefix = None
            else:
                route_prefix = self.route_prefix

        if ray_actor_options is None:
            ray_actor_options = self.ray_actor_options

        return Deployment(
            backend_def,
            name,
            new_config,
            version=version,
            init_args=init_args,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    def __eq__(self, other):
        return all([
            self.name == other.name,
            self.version == other.version,
            self.config == other.config,
            self.init_args == other.init_args,
            self.route_prefix == other.route_prefix,
            self.ray_actor_options == self.ray_actor_options,
        ])

    def __str__(self):
        if self.route_prefix is None:
            route_prefix = f"/{self.name}"
        else:
            route_prefix = self.route_prefix
        return (f"Deployment(name={self.name},"
                f"version={self.version},"
                f"route_prefix={route_prefix})")

    def __repr__(self):
        return str(self)


@overload
def deployment(backend_def: Callable) -> Deployment:
    pass


@overload
def deployment(name: Optional[str] = None,
               version: Optional[str] = None,
               num_replicas: Optional[int] = None,
               init_args: Optional[Tuple[Any]] = None,
               ray_actor_options: Optional[Dict] = None,
               user_config: Optional[Any] = None,
               max_concurrent_queries: Optional[int] = None
               ) -> Callable[[Callable], Deployment]:
    pass


def deployment(
        _backend_def: Optional[Callable] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        num_replicas: Optional[int] = None,
        init_args: Optional[Tuple[Any]] = None,
        route_prefix: Optional[str] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
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
        num_replicas (Optional[int]): The number of processes to start up that
            will handle requests to this backend. Defaults to 1.
        init_args (Optional[Tuple]): Arguments to be passed to the class
            constructor when starting up deployment replicas. These can also be
            passed when you call `.deploy()` on the returned Deployment.
        route_prefix (Optional[str]): Requests to paths under this HTTP path
            prefix will be routed to this deployment. Defaults to '/{name}'.
            Routing is done based on longest-prefix match, so if you have
            deployment A with a prefix of '/a' and deployment B with a prefix
            of '/a/b', requests to '/a', '/a/', and '/a/c' go to A and requests
            to '/a/b', '/a/b/', and '/a/b/c' go to B. Routes must not end with
            a '/' unless they're the root (just '/'), which acts as a
            catch-all.
        ray_actor_options (dict): Options to be passed to the Ray actor
            constructor such as resource requirements.
        user_config (Optional[Any]): [experimental] Arguments to pass to the
            reconfigure method of the backend. The reconfigure method is
            called if user_config is not None.
        max_concurrent_queries (Optional[int]): The maximum number of queries
            that will be sent to a replica of this backend without receiving a
            response. Defaults to 100.

    Example:

    >>> @serve.deployment(name="deployment1", version="v1")
        class MyDeployment:
            pass

    >>> MyDeployment.deploy(*init_args)
    >>> MyDeployment.options(num_replicas=2, init_args=init_args).deploy()

    Returns:
        Deployment
    """

    config = BackendConfig()
    if num_replicas is not None:
        config.num_replicas = num_replicas

    if user_config is not None:
        config.user_config = user_config

    if max_concurrent_queries is not None:
        config.max_concurrent_queries = max_concurrent_queries

    def decorator(_backend_def):
        return Deployment(
            _backend_def,
            name if name is not None else _backend_def.__name__,
            config,
            version=version,
            init_args=init_args,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    # This handles both parametrized and non-parametrized usage of the
    # decorator. See the @serve.batch code for more details.
    return decorator(_backend_def) if callable(_backend_def) else decorator


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
        backend_info, route_prefix = _get_global_client().get_deployment_info(
            name, _internal=True)
    except KeyError:
        raise KeyError(f"Deployment {name} was not found. "
                       "Did you call Deployment.deploy()?")
    return Deployment(
        backend_info.replica_config.backend_def,
        name,
        backend_info.backend_config,
        version=backend_info.version,
        init_args=backend_info.replica_config.init_args,
        route_prefix=route_prefix,
        ray_actor_options=backend_info.replica_config.ray_actor_options,
        _internal=True,
    )


def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = _get_global_client().list_deployments(_internal=True)

    deployments = {}
    for name, (backend_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            backend_info.replica_config.backend_def,
            name,
            backend_info.backend_config,
            version=backend_info.version,
            init_args=backend_info.replica_config.init_args,
            route_prefix=route_prefix,
            ray_actor_options=backend_info.replica_config.ray_actor_options,
            _internal=True,
        )

    return deployments
