import asyncio
import atexit
import collections
import inspect
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from functools import wraps
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple,
                    Type, Union, overload)
from warnings import warn
from weakref import WeakValueDictionary

from starlette.requests import Request
from uvicorn.lifespan.on import LifespanOn
from uvicorn.config import Config

from ray import cloudpickle
from ray.actor import ActorHandle
from ray.util.annotations import Deprecated, PublicAPI
from ray.serve.common import BackendInfo, GoalId
from ray.serve.config import (BackendConfig, HTTPOptions, ReplicaConfig)
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 HTTP_PROXY_TIMEOUT, SERVE_CONTROLLER_NAME)
from ray.serve.controller import ReplicaTag, ServeController
from ray.serve.exceptions import RayServeException
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.http_util import (ASGIHTTPSender, make_fastapi_class_based_view)
from ray.serve.utils import (ensure_serialization_context, format_actor_name,
                             get_current_node_resource_key, get_random_letters,
                             logger, LoggingContext)

import ray

if TYPE_CHECKING:
    from fastapi import APIRouter, FastAPI  # noqa: F401

_INTERNAL_REPLICA_CONTEXT = None
_global_client = None

_UUID_RE = re.compile(
    "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}")


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
            for goal_id in ray.get(self._controller.shutdown.remote()):
                self._wait_for_goal(goal_id)

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
                       goal_id: Optional[GoalId],
                       timeout: Optional[float] = None) -> bool:
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

        ray.get(
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
            ray.get(
                self._controller.update_backend_config.remote(
                    backend_tag, config_options)))

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
        if not ray.util.client.ray.is_connected() and sys.platform != "win32":
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
            ray.get(
                self._controller.create_backend.remote(
                    backend_tag, backend_config, replica_config)))

    @_ensure_connected
    def deploy(self,
               name: str,
               backend_def: Union[Callable, Type[Callable], str],
               *init_args: Any,
               ray_actor_options: Optional[Dict] = None,
               config: Optional[Union[BackendConfig, Dict[str, Any]]] = None,
               version: Optional[str] = None,
               prev_version: Optional[str] = None,
               route_prefix: Optional[str] = None,
               _blocking: Optional[bool] = True) -> Optional[GoalId]:
        if config is None:
            config = {}
        if ray_actor_options is None:
            ray_actor_options = {}

        curr_job_env = ray.get_runtime_context().runtime_env
        if "runtime_env" in ray_actor_options:
            ray_actor_options["runtime_env"].setdefault(
                "uris", curr_job_env.get("uris"))
        else:
            ray_actor_options[
                "runtime_env"] = ray.get_runtime_context().runtime_env
            if "working_dir" in ray_actor_options["runtime_env"]:
                del ray_actor_options["runtime_env"]["working_dir"]

        replica_config = ReplicaConfig(
            backend_def, *init_args, ray_actor_options=ray_actor_options)

        if isinstance(config, dict):
            backend_config = BackendConfig.parse_obj(config)
        elif isinstance(config, BackendConfig):
            backend_config = config
        else:
            raise TypeError("config must be a BackendConfig or a dictionary.")

        python_methods = []
        if inspect.isclass(backend_def):
            for method_name, _ in inspect.getmembers(backend_def,
                                                     inspect.isfunction):
                python_methods.append(method_name)

        goal_id, updating = ray.get(
            self._controller.deploy.remote(
                name, backend_config, replica_config, python_methods, version,
                prev_version, route_prefix))

        if updating:
            msg = f"Updating deployment '{name}'"
            if version is not None:
                msg += f" to version '{version}'"
            logger.info(f"{msg}.")
        else:
            logger.info(f"Deployment '{name}' is already at version "
                        f"'{version}', not updating.")

        if _blocking:
            self._wait_for_goal(goal_id)
        else:
            return goal_id

    @_ensure_connected
    def delete_deployment(self, name: str) -> None:
        self._wait_for_goal(
            ray.get(self._controller.delete_deployment.remote(name)))

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
            ray.get(
                self._controller.delete_backend.remote(backend_tag, force)))

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
            sync: bool = True,
            _internal_use_serve_request: Optional[bool] = True,
            _internal_pickled_http_request: bool = False,
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
                _internal_use_serve_request=_internal_use_serve_request,
                _internal_pickled_http_request=_internal_pickled_http_request,
            )
        else:
            handle = RayServeHandle(
                self._controller,
                endpoint_name,
                known_python_methods=python_methods,
                _internal_use_serve_request=_internal_use_serve_request,
                _internal_pickled_http_request=_internal_pickled_http_request,
            )

        self.handle_cache[cache_key] = handle
        return handle


@PublicAPI
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
    ray.init(address="auto") or ray.init("ray://<remote_addr>")).

    Args:
        detached (bool): Whether not the instance should be detached from this
          script. If set, the instance will live on the Ray cluster until it is
          explicitly stopped with serve.shutdown(). This should *not* be set in
          an anonymous Ray namespace because you will not be able to reconnect
          to the instance after the script exits.
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
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

    current_namespace = ray.get_runtime_context().namespace
    if detached:
        if _UUID_RE.fullmatch(current_namespace) is not None:
            raise RuntimeError(
                "serve.start(detached=True) should not be called in anonymous "
                "Ray namespaces because you won't be able to reconnect to the "
                "Serve instance after the script exits. If you want to start "
                "a long-lived Serve instance, provide a namespace when "
                "connecting to Ray. See the documentation for more details: "
                "https://docs.ray.io/en/master/namespaces.html?highlight=namespace#using-namespaces."  # noqa: E501
            )

    try:
        _get_global_client()
        logger.info("Connecting to existing Serve instance in namespace "
                    f"'{current_namespace}'.")
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
    logger.info(f"Started{' detached ' if detached else ' '}Serve instance in "
                f"namespace '{current_namespace}'.")
    return client


@Deprecated
def connect() -> Client:
    """Connect to an existing Serve instance on this Ray cluster.

    If calling from the driver program, the Serve instance on this Ray cluster
    must first have been initialized using `serve.start(detached=True)`.

    If called from within a backend, this will connect to the same Serve
    instance that the backend is running in.
    """

    # Initialize ray if needed.
    ray.worker.global_worker.filter_logs_by_job = False
    if not ray.is_initialized():
        ray.init(namespace="serve")

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


@Deprecated
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


@Deprecated
def delete_endpoint(endpoint: str) -> None:
    """Delete the given endpoint.

    Does not delete any associated backends.
    """
    return _get_global_client().delete_endpoint(endpoint, _internal=True)


@Deprecated
def list_endpoints() -> Dict[str, Dict[str, Any]]:
    """Returns a dictionary of all registered endpoints.

    The dictionary keys are endpoint names and values are dictionaries
    of the form: {"methods": List[str], "traffic": Dict[str, float]}.
    """
    return _get_global_client().list_endpoints(_internal=True)


@Deprecated
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
            - "max_concurrent_queries": the maximum number of queries
            that will be sent to a replica of this backend
            without receiving a response.
            - "user_config" (experimental): Arguments to pass to the
            reconfigure method of the backend. The reconfigure method is
            called if "user_config" is not None.
    """
    return _get_global_client().update_backend_config(
        backend_tag, config_options, _internal=True)


@Deprecated
def get_backend_config(backend_tag: str) -> BackendConfig:
    """Get the backend configuration for a backend tag.

    Args:
        backend_tag(str): A registered backend.
    """
    return _get_global_client().get_backend_config(backend_tag, _internal=True)


@Deprecated
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


@Deprecated
def list_backends() -> Dict[str, BackendConfig]:
    """Returns a dictionary of all registered backends.

    Dictionary maps backend tags to backend config objects.
    """
    return _get_global_client().list_backends(_internal=True)


@Deprecated
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


@Deprecated
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


@Deprecated
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


@Deprecated
def get_handle(
        endpoint_name: str,
        missing_ok: bool = False,
        sync: bool = True,
        _internal_use_serve_request: bool = True,
        _internal_pickled_http_request: bool = False,
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
    return _get_global_client().get_handle(
        endpoint_name,
        missing_ok=missing_ok,
        sync=sync,
        _internal_use_serve_request=_internal_use_serve_request,
        _internal_pickled_http_request=_internal_pickled_http_request,
        _internal=True)


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
                                "Ray Serve backend.")
    return _INTERNAL_REPLICA_CONTEXT


@PublicAPI(stability="beta")
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

        class FastAPIWrapper(cls):
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

            def __del__(self):
                # LifespanOn's logger logs in INFO level thus becomes spammy
                # Within this block we temporarily uplevel for cleaner logging
                with LoggingContext(
                        self._serve_asgi_lifespan.logger,
                        level=logging.WARNING):
                    asyncio.get_event_loop().run_until_complete(
                        self._serve_asgi_lifespan.shutdown())

        FastAPIWrapper.__name__ = cls.__name__
        return FastAPIWrapper

    return decorator


@PublicAPI
class Deployment:
    def __init__(self,
                 func_or_class: Callable,
                 name: str,
                 config: BackendConfig,
                 version: Optional[str] = None,
                 prev_version: Optional[str] = None,
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
        if not callable(func_or_class):
            raise TypeError(
                "@serve.deployment must be called on a class or function.")
        if not isinstance(name, str):
            raise TypeError("name must be a string.")
        if not (version is None or isinstance(version, str)):
            raise TypeError("version must be a string.")
        if not (prev_version is None or isinstance(prev_version, str)):
            raise TypeError("prev_version must be a string.")
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

        self._func_or_class = func_or_class
        self._name = name
        self._version = version
        self._prev_version = prev_version
        self._config = config
        self._init_args = init_args
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
        """HTTP route prefix that this deploymet is exposed under."""
        return self._route_prefix

    @property
    def ray_actor_options(self) -> Optional[Dict]:
        """Actor options such as resources required for each replica."""
        return self._ray_actor_options

    @property
    def init_args(self) -> Tuple[Any]:
        """Arguments passed to the underlying class' constructor."""
        return self._init_args

    def __call__(self):
        raise RuntimeError("Deployments cannot be constructed directly. "
                           "Use `deployment.deploy() instead.`")

    @PublicAPI
    def deploy(self, *init_args, _blocking=True):
        """Deploy or update this deployment.

        Args:
            init_args (optional): args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        if len(init_args) == 0 and self._init_args is not None:
            init_args = self._init_args

        return _get_global_client().deploy(
            self._name,
            self._func_or_class,
            *init_args,
            ray_actor_options=self._ray_actor_options,
            config=self._config,
            version=self._version,
            prev_version=self._prev_version,
            route_prefix=self._route_prefix,
            _blocking=_blocking,
            _internal=True)

    @PublicAPI
    def delete(self):
        """Delete this deployment."""
        return _get_global_client().delete_deployment(
            self._name, _internal=True)

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
            self._name,
            missing_ok=True,
            sync=sync,
            _internal_use_serve_request=False,
            _internal=True)

    @PublicAPI
    def options(
            self,
            func_or_class: Optional[Callable] = None,
            name: Optional[str] = None,
            version: Optional[str] = None,
            prev_version: Optional[str] = None,
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

        if route_prefix is None:
            if self._route_prefix == f"/{self._name}":
                route_prefix = None
            else:
                route_prefix = self._route_prefix

        if ray_actor_options is None:
            ray_actor_options = self._ray_actor_options

        return Deployment(
            func_or_class,
            name,
            new_config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
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
            self._route_prefix == other._route_prefix,
            self._ray_actor_options == self._ray_actor_options,
        ])

    def __str__(self):
        if self._route_prefix is None:
            route_prefix = f"/{self._name}"
        else:
            route_prefix = self._route_prefix
        return (f"Deployment(name={self._name},"
                f"version={self._version},"
                f"route_prefix={route_prefix})")

    def __repr__(self):
        return str(self)


@overload
def deployment(func_or_class: Callable) -> Deployment:
    pass


@overload
def deployment(name: Optional[str] = None,
               version: Optional[str] = None,
               prev_version: Optional[str] = None,
               num_replicas: Optional[int] = None,
               init_args: Optional[Tuple[Any]] = None,
               ray_actor_options: Optional[Dict] = None,
               user_config: Optional[Any] = None,
               max_concurrent_queries: Optional[int] = None
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
        prev_version (Optional[str]): Version of the existing deployment which
            is used as a precondition for the next deployment. If prev_version
            does not match with the existing deployment's version, the
            deployment will fail. If not provided, deployment procedure will
            not check the existing deployment's version.
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
        user_config (Optional[Any]): [experimental] Config to pass to the
            reconfigure method of the backend. This can be updated dynamically
            without changing the version of the deployment and restarting its
            replicas. The user_config needs to be hashable to keep track of
            updates, so it must only contain hashable types, or hashable types
            nested in lists and dictionaries.
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

    def decorator(_func_or_class):
        return Deployment(
            _func_or_class,
            name if name is not None else _func_or_class.__name__,
            config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
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
        backend_info, route_prefix = _get_global_client().get_deployment_info(
            name, _internal=True)
    except KeyError:
        raise KeyError(f"Deployment {name} was not found. "
                       "Did you call Deployment.deploy()?")
    return Deployment(
        cloudpickle.loads(backend_info.replica_config.serialized_backend_def),
        name,
        backend_info.backend_config,
        version=backend_info.version,
        init_args=backend_info.replica_config.init_args,
        route_prefix=route_prefix,
        ray_actor_options=backend_info.replica_config.ray_actor_options,
        _internal=True,
    )


@PublicAPI
def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = _get_global_client().list_deployments(_internal=True)

    deployments = {}
    for name, (backend_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            cloudpickle.loads(
                backend_info.replica_config.serialized_backend_def),
            name,
            backend_info.backend_config,
            version=backend_info.version,
            init_args=backend_info.replica_config.init_args,
            route_prefix=route_prefix,
            ray_actor_options=backend_info.replica_config.ray_actor_options,
            _internal=True,
        )

    return deployments
