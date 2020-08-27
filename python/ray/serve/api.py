from functools import wraps

import ray
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 SERVE_CONTROLLER_NAME, HTTP_PROXY_TIMEOUT)
from ray.serve.controller import ServeController
from ray.serve.handle import RayServeHandle
from ray.serve.utils import (block_until_http_ready, format_actor_name, get_random_letters)
from ray.serve.exceptions import RayServeException
from ray.serve.config import BackendConfig, ReplicaConfig
from ray.actor import ActorHandle
from typing import Any, Callable, Dict, List, Optional, Type, Union


def _ensure_connected(f: Callable) -> Callable:
    @wraps(f)
    def check(self, *args, **kwargs):
        if self._controller is None:
            raise RayServeException("TODO: not connected.")
        return f(self, *args, **kwargs)

    return check

class Client:
    def __init__(self, controller, controller_name, detached=False):
        self._controller = controller
        self._controller_name = controller_name
        self._detached = detached

    def __del__(self):
        if not self._detached:
            self.shutdown()

    def shutdown(self) -> None:
        """Completely shut down the connected Serve instance.

        Shuts down all processes and deletes all state associated with the Serve
        instance that's currently connected to (via serve.init).
        """
        if self._controller is not None:
            ray.get(self._controller.shutdown.remote())
            ray.kill(self._controller, no_restart=True)
            self._controller = None


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
                this endpoint. To change this or split traffic among backends, use
                `serve.set_traffic`.
            route (str, optional): A string begin with "/". HTTP server will use
                the string to match the path.
            methods(List[str], optional): The HTTP methods that are valid for this
                endpoint.
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
            if methods_old.sort() == methods.sort() and route_old == route:
                raise ValueError(
                    "Route '{}' is already registered to endpoint '{}' "
                    "with methods '{}'.  To set the backend for this "
                    "endpoint, please use serve.set_traffic().".format(
                        route, endpoint_name, methods))

        upper_methods = []
        for method in methods:
            if not isinstance(method, str):
                raise TypeError("methods must be a list of strings, but contained "
                                "an element of type {}".format(type(method)))
            upper_methods.append(method.upper())

        ray.get(
            self._controller.create_endpoint.remote(endpoint_name, {backend: 1.0}, route,
                                              upper_methods))


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
    def update_backend_config(self,
                              backend_tag: str,
                              config_options: Dict[str, Any]) -> None:
        """Update a backend configuration for a backend tag.

        Keys not specified in the passed will be left unchanged.

        Args:
            backend_tag(str): A registered backend.
            config_options(dict): Backend config options to update.
                Supported options:
                - "num_replicas": number of worker processes to start up that
                will handle requests to this backend.
                - "max_batch_size": the maximum number of requests that will
                be processed in one batch by this backend.
                - "batch_wait_timeout": time in seconds that backend replicas
                will wait for a full batch of requests before
                processing a partial batch.
                - "max_concurrent_queries": the maximum number of queries
                that will be sent to a replica of this backend
                without receiving a response.
        """
        if not isinstance(config_options, dict):
            raise ValueError("config_options must be a dictionary.")
        ray.get(
            self._controller.update_backend_config.remote(backend_tag, config_options))


    @_ensure_connected
    def get_backend_config(self, backend_tag: str):
        """Get the backend configuration for a backend tag.

        Args:
            backend_tag(str): A registered backend.
        """
        return ray.get(self._controller.get_backend_config.remote(backend_tag))


    @_ensure_connected
    def create_backend(self,
                       backend_tag: str,
                       func_or_class: Union[Callable, Type[Callable]],
                       *actor_init_args: Any,
                       ray_actor_options: Optional[Dict] = None,
                       config: Optional[Dict[str, Any]] = None) -> None:
        """Create a backend with the provided tag.

        The backend will serve requests with func_or_class.

        Args:
            backend_tag (str): a unique tag assign to identify this backend.
            func_or_class (callable, class): a function or a class implementing
                __call__.
            actor_init_args (optional): the arguments to pass to the class.
                initialization method.
            ray_actor_options (optional): options to be passed into the
                @ray.remote decorator for the backend actor.
            config (optional): configuration options for this backend.
                Supported options:
                - "num_replicas": number of worker processes to start up that will
                handle requests to this backend.
                - "max_batch_size": the maximum number of requests that will
                be processed in one batch by this backend.
                - "batch_wait_timeout": time in seconds that backend replicas
                will wait for a full batch of requests before processing a
                partial batch.
                - "max_concurrent_queries": the maximum number of queries that will
                be sent to a replica of this backend without receiving a
                response.
        """
        if backend_tag in self.list_backends():
            raise ValueError(
                "Cannot create backend. "
                "Backend '{}' is already registered.".format(backend_tag))

        if config is None:
            config = {}
        if not isinstance(config, dict):
            raise TypeError("config must be a dictionary.")

        replica_config = ReplicaConfig(
            func_or_class, *actor_init_args, ray_actor_options=ray_actor_options)
        backend_config = BackendConfig(config, replica_config.accepts_batches,
                                       replica_config.is_blocking)

        ray.get(
            self._controller.create_backend.remote(backend_tag, backend_config,
                                             replica_config))


    @_ensure_connected
    def list_backends(self) -> Dict[str, Dict[str, Any]]:
        """Returns a dictionary of all registered backends.

        Dictionary maps backend tags to backend configs.
        """
        return ray.get(self._controller.get_all_backends.remote())


    @_ensure_connected
    def delete_backend(self, backend_tag: str) -> None:
        """Delete the given backend.

        The backend must not currently be used by any endpoints.
        """
        ray.get(self._controller.delete_backend.remote(backend_tag))


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

        if not isinstance(proportion, (float, int)) or not 0 <= proportion <= 1:
            raise TypeError("proportion must be a float from 0 to 1.")

        ray.get(
            self._controller.shadow_traffic.remote(endpoint_name, backend_tag,
                                             proportion))


    @_ensure_connected
    def get_handle(self, endpoint_name: str) -> RayServeHandle:
        """Retrieve RayServeHandle for service endpoint to invoke it from Python.

        Args:
            endpoint_name (str): A registered service endpoint.

        Returns:
            RayServeHandle
        """
        assert endpoint_name in ray.get(self._controller.get_all_endpoints.remote())

        # TODO(edoakes): we should choose the router on the same node.
        routers = ray.get(self._controller.get_routers.remote())
        return RayServeHandle(
            list(routers.values())[0],
            endpoint_name,
        )


def start(detached: bool = False,
          http_host: str = DEFAULT_HTTP_HOST,
          http_port: int = DEFAULT_HTTP_PORT,
          _http_middlewares: List[Any] = []) -> Client:
    """Initialize a serve cluster.

    If serve cluster is already initialized, this function will just return.

    If `ray.init` has not been called in this process, it will be called with
    no arguments. To specify kwargs to `ray.init`, it should be called
    separately before calling `serve.init`.

    Args:
        detached (bool): TODO.
        http_host (str): Host for HTTP servers. Default to "0.0.0.0". Serve
            starts one HTTP server per node in the Ray cluster.
        http_port (int): Port for HTTP server. Defaults to 8000.
    """
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
        controller_name = format_actor_name(SERVE_CONTROLLER_NAME, get_random_letters())

    controller = ServeController.options(
        detached=detached,
        name=controller_name,
        max_restarts=-1,
        max_task_retries=-1,
    ).remote(controller_name, http_host, http_port, _http_middlewares)

    futures = []
    for node_id in ray.state.node_ids():
        future = block_until_http_ready.options(
            num_cpus=0, resources={
                node_id: 0.01
            }).remote(
                "http://{}:{}/-/routes".format(http_host, http_port),
                timeout=HTTP_PROXY_TIMEOUT)
        futures.append(future)
    ray.get(futures)

    return Client(controller, controller_name, detached=detached)


def connect() -> Client:
    """TODO."""

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init()

    # Try to get serve controller if it exists
    try:
        controller = ray.get_actor(SERVE_CONTROLLER_NAME)
    except ValueError:
        raise Exception("TODO")

    return Client(controller, SERVE_CONTROLLER_NAME, detached=True)


def accept_batch(f: Callable) -> Callable:
    """Annotation to mark a serving function that batch is accepted.

    This annotation need to be used to mark a function expect all arguments
    to be passed into a list.

    Example:

    >>> @serve.accept_batch
        def serving_func(flask_request):
            assert isinstance(flask_request, list)
            ...

    >>> class ServingActor:
            @serve.accept_batch
            def __call__(self, *, python_arg=None):
                assert isinstance(python_arg, list)
    """
    f._serve_accept_batch = True
    return f
