from functools import wraps

import ray
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 SERVE_CONTROLLER_NAME, HTTP_PROXY_TIMEOUT)
from ray.serve.controller import ServeController
from ray.serve.handle import RayServeHandle
from ray.serve.utils import (block_until_http_ready, format_actor_name)
from ray.serve.exceptions import RayServeException
from ray.serve.config import BackendConfig, ReplicaConfig
from ray.serve.metric import InMemoryExporter

controller = None


def _get_controller():
    """Used for internal purpose because using just import serve.global_state
    will always reference the original None object.
    """
    global controller
    if controller is None:
        raise RayServeException("Please run serve.init to initialize or "
                                "connect to existing ray serve cluster.")
    return controller


def _ensure_connected(f):
    @wraps(f)
    def check(*args, **kwargs):
        _get_controller()
        return f(*args, **kwargs)

    return check


def accept_batch(f):
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


def init(
        name=None,
        http_host=DEFAULT_HTTP_HOST,
        http_port=DEFAULT_HTTP_PORT,
        metric_exporter=InMemoryExporter,
):
    """Initialize or connect to a serve cluster.

    If serve cluster is already initialized, this function will just return.

    If `ray.init` has not been called in this process, it will be called with
    no arguments. To specify kwargs to `ray.init`, it should be called
    separately before calling `serve.init`.

    Args:
        name (str): A unique name for this serve instance. This allows
            multiple serve instances to run on the same ray cluster. Must be
            specified in all subsequent serve.init() calls.
        http_host (str): Host for HTTP servers. Default to "0.0.0.0". Serve
            starts one HTTP server per node in the Ray cluster.
        http_port (int, List[int]): Port for HTTP server. Default to 8000.
        metric_exporter(ExporterInterface): The class aggregates metrics from
            all RayServe actors and optionally export them to external
            services. Ray Serve has two options built in: InMemoryExporter and
            PrometheusExporter
    """
    if name is not None and not isinstance(name, str):
        raise TypeError("name must be a string.")

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init()

    # Try to get serve controller if it exists
    global controller
    controller_name = format_actor_name(SERVE_CONTROLLER_NAME, name)
    try:
        controller = ray.get_actor(controller_name)
        return
    except ValueError:
        pass

    controller = ServeController.options(
        name=controller_name,
        max_restarts=-1,
        max_task_retries=-1,
    ).remote(
        name,
        http_host,
        http_port,
        metric_exporter,
    )

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


@_ensure_connected
def shutdown():
    """Completely shut down the connected Serve instance.

    Shuts down all processes and deletes all state associated with the Serve
    instance that's currently connected to (via serve.init).
    """
    global controller
    ray.get(controller.shutdown.remote())
    ray.kill(controller, no_restart=True)
    controller = None


@_ensure_connected
def create_endpoint(endpoint_name,
                    *,
                    backend=None,
                    route=None,
                    methods=["GET"]):
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

    upper_methods = []
    for method in methods:
        if not isinstance(method, str):
            raise TypeError("methods must be a list of strings, but contained "
                            "an element of type {}".format(type(method)))
        upper_methods.append(method.upper())

    ray.get(
        controller.create_endpoint.remote(endpoint_name, {backend: 1.0}, route,
                                          upper_methods))


@_ensure_connected
def delete_endpoint(endpoint):
    """Delete the given endpoint.

    Does not delete any associated backends.
    """
    ray.get(controller.delete_endpoint.remote(endpoint))


@_ensure_connected
def list_endpoints():
    """Returns a dictionary of all registered endpoints.

    The dictionary keys are endpoint names and values are dictionaries
    of the form: {"methods": List[str], "traffic": Dict[str, float]}.
    """
    return ray.get(controller.get_all_endpoints.remote())


@_ensure_connected
def update_backend_config(backend_tag, config_options):
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
        controller.update_backend_config.remote(backend_tag, config_options))


@_ensure_connected
def get_backend_config(backend_tag):
    """Get the backend configuration for a backend tag.

    Args:
        backend_tag(str): A registered backend.
    """
    return ray.get(controller.get_backend_config.remote(backend_tag))


@_ensure_connected
def create_backend(backend_tag,
                   func_or_class,
                   *actor_init_args,
                   ray_actor_options=None,
                   config=None):
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
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")

    replica_config = ReplicaConfig(
        func_or_class, *actor_init_args, ray_actor_options=ray_actor_options)
    backend_config = BackendConfig(config, replica_config.accepts_batches,
                                   replica_config.is_blocking)

    ray.get(
        controller.create_backend.remote(backend_tag, backend_config,
                                         replica_config))


@_ensure_connected
def list_backends():
    """Returns a dictionary of all registered backends.

    Dictionary maps backend tags to backend configs.
    """
    return ray.get(controller.get_all_backends.remote())


@_ensure_connected
def delete_backend(backend_tag):
    """Delete the given backend.

    The backend must not currently be used by any endpoints.
    """
    ray.get(controller.delete_backend.remote(backend_tag))


@_ensure_connected
def set_traffic(endpoint_name, traffic_policy_dictionary):
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
        controller.set_traffic.remote(endpoint_name,
                                      traffic_policy_dictionary))


@_ensure_connected
def shadow_traffic(endpoint_name, backend_tag, proportion):
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
        controller.shadow_traffic.remote(endpoint_name, backend_tag,
                                         proportion))


@_ensure_connected
def get_handle(endpoint_name,
               relative_slo_ms=None,
               absolute_slo_ms=None,
               missing_ok=False):
    """Retrieve RayServeHandle for service endpoint to invoke it from Python.

    Args:
        endpoint_name (str): A registered service endpoint.
        relative_slo_ms(float): Specify relative deadline in milliseconds for
            queries fired using this handle. (Default: None)
        absolute_slo_ms(float): Specify absolute deadline in milliseconds for
            queries fired using this handle. (Default: None)
        missing_ok (bool): If true, skip the check for the endpoint existence.
            It can be useful when the endpoint has not been registered.

    Returns:
        RayServeHandle
    """
    if not missing_ok:
        assert endpoint_name in ray.get(controller.get_all_endpoints.remote())

    return RayServeHandle(
        ray.get(controller.get_router.remote())[0],
        endpoint_name,
        relative_slo_ms,
        absolute_slo_ms,
    )


@_ensure_connected
def stat():
    """Retrieve metric statistics about ray serve system.

    Returns:
        metric_stats(Any): Metric information returned by the metric exporter.
            This can vary by exporter. For the default InMemoryExporter, it
            returns a list of the following format:

            .. code-block::python
              [
                  {"info": {
                      "name": ...,
                      "type": COUNTER|MEASURE,
                      "label_key": label_value,
                      "label_key": label_value,
                      ...
                  }, "value": float}
              ]

            For PrometheusExporter, it returns the metrics in prometheus format
            in plain text.
    """
    [metric_exporter] = ray.get(controller.get_metric_exporter.remote())
    return ray.get(metric_exporter.inspect_metrics.remote())
