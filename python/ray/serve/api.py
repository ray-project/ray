from functools import wraps
from tempfile import mkstemp

from multiprocessing import cpu_count

import ray
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 SERVE_MASTER_NAME)
from ray.serve.master import ServeMaster
from ray.serve.handle import RayServeHandle
from ray.serve.kv_store_service import SQLiteKVStore
from ray.serve.utils import block_until_http_ready, retry_actor_failures
from ray.serve.exceptions import RayServeException
from ray.serve.config import BackendConfig, ReplicaConfig
from ray.serve.policy import RoutePolicy
from ray.serve.router import Query
from ray.serve.request_params import RequestMetadata

master_actor = None


def _get_master_actor():
    """Used for internal purpose because using just import serve.global_state
    will always reference the original None object.
    """
    return master_actor


def _ensure_connected(f):
    @wraps(f)
    def check(*args, **kwargs):
        if _get_master_actor() is None:
            raise RayServeException("Please run serve.init to initialize or "
                                    "connect to existing ray serve cluster.")
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
        kv_store_connector=None,
        kv_store_path=None,
        blocking=False,
        start_server=True,
        http_host=DEFAULT_HTTP_HOST,
        http_port=DEFAULT_HTTP_PORT,
        ray_init_kwargs={
            "object_store_memory": int(1e8),
            "num_cpus": max(cpu_count(), 8)
        },
        gc_window_seconds=3600,
        queueing_policy=RoutePolicy.Random,
        policy_kwargs={},
):
    """Initialize a serve cluster.

    If serve cluster has already initialized, this function will just return.

    Calling `ray.init` before `serve.init` is optional. When there is not a ray
    cluster initialized, serve will call `ray.init` with `object_store_memory`
    requirement.

    Args:
        kv_store_connector (callable): Function of (namespace) => TableObject.
            We will use a SQLite connector that stores to /tmp by default.
        kv_store_path (str, path): Path to the SQLite table.
        blocking (bool): If true, the function will wait for the HTTP server to
            be healthy, and other components to be ready before returns.
        start_server (bool): If true, `serve.init` starts http server.
            (Default: True)
        http_host (str): Host for HTTP server. Default to "0.0.0.0".
        http_port (int): Port for HTTP server. Default to 8000.
        ray_init_kwargs (dict): Argument passed to ray.init, if there is no ray
            connection. Default to {"object_store_memory": int(1e8)} for
            performance stability reason
        gc_window_seconds(int): How long will we keep the metric data in
            memory. Data older than the gc_window will be deleted. The default
            is 3600 seconds, which is 1 hour.
        queueing_policy(RoutePolicy): Define the queueing policy for selecting
            the backend for a service. (Default: RoutePolicy.Random)
        policy_kwargs: Arguments required to instantiate a queueing policy
    """
    global master_actor
    if master_actor is not None:
        return

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init(**ray_init_kwargs)

    # Register serialization context once
    ray.register_custom_serializer(Query, Query.ray_serialize,
                                   Query.ray_deserialize)

    # Try to get serve master actor if it exists
    try:
        master_actor = ray.util.get_actor(SERVE_MASTER_NAME)
        return
    except ValueError:
        pass

    # Register serialization context once
    ray.register_custom_serializer(Query, Query.ray_serialize,
                                   Query.ray_deserialize)
    ray.register_custom_serializer(RequestMetadata,
                                   RequestMetadata.ray_serialize,
                                   RequestMetadata.ray_deserialize)

    if kv_store_path is None:
        _, kv_store_path = mkstemp()

    # Serve has not been initialized, perform init sequence
    # TODO move the db to session_dir.
    #    ray.worker._global_node.address_info["session_dir"]
    def kv_store_connector(namespace):
        return SQLiteKVStore(namespace, db_path=kv_store_path)

    master_actor = ServeMaster.options(
        detached=True,
        name=SERVE_MASTER_NAME,
        max_reconstructions=ray.ray_constants.INFINITE_RECONSTRUCTION,
    ).remote(kv_store_connector, queueing_policy.value, policy_kwargs,
             start_server, http_host, http_port, gc_window_seconds)

    if start_server and blocking:
        block_until_http_ready("http://{}:{}/-/routes".format(
            http_host, http_port))


@_ensure_connected
def create_endpoint(endpoint_name, route=None, methods=["GET"]):
    """Create a service endpoint given route_expression.

    Args:
        endpoint_name (str): A name to associate to the endpoint. It will be
            used as key to set traffic policy.
        route (str): A string begin with "/". HTTP server will use
            the string to match the path.
        blocking (bool): If true, the function will wait for service to be
            registered before returning
    """
    retry_actor_failures(master_actor.create_endpoint, route, endpoint_name,
                         [m.upper() for m in methods])


@_ensure_connected
def delete_endpoint(endpoint):
    """Delete the given endpoint.

    Does not delete any associated backends.
    """
    retry_actor_failures(master_actor.delete_endpoint, endpoint)


@_ensure_connected
def update_backend_config(backend_tag, config_options):
    """Update a backend configuration for a backend tag.

    Keys not specified in the passed will be left unchanged.

    Args:
        backend_tag(str): A registered backend.
        config_options(dict): Backend config options to update.
    """
    if not isinstance(config_options, dict):
        raise ValueError("config_options must be a dictionary.")
    retry_actor_failures(master_actor.update_backend_config, backend_tag,
                         config_options)


@_ensure_connected
def get_backend_config(backend_tag):
    """Get the backend configuration for a backend tag.

    Args:
        backend_tag(str): A registered backend.
    """
    return retry_actor_failures(master_actor.get_backend_config, backend_tag)


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
        config: (optional) configuration options for this backend.
    """
    if config is None:
        config = {}
    if not isinstance(config, dict):
        raise TypeError("config must be a dictionary.")

    replica_config = ReplicaConfig(
        func_or_class, *actor_init_args, ray_actor_options=ray_actor_options)
    backend_config = BackendConfig(config, replica_config.accepts_batches)

    retry_actor_failures(master_actor.create_backend, backend_tag,
                         backend_config, replica_config)


@_ensure_connected
def delete_backend(backend_tag):
    """Delete the given backend.

    The backend must not currently be used by any endpoints.
    """
    retry_actor_failures(master_actor.delete_backend, backend_tag)


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
    retry_actor_failures(master_actor.set_traffic, endpoint_name,
                         traffic_policy_dictionary)


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
        assert endpoint_name in retry_actor_failures(
            master_actor.get_all_endpoints)

    return RayServeHandle(
        retry_actor_failures(master_actor.get_router)[0],
        endpoint_name,
        relative_slo_ms,
        absolute_slo_ms,
    )


@_ensure_connected
def stat(percentiles=[50, 90, 95],
         agg_windows_seconds=[10, 60, 300, 600, 3600]):
    """Retrieve metric statistics about ray serve system.

    Args:
        percentiles(List[int]): The percentiles for aggregation operations.
            Default is 50th, 90th, 95th percentile.
        agg_windows_seconds(List[int]): The aggregation windows in seconds.
            The longest aggregation window must be shorter or equal to the
            gc_window_seconds.
    """
    [monitor] = retry_actor_failures(master_actor.get_metric_monitor)
    return ray.get(monitor.collect.remote(percentiles, agg_windows_seconds))
