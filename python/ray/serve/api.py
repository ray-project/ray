import inspect
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
from ray.serve.exceptions import RayServeException, batch_annotation_not_found
from ray.serve.backend_config import BackendConfig
from ray.serve.policy import RoutePolicy
from ray.serve.router import Query
from ray.serve.request_params import RequestMetadata
from ray.serve.metric import InMemorySink

master_actor = None


def _get_master_actor():
    """Used for internal purpose because using just import serve.global_state
    will always reference the original None object.
    """
    global master_actor
    if master_actor is None:
        master_actor = ray.util.get_actor(SERVE_MASTER_NAME)
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
    f.serve_accept_batch = True
    return f


def init(kv_store_connector=None,
         kv_store_path=None,
         blocking=False,
         start_server=True,
         http_host=DEFAULT_HTTP_HOST,
         http_port=DEFAULT_HTTP_PORT,
         ray_init_kwargs={
             "object_store_memory": int(1e8),
             "num_cpus": max(cpu_count(), 8)
         },
         queueing_policy=RoutePolicy.Random,
         policy_kwargs={},
         metric_sink=InMemorySink,
         metric_push_interval=2):
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
        queueing_policy(RoutePolicy): Define the queueing policy for selecting
            the backend for a service. (Default: RoutePolicy.Random)
        policy_kwargs: Arguments required to instantiate a queueing policy
        metric_sink(BaseSink): The metric storage actor for all RayServe actors
            to push to. RayServe has two options built in: InMemorySink and
            PrometheusSink
        metric_push_interval(float): The interval for each actors to push to
            the metric sink. Default is 2s.
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
             start_server, http_host, http_port, metric_sink,
             metric_push_interval)

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
def set_backend_config(backend_tag, backend_config):
    """Set a backend configuration for a backend tag

    Args:
        backend_tag(str): A registered backend.
        backend_config(BackendConfig) : Desired backend configuration.
    """
    retry_actor_failures(master_actor.set_backend_config, backend_tag,
                         backend_config)


@_ensure_connected
def get_backend_config(backend_tag):
    """Get the backend configuration for a backend tag.

    Args:
        backend_tag(str): A registered backend.
    """
    return retry_actor_failures(master_actor.get_backend_config, backend_tag)


def _backend_accept_batch(func_or_class):
    if inspect.isfunction(func_or_class):
        return hasattr(func_or_class, "serve_accept_batch")
    elif inspect.isclass(func_or_class):
        return hasattr(func_or_class.__call__, "serve_accept_batch")


@_ensure_connected
def create_backend(func_or_class,
                   backend_tag,
                   *actor_init_args,
                   backend_config=None):
    """Create a backend using func_or_class and assign backend_tag.

    Args:
        func_or_class (callable, class): a function or a class implementing
            __call__.
        backend_tag (str): a unique tag assign to this backend. It will be used
            to associate services in traffic policy.
        backend_config (BackendConfig): An object defining backend properties
        for starting a backend.
        *actor_init_args (optional): the argument to pass to the class
            initialization method.
    """
    # Configure backend_config
    if backend_config is None:
        backend_config = BackendConfig()
    assert isinstance(backend_config,
                      BackendConfig), ("backend_config must be"
                                       " of instance BackendConfig")

    # Validate that func_or_class is a function or class.
    if inspect.isfunction(func_or_class):
        if len(actor_init_args) != 0:
            raise ValueError(
                "actor_init_args not supported for function backend.")
    elif not inspect.isclass(func_or_class):
        raise ValueError(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)))

    # Make sure the batch size is correct.
    should_accept_batch = backend_config.max_batch_size is not None
    if should_accept_batch and not _backend_accept_batch(func_or_class):
        raise batch_annotation_not_found
    if _backend_accept_batch(func_or_class):
        backend_config.has_accept_batch_annotation = True

    retry_actor_failures(master_actor.create_backend, backend_tag,
                         backend_config, func_or_class, actor_init_args)


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
def stat():
    """Retrieve metric statistics about ray serve system."""
    [metric_sink, _] = ray.get(master_actor.get_metric_sink.remote())
    return ray.get(metric_sink.get_metric.remote())
