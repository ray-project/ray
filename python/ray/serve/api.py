import inspect
from functools import wraps
from tempfile import mkstemp

from multiprocessing import cpu_count

import numpy as np

import ray
from ray.serve.constants import (DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT,
                                 SERVE_NURSERY_NAME)
from ray.serve.global_state import GlobalState, start_initial_state
from ray.serve.kv_store_service import SQLiteKVStore
from ray.serve.task_runner import RayServeMixin, TaskRunnerActor
from ray.serve.utils import block_until_http_ready, get_random_letters, expand
from ray.serve.exceptions import RayServeException, batch_annotation_not_found
from ray.serve.backend_config import BackendConfig
from ray.serve.policy import RoutePolicy
from ray.serve.queues import Query
from ray.serve.request_params import RequestMetadata

global_state = None


def _get_global_state():
    """Used for internal purpose. Because just import serve.global_state
    will always reference the original None object
    """
    return global_state


def _ensure_connected(f):
    @wraps(f)
    def check(*args, **kwargs):
        if _get_global_state() is None:
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
    global global_state
    # Noop if global_state is no longer None
    if global_state is not None:
        return

    # Initialize ray if needed.
    if not ray.is_initialized():
        ray.init(**ray_init_kwargs)

    # Try to get serve nursery if there exists
    try:
        ray.util.get_actor(SERVE_NURSERY_NAME)
        global_state = GlobalState()
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
    # TODO move the db to session_dir
    #    ray.worker._global_node.address_info["session_dir"]
    def kv_store_connector(namespace):
        return SQLiteKVStore(namespace, db_path=kv_store_path)

    nursery = start_initial_state(kv_store_connector)

    global_state = GlobalState(nursery)
    if start_server:
        global_state.init_or_get_http_server(host=http_host, port=http_port)
    global_state.init_or_get_router(
        queueing_policy=queueing_policy, policy_kwargs=policy_kwargs)
    global_state.init_or_get_metric_monitor(
        gc_window_seconds=gc_window_seconds)

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
    methods = [m.upper() for m in methods]
    global_state.route_table.register_service(
        route, endpoint_name, methods=methods)


@_ensure_connected
def set_backend_config(backend_tag, backend_config):
    """Set a backend configuration for a backend tag

    Args:
        backend_tag(str): A registered backend.
        backend_config(BackendConfig) : Desired backend configuration.
    """
    assert (backend_tag in global_state.backend_table.list_backends()
            ), "Backend {} is not registered.".format(backend_tag)
    assert isinstance(backend_config,
                      BackendConfig), ("backend_config must be"
                                       " of instance BackendConfig")
    backend_config_dict = dict(backend_config)
    old_backend_config_dict = global_state.backend_table.get_info(backend_tag)

    if (not old_backend_config_dict["has_accept_batch_annotation"]
            and backend_config.max_batch_size is not None):
        raise batch_annotation_not_found

    global_state.backend_table.register_info(backend_tag, backend_config_dict)

    # inform the router about change in configuration
    # particularly for setting max_batch_size
    ray.get(global_state.init_or_get_router().set_backend_config.remote(
        backend_tag, backend_config_dict))

    # checking if replicas need to be restarted
    # Replicas are restarted if there is any change in the backend config
    # related to restart_configs
    # TODO(alind) : have replica restarting policies selected by the user

    need_to_restart_replicas = any(
        old_backend_config_dict[k] != backend_config_dict[k]
        for k in BackendConfig.restart_on_change_fields)
    if need_to_restart_replicas:
        # kill all the replicas for restarting with new configurations
        _scale(backend_tag, 0)

    # scale the replicas with new configuration
    _scale(backend_tag, backend_config_dict["num_replicas"])


@_ensure_connected
def get_backend_config(backend_tag):
    """get the backend configuration for a backend tag

    Args:
        backend_tag(str): A registered backend.
    """
    assert (backend_tag in global_state.backend_table.list_backends()
            ), "Backend {} is not registered.".format(backend_tag)
    backend_config_dict = global_state.backend_table.get_info(backend_tag)
    return BackendConfig(**backend_config_dict)


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
        func_or_class (callable, class): a function or a class implements
            __call__ protocol.
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

    # Make sure the batch size is correct
    should_accept_batch = backend_config.max_batch_size is not None
    if should_accept_batch and not _backend_accept_batch(func_or_class):
        raise batch_annotation_not_found
    if _backend_accept_batch(func_or_class):
        backend_config.has_accept_batch_annotation = True

    arg_list = []
    if inspect.isfunction(func_or_class):
        # arg list for a fn is function itself
        arg_list = [func_or_class]
        # ignore lint on lambda expression
        creator = lambda kwrgs: TaskRunnerActor._remote(**kwrgs)  # noqa: E731
    elif inspect.isclass(func_or_class):
        # Python inheritance order is right-to-left. We put RayServeMixin
        # on the left to make sure its methods are not overriden.
        @ray.remote
        class CustomActor(RayServeMixin, func_or_class):
            @wraps(func_or_class.__init__)
            def __init__(self, *args, **kwargs):
                init()  # serve init
                super().__init__(*args, **kwargs)

        arg_list = actor_init_args
        # ignore lint on lambda expression
        creator = lambda kwargs: CustomActor._remote(**kwargs)  # noqa: E731
    else:
        raise TypeError(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)))

    backend_config_dict = dict(backend_config)

    # save creator which starts replicas
    global_state.backend_table.register_backend(backend_tag, creator)

    # save information about configurations needed to start the replicas
    global_state.backend_table.register_info(backend_tag, backend_config_dict)

    # save the initial arguments needed by replicas
    global_state.backend_table.save_init_args(backend_tag, arg_list)

    # set the backend config inside the router
    # particularly for max-batch-size
    ray.get(global_state.init_or_get_router().set_backend_config.remote(
        backend_tag, backend_config_dict))
    _scale(backend_tag, backend_config_dict["num_replicas"])


def _start_replica(backend_tag):
    assert (backend_tag in global_state.backend_table.list_backends()
            ), "Backend {} is not registered.".format(backend_tag)

    replica_tag = "{}#{}".format(backend_tag, get_random_letters(length=6))

    # get the info which starts the replicas
    creator = global_state.backend_table.get_backend_creator(backend_tag)
    backend_config_dict = global_state.backend_table.get_info(backend_tag)
    backend_config = BackendConfig(**backend_config_dict)
    init_args = global_state.backend_table.get_init_args(backend_tag)

    # get actor creation kwargs
    actor_kwargs = backend_config.get_actor_creation_args(init_args)

    # Create the runner in the nursery
    [runner_handle] = ray.get(
        global_state.actor_nursery_handle.start_actor_with_creator.remote(
            creator, actor_kwargs, replica_tag))

    # Setup the worker
    ray.get(
        runner_handle._ray_serve_setup.remote(
            backend_tag, global_state.init_or_get_router(), runner_handle))
    runner_handle._ray_serve_fetch.remote()

    # Register the worker in config tables as well as metric monitor
    global_state.backend_table.add_replica(backend_tag, replica_tag)
    global_state.init_or_get_metric_monitor().add_target.remote(runner_handle)


def _remove_replica(backend_tag):
    assert (backend_tag in global_state.backend_table.list_backends()
            ), "Backend {} is not registered.".format(backend_tag)
    assert (
        len(global_state.backend_table.list_replicas(backend_tag)) >
        0), "Backend {} does not have enough replicas to be removed.".format(
            backend_tag)

    replica_tag = global_state.backend_table.remove_replica(backend_tag)
    [replica_handle] = ray.get(
        global_state.actor_nursery_handle.get_handle.remote(replica_tag))

    # Remove the replica from metric monitor.
    ray.get(global_state.init_or_get_metric_monitor().remove_target.remote(
        replica_handle))

    # Remove the replica from actor nursery.
    ray.get(
        global_state.actor_nursery_handle.remove_handle.remote(replica_tag))

    # Remove the replica from router.
    # This will also destory the actor handle.
    ray.get(
        global_state.init_or_get_router().remove_and_destory_replica.remote(
            backend_tag, replica_handle))


@_ensure_connected
def _scale(backend_tag, num_replicas):
    """Set the number of replicas for backend_tag.

    Args:
        backend_tag (str): A registered backend.
        num_replicas (int): Desired number of replicas
    """
    assert (backend_tag in global_state.backend_table.list_backends()
            ), "Backend {} is not registered.".format(backend_tag)
    assert num_replicas >= 0, ("Number of replicas must be"
                               " greater than or equal to 0.")

    replicas = global_state.backend_table.list_replicas(backend_tag)
    current_num_replicas = len(replicas)
    delta_num_replicas = num_replicas - current_num_replicas

    if delta_num_replicas > 0:
        for _ in range(delta_num_replicas):
            _start_replica(backend_tag)
    elif delta_num_replicas < 0:
        for _ in range(-delta_num_replicas):
            _remove_replica(backend_tag)


@_ensure_connected
def link(endpoint_name, backend_tag):
    """Associate a service endpoint with backend tag.

    Example:

    >>> serve.link("service-name", "backend:v1")

    Note:
    This is equivalent to

    >>> serve.split("service-name", {"backend:v1": 1.0})
    """
    split(endpoint_name, {backend_tag: 1.0})


@_ensure_connected
def split(endpoint_name, traffic_policy_dictionary):
    """Associate a service endpoint with traffic policy.

    Example:

    >>> serve.split("service-name", {
        "backend:v1": 0.5,
        "backend:v2": 0.5
    })

    Args:
        endpoint_name (str): A registered service endpoint.
        traffic_policy_dictionary (dict): a dictionary maps backend names
            to their traffic weights. The weights must sum to 1.
    """
    assert endpoint_name in expand(
        global_state.route_table.list_service(include_headless=True).values())

    assert isinstance(traffic_policy_dictionary,
                      dict), "Traffic policy must be dictionary"
    prob = 0
    for backend, weight in traffic_policy_dictionary.items():
        prob += weight
        assert (backend in global_state.backend_table.list_backends()
                ), "backend {} is not registered".format(backend)
    assert np.isclose(
        prob, 1,
        atol=0.02), "weights must sum to 1, currently it sums to {}".format(
            prob)

    global_state.policy_table.register_traffic_policy(
        endpoint_name, traffic_policy_dictionary)
    ray.get(global_state.init_or_get_router().set_traffic.remote(
        endpoint_name, traffic_policy_dictionary))


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
        assert endpoint_name in expand(
            global_state.route_table.list_service(
                include_headless=True).values())

    # Delay import due to it's dependency on global_state
    from ray.serve.handle import RayServeHandle

    return RayServeHandle(
        global_state.init_or_get_router(),
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
    return ray.get(global_state.init_or_get_metric_monitor().collect.remote(
        percentiles, agg_windows_seconds))


class route:
    """Convient method to create a backend and link to service.

    When called, the following will happen:
    - An endpoint is created with the same of the function
    - A backend is created and instantiate the function
    - The endpoint and backend are linked together
    - The handle is returned

    .. code-block:: python

        @serve.route("/path")
        def my_handler(flask_request):
            ...
    """

    def __init__(self, url_route):
        self.route = url_route

    def __call__(self, func_or_class):
        name = func_or_class.__name__
        backend_tag = "{}:v0".format(name)

        create_backend(func_or_class, backend_tag)
        create_endpoint(name, self.route)
        link(name, backend_tag)

        return get_handle(name)
