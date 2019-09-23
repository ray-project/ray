import inspect

import numpy as np

import ray
from ray.experimental.serve.task_runner import RayServeMixin, TaskRunnerActor
from ray.experimental.serve.utils import pformat_color_json, logger
from ray.experimental.serve.global_state import GlobalState

global_state = GlobalState()


def init(blocking=False, object_store_memory=int(1e8)):
    """Initialize a serve cluster.

    Calling `ray.init` before `serve.init` is optional. When there is not a ray
    cluster initialized, serve will call `ray.init` with `object_store_memory`
    requirement.

    Args:
        blocking (bool): If true, the function will wait for the HTTP server to
            be healthy before returns.
        object_store_memory (int): Allocated shared memory size in bytes. The
            default is 100MiB. The default is kept low for latency stability
            reason.
    """
    if not ray.is_initialized():
        ray.init(object_store_memory=object_store_memory)

    # NOTE(simon): Currently the initialization order is fixed.
    # HTTP server depends on the API server.
    global_state.init_api_server()
    global_state.init_router()
    global_state.init_http_server()

    # Monitor depends on the router to be initialized properly.
    global_state.init_monitor()
    global_state.monitor_handle.add_target.remote(
        global_state.router_actor_handle)

    if blocking:
        global_state.wait_until_http_ready()


def stat():
    """Retrieve metric statistics from all components"""
    # TODO(simon): this function should accept parameters including
    # endpoint_name, backend_name, compoenent_name ("router")
    return ray.get(global_state.monitor_handle.get_stats.remote())


def create_endpoint(endpoint_name, route_expression, blocking=True):
    """Create a service endpoint given route_expression.

    Args:
        endpoint_name (str): A name to associate to the endpoint. It will be
            used as key to set traffic policy.
        route_expression (str): A string begin with "/". HTTP server will use
            the string to match the path.
        blocking (bool): If true, the function will wait for service to be
            registered before returning
    """
    future = global_state.kv_store_actor_handle.register_service.remote(
        route_expression, endpoint_name)
    if blocking:
        ray.get(future)
    global_state.registered_endpoints.add(endpoint_name)


def create_backend(func_or_class, backend_tag, *actor_init_args):
    """Create a backend using func_or_class and assign backend_tag.

    Args:
        func_or_class (callable, class): a function or a class implements
            __call__ protocol.
        backend_tag (str): a unique tag assign to this backend. It will be used
            to associate services in traffic policy.
        *actor_init_args (optional): the argument to pass to the class
            initialization method.
    """
    if inspect.isfunction(func_or_class):
        runner_factory = lambda: TaskRunnerActor.remote(func_or_class)  # noqa: E501 E731
        # For linter trailing comment above: we ignored warning about
        # inline-lambda and line-to-long
    elif inspect.isclass(func_or_class):
        assert hasattr(func_or_class,
                       "__call__"), "Backend class must implement __call__"

        # Python inheritance order is right-to-left. We put RayServeMixin
        # on the left to make sure its methods are not overriden.
        @ray.remote
        class CustomActor(RayServeMixin, func_or_class):
            pass

        runner_factory = lambda: CustomActor.remote(*actor_init_args)  # noqa: E501 E731
    else:
        raise TypeError(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)))

    global_state.backend_actor_facotry[backend_tag] = runner_factory
    global_state.registered_backends.add(backend_tag)
    set_replica(backend_tag, 1)


def set_replica(backend_tag, new_replica):
    old_replica = global_state.num_replicas[backend_tag]
    if new_replica > old_replica:
        for _ in range(new_replica - old_replica):
            replica_id = global_state.replica_id_counter[backend_tag]
            _add_replica(backend_tag, replica_id)
            global_state.active_replicas[backend_tag].add(replica_id)
            global_state.replica_id_counter[backend_tag] += 1
    elif new_replica < old_replica:
        for _ in range(old_replica - new_replica):
            replica_to_be_removed = global_state.active_replicas[
                backend_tag].pop()
            blacklist(backend_tag, replica_to_be_removed)
            _remove_replica(backend_tag, replica_to_be_removed)

    global_state.num_replicas[backend_tag] = new_replica
    global_state.router_actor_handle.flush.remote()


def blacklist(backend_tag, replica_id):
    global_state.router_actor_handle.black_list.remote(backend_tag, replica_id)


def _add_replica(backend_tag, replica_id):
    assert backend_tag in global_state.registered_backends
    runner_actor_handle = global_state.backend_actor_facotry[backend_tag]()
    runner_actor_handle._ray_serve_setup.remote(
        backend_tag, global_state.router_actor_handle, replica_id)
    runner_actor_handle._ray_serve_main_loop.remote(runner_actor_handle)
    global_state.backend_actor_handles[(backend_tag,
                                        replica_id)] = runner_actor_handle
    global_state.monitor_handle.add_target.remote(runner_actor_handle)


def _remove_replica(backend_tag, replica_id):
    runner_actor_handle = global_state.backend_actor_handles.pop((backend_tag,
                                                                  replica_id))
    global_state.monitor_handle.remove_target.remote(runner_actor_handle)
    del runner_actor_handle


def link(endpoint_name, backend_tag):
    """Associate a service endpoint with backend tag.

    Example:

    >>> serve.link("service-name", "backend:v1")

    Note:
    This is equivalent to

    >>> serve.split("service-name", {"backend:v1": 1.0})
    """
    assert endpoint_name in global_state.registered_endpoints

    global_state.router_actor_handle.link.remote(endpoint_name, backend_tag)
    global_state.policy_action_history[endpoint_name].append({backend_tag: 1})


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

    # Perform dictionary checks
    assert endpoint_name in global_state.registered_endpoints

    assert isinstance(traffic_policy_dictionary,
                      dict), "Traffic policy must be dictionary"
    prob = 0
    for backend, weight in traffic_policy_dictionary.items():
        prob += weight
        assert (backend in global_state.registered_backends
                ), "backend {} is not registered".format(backend)
    assert np.isclose(
        prob, 1), "weights must sum to 1, currently it sums to {}".format(prob)

    global_state.router_actor_handle.set_traffic.remote(
        endpoint_name, traffic_policy_dictionary)
    global_state.policy_action_history[endpoint_name].append(
        traffic_policy_dictionary)


def rollback(endpoint_name):
    """Rollback a traffic policy decision.

    Args:
        endpoint_name (str): A registered service endpoint.
    """
    assert endpoint_name in global_state.registered_endpoints
    action_queues = global_state.policy_action_history[endpoint_name]
    cur_policy, prev_policy = action_queues[-1], action_queues[-2]

    logger.warning("""
Current traffic policy is:
{cur_policy}

Will rollback to:
{prev_policy}
""".format(
        cur_policy=pformat_color_json(cur_policy),
        prev_policy=pformat_color_json(prev_policy)))

    action_queues.pop()
    global_state.router_actor_handle.set_traffic.remote(
        endpoint_name, prev_policy)


def get_handle(endpoint_name):
    """Retrieve RayServeHandle for service endpoint to invoke it from Python.

    Args:
        endpoint_name (str): A registered service endpoint.

    Returns:
        RayServeHandle
    """
    assert endpoint_name in global_state.registered_endpoints

    # Delay import due to it's dependency on global_state
    from ray.experimental.serve.handle import RayServeHandle

    return RayServeHandle(global_state.router_actor_handle, endpoint_name)
