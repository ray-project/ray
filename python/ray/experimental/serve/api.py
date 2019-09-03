import inspect

import numpy as np

import ray
from ray.experimental.serve.task_runner import RayServeMixin, TaskRunnerActor
from ray.experimental.serve.utils import pformat_color_json, logger
from ray.experimental.serve.global_state import GlobalState

global_state = GlobalState()


def init(blocking=False, object_store_memory=int(1e8)):
    """Initialize a ray serve cluster.

    Args:
        blocking (bool): If true, the function will wait for HTTP server to be
            healthy before returns
        object_store_memory (int): Allocated shared memory size. The default
            is 100MB. Unit in bytes.
    """
    if not ray.is_initialized():
        ray.init(object_store_memory=object_store_memory)

    # Currently the initialization order is fixed.
    global_state.init_api_server()
    global_state.init_router()
    global_state.init_http_server()

    if blocking:
        global_state.wait_until_http_ready()


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
    future = global_state.api_handle.register_service.remote(
        route_expression, endpoint_name)
    if blocking:
        ray.get(future)
    global_state.registered_endpoints.append(endpoint_name)


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
        runner = TaskRunnerActor.remote(func_or_class)
    elif inspect.isclass(func_or_class):

        @ray.remote
        class CustomActor(func_or_class, RayServeMixin):
            pass

        runner = CustomActor.remote(*actor_init_args)
    else:
        raise Exception(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)))

    global_state.actor_nursery.append(runner)

    runner.setup.remote(backend_tag, global_state.router)
    runner.main_loop.remote(runner)

    global_state.registered_backends.append(backend_tag)


def link(endpoint_name, backend_tag):
    """Associate a service endpoint with backend tag.

    Usage:

    >>> srv.link("service-name", "backend:v1")

    Note:
    This is equivalent to

    >>> srv.split("service-name", {"backend:v1": 1.0})
    """
    assert endpoint_name in global_state.registered_endpoints

    global_state.router.link.remote(endpoint_name, backend_tag)
    global_state.policy_action_history[endpoint_name].append({backend_tag: 1})


def split(endpoint_name, traffic_policy_dictionary):
    """Associate a service endpoint with traffic policy.

    Usage:

    >>> srv.split("service-name", {
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
        prob, 1,
        atol=0.02), "weights must sum to 1, currently it sums to {}".format(
            prob)

    global_state.router.set_traffic.remote(endpoint_name,
                                           traffic_policy_dictionary)
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

    logger.warning(f"""
Current traffic policy is:
{pformat_color_json(cur_policy)}

Will rollback to:
{pformat_color_json(prev_policy)}
""")

    action_queues.pop()
    global_state.router.set_traffic.remote(endpoint_name, prev_policy)


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

    return RayServeHandle(global_state.router, endpoint_name)
