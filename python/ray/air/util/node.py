from typing import Dict, Optional, Union

import ray


def _get_node_id_from_node_ip(node_ip: str) -> Optional[str]:
    """Returns the node ID for the first alive node with the input IP."""
    for node in ray.nodes():
        if node["Alive"] and node["NodeManagerAddress"] == node_ip:
            return node["NodeID"]

    return None


def _force_on_node(
    node_id: str,
    remote_func_or_actor_class: Optional[
        Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]
    ] = None,
) -> Union[Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass], Dict]:
    """Schedule a remote function or actor class on a given node.

    Args:
        node_id: The node to schedule on.
        remote_func_or_actor_class: A Ray remote function or actor class
            to schedule on the input node. If None, this function will directly
            return the options dict to pass to another remote function or actor class
            as remote options.
    Returns:
        The provided remote function or actor class, but with options modified to force
        placement on the input node. If remote_func_or_actor_class is None,
        the options dict to pass to another remote function or
        actor class as remote options kwargs.
    """

    scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=node_id, soft=False
    )

    options = {"scheduling_strategy": scheduling_strategy}

    if remote_func_or_actor_class is None:
        return options

    return remote_func_or_actor_class.options(**options)


def _force_on_current_node(
    remote_func_or_actor_class: Optional[
        Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]
    ] = None
) -> Union[Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass], Dict]:
    """Schedule a remote function or actor class on the current node.

    If using Ray Client, the current node is the client server node.

    Args:
        remote_func_or_actor_class: A Ray remote function or actor class
            to schedule on the current node. If None, this function will directly
            return the options dict to pass to another remote function or actor class
            as remote options.
    Returns:
        The provided remote function or actor class, but with options modified to force
        placement on the current node. If remote_func_or_actor_class is None,
        the options dict to pass to another remote function or
        actor class as remote options kwargs.
    """
    current_node_id = ray.get_runtime_context().get_node_id()
    return _force_on_node(current_node_id, remote_func_or_actor_class)
