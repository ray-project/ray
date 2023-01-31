from typing import Optional, Union
import ray


def _get_node_id_from_node_ip(node_ip: str) -> Optional[str]:
    """Returns the node ID for the first alive node with the input IP."""
    for node in ray.nodes():
        if node["Alive"] and node["NodeManagerAddress"] == node_ip:
            return node["NodeID"]

    return None


def _force_on_node(
    node_id: str,
    task_or_actor: Optional[
        Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]
    ] = None,
) -> Optional[Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]]:
    """Schedule a task or actor on a given node.

    Args:
        node_id: The node to schedule on.
        task_or_actor: A Ray remote function or actor class to place on the
            input node. If None, returns the options dict to pass to
            another remote function or actor class as remote options.
    Returns:
        The provided task or actor, but with options modified to force
        placement on the input node. If task_or_actor is None,
        the options dict to pass to another remote function or
        actor class as remote options kwargs.
    """

    scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=node_id, soft=False
    )

    options = {"scheduling_strategy": scheduling_strategy}

    if task_or_actor is None:
        return options

    return task_or_actor.options(**options)


def _force_on_current_node(
    task_or_actor: Optional[
        Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]
    ] = None
) -> Optional[Union[ray.remote_function.RemoteFunction, ray.actor.ActorClass]]:
    """Schedule a task or actor on the current node.

    If using Ray Client, the current node is the client server node.

        task_or_actor: A Ray remote function or actor class to place on the
            current node. If None, returns the options dict to pass to
            another remote function or actor class as remote options.
    Returns:
        The provided task or actor, but with options modified to force
        placement on the input node. If task_or_actor is None,
        the options dict to pass to another remote function or
        actor class as remote options kwargs.
    """
    current_node_id = ray.get_runtime_context().get_node_id()
    return _force_on_node(current_node_id, task_or_actor)
