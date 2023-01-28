import ray

def _get_node_id_from_node_ip(node_ip: str):
    """Returns the node ID for the first alive node with the input IP."""
    for node in ray.nodes():
        if node["Alive"] and node["NodeManagerAddress"] == node_ip:
            return node["NodeID"]
    
    return None


def _force_on_current_node(task_or_actor=None):
    """Given a task or actor, place it on the current node.
    If using Ray Client, the current node is the client server node.
    Args:
        task_or_actor: A Ray remote function or class to place on the
            current node. If None, returns the options dict to pass to
            another actor.
    Returns:
        The provided task or actor, but with options modified to force
            placement on the current node.
    """
    current_node_id = ray.get_runtime_context().get_node_id()

    scheduling_strategy = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        node_id=current_node_id,
        soft=False,
    )

    options = {"scheduling_strategy": scheduling_strategy}

    if task_or_actor is None:
        return options

    return task_or_actor.options(**options)
