import ray


def _get_current_node_resource_key() -> str:
    """Get the Ray resource key for current node.
    It can be used for actor placement.
    If using Ray Client, this will return the resource key for the node that
    is running the client server.
    Returns:
        (str) A string of the format node:<CURRENT-NODE-IP-ADDRESS>
    """
    current_node_id = ray.get_runtime_context().get_node_id()
    for node in ray.nodes():
        if node["NodeID"] == current_node_id:
            # Found the node.
            for key in node["Resources"].keys():
                if key.startswith("node:"):
                    return key
    else:
        raise ValueError("Cannot found the node dictionary for current node.")


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
    node_resource_key = _get_current_node_resource_key()
    options = {"resources": {node_resource_key: 0.01}}

    if task_or_actor is None:
        return options

    return task_or_actor.options(**options)
