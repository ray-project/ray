from typing import List, Optional, Tuple

import ray


def get_self_actor() -> Optional["ray.actor.ActorHandle"]:
    """
    Get the current actor handle in this worker.
    If this is called in a driver process, it will return None.
    """
    try:
        return ray.get_runtime_context().current_actor
    except RuntimeError:
        return None


def split_readers_by_locality(
    writer: "ray.actor.ActorHandle",
    reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
) -> Tuple[
    List[Tuple["ray.actor.ActorHandle", str]], List[Tuple["ray.actor.ActorHandle", str]]
]:
    """Split readers into remote and local readers based on writer.

    Args:
        writer: The actor handle of the writer
        reader_and_node_list: List of (reader, node) tuples

    Returns:
        Tuple containing:
            - List of (reader, node) tuples for remote readers
            - List of (reader, node) tuples for local readers
    """
    remote_readers = []
    local_readers = []

    for reader, node in reader_and_node_list:
        if reader != writer:
            remote_readers.append((reader, node))
        else:
            local_readers.append((reader, node))

    return remote_readers, local_readers


def split_actors_by_node_locality(
    node: str,
    actor_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
) -> Tuple[
    List[Tuple["ray.actor.ActorHandle", str]], List[Tuple["ray.actor.ActorHandle", str]]
]:
    """Split actors into remote and local actors based on node. The local actors will be
    on the same node as the given node. The remote actors will be on a different node.

    Args:
        writer_node: The node of the writer
        actor_and_node_list: List of (actor, node) tuples

    Returns:
        Tuple containing:
            - List of (actor, node) tuples for actors on the same node
            - List of (actor, node) tuples for actors on a different node
    """
    actors_on_same_node = []
    actors_on_different_node = []

    for actor, actor_node in actor_and_node_list:
        if node == actor_node:
            actors_on_same_node.append((actor, actor_node))
        else:
            actors_on_different_node.append((actor, actor_node))

    return actors_on_same_node, actors_on_different_node


def get_actor_node(actor: Optional["ray.actor.ActorHandle"]) -> str:
    """Get the node of the actor.

    Args:
        actor: The actor handle of the actor

    Returns:
        The node of the actor
    """
    if actor is None or actor == ray.get_runtime_context().current_actor:
        return ray.get_runtime_context().get_node_id()
    else:
        return ray.get(
            actor.__ray_call__.remote(
                lambda self: ray.get_runtime_context().get_node_id()
            )
        )
