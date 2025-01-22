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


def split_readers_by_node_locality(
    writer_node: str,
    reader_and_node_list: List[Tuple["ray.actor.ActorHandle", str]],
) -> Tuple[
    List[Tuple["ray.actor.ActorHandle", str]], List[Tuple["ray.actor.ActorHandle", str]]
]:
    """Split readers into remote and local readers based on writer.

    Args:
        writer_node: The node of the writer
        reader_and_node_list: List of (reader, node) tuples

    Returns:
        Tuple containing:
            - List of (reader, node) tuples for readers on the same node
            - List of (reader, node) tuples for readers on a different node
    """
    readers_on_same_node = []
    readers_on_different_node = []

    for reader, node in reader_and_node_list:
        if node == writer_node:
            readers_on_same_node.append((reader, node))
        else:
            readers_on_different_node.append((reader, node))

    return readers_on_same_node, readers_on_different_node


def get_writer_node(writer: "ray.actor.ActorHandle"):
    """Get the node of the writer.

    Args:
        writer: The actor handle of the writer

    Returns:
        The node of the writer
    """
    if writer is None or writer == ray.get_runtime_context().current_actor:
        return ray.get_runtime_context().get_node_id()
    else:
        return ray.get(
            writer.__ray_call__.remote(
                lambda self: ray.get_runtime_context().get_node_id()
            )
        )
