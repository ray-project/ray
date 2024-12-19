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
