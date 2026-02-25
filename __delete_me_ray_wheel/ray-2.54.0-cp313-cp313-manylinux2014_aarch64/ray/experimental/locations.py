from typing import Any, Dict, List

import ray
from ray._raylet import ObjectRef


def get_object_locations(
    obj_refs: List[ObjectRef], timeout_ms: int = -1
) -> Dict[ObjectRef, Dict[str, Any]]:
    """Lookup the locations for a list of objects.

    It returns a dict maps from an object to its location. The dict excludes
    those objects whose location lookup failed.

    Args:
        object_refs (List[ObjectRef]): List of object refs.
        timeout_ms: The maximum amount of time in micro seconds to wait
            before returning. Wait infinitely if it's negative.

    Returns:
        A dict maps from an object to its location. The dict excludes those
        objects whose location lookup failed.

        The location is stored as a dict with following attributes:

        - node_ids (List[str]): The hex IDs of the nodes that have a
          copy of this object. Objects less than 100KB will be in memory
          store not plasma store and therefore will have nodes_id = [].

        - object_size (int): The size of data + metadata in bytes. Can be None if the
          size is unknown yet (e.g. task not completed).

    Raises:
        RuntimeError: if the processes were not started by ray.init().
        ray.exceptions.GetTimeoutError: if it couldn't finish the
            request in time.
    """
    if not ray.is_initialized():
        raise RuntimeError("Ray hasn't been initialized.")
    return ray._private.worker.global_worker.core_worker.get_object_locations(
        obj_refs, timeout_ms
    )


def get_local_object_locations(
    obj_refs: List[ObjectRef],
) -> Dict[ObjectRef, Dict[str, Any]]:
    """Lookup the locations for a list of objects *from the local core worker*. No RPCs
    are made in this method.

    It returns a dict maps from an object to its location. The dict excludes
    those objects whose location lookup failed.

    Args:
        object_refs (List[ObjectRef]): List of object refs.

    Returns:
        A dict maps from an object to its location. The dict excludes those
        objects whose location lookup failed.

        The location is stored as a dict with following attributes:

        - node_ids (List[str]): The hex IDs of the nodes that have a
          copy of this object. Objects less than 100KB will be in memory
          store not plasma store and therefore will have nodes_id = [].

        - object_size (int): The size of data + metadata in bytes. Can be None if the
          size is unknown yet (e.g. task not completed).

    Raises:
        RuntimeError: if the processes were not started by ray.init().
    """
    if not ray.is_initialized():
        raise RuntimeError("Ray hasn't been initialized.")
    core_worker = ray._private.worker.global_worker.core_worker
    return core_worker.get_local_object_locations(obj_refs)
