import ray
from ray._raylet import ObjectRef
from typing import Any, Dict, List


def get_object_locations(
    obj_refs: List[ObjectRef], timeout_ms: int = -1
) -> Dict[ObjectRef, Dict[str, Any]]:
    """Lookup the locations for a list of objects.

    It returns a dict maps from an object to its location. The dict excludes
    those objects whose location lookup failed.

    Args:
        object_refs (List[ObjectRef]): List of object refs.
        timeout_ms (int): The maximum amount of time in micro seconds to wait
            before returning. Wait infinitely if it's negative.

    Returns:
        A dict maps from an object to its location. The dict excludes those
        objects whose location lookup failed.

        The location is stored as a dict with following attributes:

        - node_ids (List[str]): The hex IDs of the nodes that have a
          copy of this object.

        - object_size (int): The size of data + metadata in bytes.

    Raises:
        RuntimeError: if the processes were not started by ray.init().
        ray.exceptions.GetTimeoutError: if it couldn't finish the
            request in time.
    """
    if not ray.is_initialized():
        raise RuntimeError("Ray hasn't been initialized.")
    return ray.worker.global_worker.core_worker.get_object_locations(
        obj_refs, timeout_ms
    )
