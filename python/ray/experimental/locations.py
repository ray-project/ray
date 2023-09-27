from typing import Any, Dict, List

import ray
from ray._raylet import ObjectRef


def get_object_locations(
    obj_refs: List[ObjectRef], timeout_ms: int = -1, skip_get_locations: bool = False
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
          copy of this object.

        - object_size (int): The size of data + metadata in bytes.

        - for_metrics (bool): If the use case is purely for metrics.

    Raises:
        RuntimeError: if the processes were not started by ray.init().
        ray.exceptions.GetTimeoutError: if it couldn't finish the
            request in time.
    """
    if not ray.is_initialized():
        raise RuntimeError("Ray hasn't been initialized.")
    # This call can be expensive if not called by the driver.
    # If this is for purely metrics use, we can skip the call for performance.
    if (
        skip_get_locations
        and not ray.data.context.DataContext.get_current().enable_get_object_locations_for_metrics
    ):
        return {
            ref: {"node_ids": [], "object_size": 0, "did_spill": False}
            for ref in obj_refs
        }
    return ray._private.worker.global_worker.core_worker.get_object_locations(
        obj_refs, timeout_ms
    )
