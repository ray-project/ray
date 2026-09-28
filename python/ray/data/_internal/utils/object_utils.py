from typing import TYPE_CHECKING, Any, Dict, Set

from ray.data._internal.utils.cached_ray_internals import get_drained_nodes
from ray.experimental import locations

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle

# Objects smaller than this are inlined in the in-memory store instead of plasma,
# so they report no node locations even though they are readily available.
# TODO: Ray Core should provide whether an object is in plasma or not.
DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE = 100 * 1024


def object_does_exist(object_info: Dict[str, Any], drained_nodes: Set[str]) -> bool:
    """Check if an object exists on a non-drained node.

    Args:
        object_info: An entry of ``ray.experimental.get_local_object_locations``,
            containing the object's ``object_size`` and ``node_ids``.
        drained_nodes: Set of node IDs that are draining and past their deadline.

    Returns:
        True if the object is inlined or has a copy on a non-drained node.
    """
    object_size = object_info["object_size"]
    if object_size is not None and object_size < DEFAULT_MAX_DIRECT_CALL_OBJECT_SIZE:
        return True

    return len(set(object_info["node_ids"]) - drained_nodes) > 0


def all_objects_exist_for_bundle(bundle: "RefBundle") -> bool:
    """Check if all blocks in a bundle exist on non-drained nodes.

    Only the local core worker's view of object locations is consulted, so no
    RPCs are made.
    """
    object_locs = locations.get_local_object_locations(
        bundle.block_refs  # pyrefly: ignore[bad-argument-type]
    )
    drained_nodes = get_drained_nodes()
    return all(
        object_does_exist(obj_info, drained_nodes) for obj_info in object_locs.values()
    )
