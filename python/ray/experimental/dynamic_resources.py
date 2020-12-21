import ray


def set_resource(resource_name, capacity, node_id=None):
    """Set a resource to a specified capacity.

    Dynamic custom resources are deprecated and won't work unless
    RAY_ENABLE_NEW_SCHEDULER=0 is set; consider using placement groups
    instead (docs.ray.io/en/master/placement-group.html).
    """

    if node_id is not None:
        node_id_obj = ray.NodeID(ray.utils.hex_to_binary(node_id))
    else:
        node_id_obj = ray.NodeID.nil()
    if (capacity < 0) or (capacity != int(capacity)):
        raise ValueError(
            "Capacity {} must be a non-negative integer.".format(capacity))
    return ray.worker.global_worker.core_worker.set_resource(
        resource_name, capacity, node_id_obj)
