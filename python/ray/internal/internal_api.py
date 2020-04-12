import ray.worker
from ray import profiling

__all__ = ["free", "global_gc"]


def global_gc():
    """Trigger gc.collect() on all workers in the cluster."""

    worker = ray.worker.global_worker
    worker.core_worker.global_gc()


def memory_summary():
    """Returns a formatted string describing memory usage in the cluster."""

    import grpc
    from ray.core.generated import node_manager_pb2
    from ray.core.generated import node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info.
    raylet = ray.nodes()[0]
    raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                    ray.nodes()[0]["NodeManagerPort"])
    channel = grpc.insecure_channel(raylet_address)
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    reply = stub.FormatGlobalMemoryInfo(
        node_manager_pb2.FormatGlobalMemoryInfoRequest(), timeout=30.0)
    return reply.memory_summary


def free(object_ids, local_only=False, delete_creating_tasks=False):
    """Free a list of IDs from object stores.

    This function is a low-level API which should be used in restricted
    scenarios.

    If local_only is false, the request will be send to all object stores.

    This method will not return any value to indicate whether the deletion is
    successful or not. This function is an instruction to object store. If
    the some of the objects are in use, object stores will delete them later
    when the ref count is down to 0.

    Examples:
        >>> x_id = f.remote()
        >>> ray.get(x_id)  # wait for x to be created first
        >>> free([x_id])  # unpin & delete x globally

    Args:
        object_ids (List[ObjectID]): List of object IDs to delete.
        local_only (bool): Whether only deleting the list of objects in local
            object store or all object stores.
        delete_creating_tasks (bool): Whether also delete the object creating
            tasks.
    """
    worker = ray.worker.global_worker

    if isinstance(object_ids, ray.ObjectID):
        object_ids = [object_ids]

    if not isinstance(object_ids, list):
        raise TypeError("free() expects a list of ObjectID, got {}".format(
            type(object_ids)))

    # Make sure that the values are object IDs.
    for object_id in object_ids:
        if not isinstance(object_id, ray.ObjectID):
            raise TypeError("Attempting to call `free` on the value {}, "
                            "which is not an ray.ObjectID.".format(object_id))

    worker.check_connected()
    with profiling.profile("ray.free"):
        if len(object_ids) == 0:
            return

        worker.core_worker.free_objects(object_ids, local_only,
                                        delete_creating_tasks)
