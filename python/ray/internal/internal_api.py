import ray
import ray._private.services as services
import ray.worker
from ray import profiling
from ray import ray_constants
from ray.state import GlobalState

__all__ = ["free", "global_gc"]
MAX_MESSAGE_LENGTH = ray._config.max_grpc_message_size()


def global_gc():
    """Trigger gc.collect() on all workers in the cluster."""

    worker = ray.worker.global_worker
    worker.core_worker.global_gc()


def memory_summary(address=None,
                   redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
                   group_by="NODE_ADDRESS",
                   sort_by="OBJECT_SIZE",
                   line_wrap=True,
                   stats_only=False):
    from ray.new_dashboard.memory_utils import memory_summary
    if not address:
        address = services.get_ray_address_to_use_or_die()
    state = GlobalState()
    state._initialize_global_state(address, redis_password)
    if stats_only:
        return get_store_stats(state)
    return (memory_summary(state, group_by, sort_by, line_wrap) +
            get_store_stats(state))


def get_store_stats(state, node_manager_address=None, node_manager_port=None):
    """Returns a formatted string describing memory usage in the cluster."""

    import grpc
    from ray.core.generated import node_manager_pb2
    from ray.core.generated import node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info, that Raylet internally
    # asks all nodes in the cluster for memory stats.
    if (node_manager_address is None or node_manager_port is None):
        raylet = state.node_table()[0]
        raylet_address = "{}:{}".format(raylet["NodeManagerAddress"],
                                        raylet["NodeManagerPort"])
    else:
        raylet_address = "{}:{}".format(node_manager_address,
                                        node_manager_port)
    channel = grpc.insecure_channel(
        raylet_address,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    reply = stub.FormatGlobalMemoryInfo(
        node_manager_pb2.FormatGlobalMemoryInfoRequest(
            include_memory_info=False),
        timeout=30.0)
    return store_stats_summary(reply)


def node_stats(node_manager_address=None,
               node_manager_port=None,
               include_memory_info=True):
    """Returns NodeStats object describing memory usage in the cluster."""

    import grpc
    from ray.core.generated import node_manager_pb2
    from ray.core.generated import node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info.
    assert (node_manager_address is not None and node_manager_port is not None)
    raylet_address = "{}:{}".format(node_manager_address, node_manager_port)
    channel = grpc.insecure_channel(
        raylet_address,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )
    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    node_stats = stub.GetNodeStats(
        node_manager_pb2.GetNodeStatsRequest(
            include_memory_info=include_memory_info),
        timeout=30.0)
    return node_stats


def store_stats_summary(reply):
    """Returns formatted string describing object store stats in all nodes."""
    store_summary = "--- Aggregate object store stats across all nodes ---\n"
    store_summary += (
        "Plasma memory usage {} MiB, {} objects, {}% full\n".format(
            int(reply.store_stats.object_store_bytes_used / (1024 * 1024)),
            reply.store_stats.num_local_objects,
            round(
                100 * reply.store_stats.object_store_bytes_used /
                reply.store_stats.object_store_bytes_avail, 2)))
    if reply.store_stats.spill_time_total_s > 0:
        store_summary += (
            "Spilled {} MiB, {} objects, avg write throughput {} MiB/s\n".
            format(
                int(reply.store_stats.spilled_bytes_total / (1024 * 1024)),
                reply.store_stats.spilled_objects_total,
                int(reply.store_stats.spilled_bytes_total / (1024 * 1024) /
                    reply.store_stats.spill_time_total_s)))
    if reply.store_stats.restore_time_total_s > 0:
        store_summary += (
            "Restored {} MiB, {} objects, avg read throughput {} MiB/s\n".
            format(
                int(reply.store_stats.restored_bytes_total / (1024 * 1024)),
                reply.store_stats.restored_objects_total,
                int(reply.store_stats.restored_bytes_total / (1024 * 1024) /
                    reply.store_stats.restore_time_total_s)))
    if reply.store_stats.consumed_bytes > 0:
        store_summary += ("Objects consumed by Ray tasks: {} MiB.".format(
            int(reply.store_stats.consumed_bytes / (1024 * 1024))))
    return store_summary


def free(object_refs, local_only=False):
    """Free a list of IDs from the in-process and plasma object stores.

    This function is a low-level API which should be used in restricted
    scenarios.

    If local_only is false, the request will be send to all object stores.

    This method will not return any value to indicate whether the deletion is
    successful or not. This function is an instruction to the object store. If
    some of the objects are in use, the object stores will delete them later
    when the ref count is down to 0.

    Examples:
        >>> x_id = f.remote()
        >>> ray.get(x_id)  # wait for x to be created first
        >>> free([x_id])  # unpin & delete x globally

    Args:
        object_refs (List[ObjectRef]): List of object refs to delete.
        local_only (bool): Whether only deleting the list of objects in local
            object store or all object stores.
    """
    worker = ray.worker.global_worker

    if isinstance(object_refs, ray.ObjectRef):
        object_refs = [object_refs]

    if not isinstance(object_refs, list):
        raise TypeError("free() expects a list of ObjectRef, got {}".format(
            type(object_refs)))

    # Make sure that the values are object refs.
    for object_ref in object_refs:
        if not isinstance(object_ref, ray.ObjectRef):
            raise TypeError(
                "Attempting to call `free` on the value {}, "
                "which is not an ray.ObjectRef.".format(object_ref))

    worker.check_connected()
    with profiling.profile("ray.free"):
        if len(object_refs) == 0:
            return

        worker.core_worker.free_objects(object_refs, local_only)
