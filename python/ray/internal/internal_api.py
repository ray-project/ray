import json

import ray
import ray._private.services as services
import ray.worker
import ray._private.profiling as profiling
import ray._private.utils as utils
from ray import ray_constants
from ray.state import GlobalState
from ray._raylet import GcsClientOptions
from typing import List

__all__ = ["free", "global_gc"]
MAX_MESSAGE_LENGTH = ray._config.max_grpc_message_size()


def global_gc():
    """Trigger gc.collect() on all workers in the cluster."""

    worker = ray.worker.global_worker
    worker.core_worker.global_gc()


def memory_summary(
    address=None,
    redis_password=ray_constants.REDIS_DEFAULT_PASSWORD,
    group_by="NODE_ADDRESS",
    sort_by="OBJECT_SIZE",
    units="B",
    line_wrap=True,
    stats_only=False,
    num_entries=None,
):
    from ray.dashboard.memory_utils import memory_summary

    address = services.canonicalize_bootstrap_address(address)

    state = GlobalState()
    options = GcsClientOptions.from_gcs_address(address)
    state._initialize_global_state(options)
    if stats_only:
        return get_store_stats(state)
    return memory_summary(
        state, group_by, sort_by, line_wrap, units, num_entries
    ) + get_store_stats(state)


def get_store_stats(state, node_manager_address=None, node_manager_port=None):
    """Returns a formatted string describing memory usage in the cluster."""

    from ray.core.generated import node_manager_pb2
    from ray.core.generated import node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info, that Raylet internally
    # asks all nodes in the cluster for memory stats.
    if node_manager_address is None or node_manager_port is None:
        # We should ask for a raylet that is alive.
        raylet = None
        for node in state.node_table():
            if node["Alive"]:
                raylet = node
                break
        assert raylet is not None, "Every raylet is dead"
        raylet_address = "{}:{}".format(
            raylet["NodeManagerAddress"], raylet["NodeManagerPort"]
        )
    else:
        raylet_address = "{}:{}".format(node_manager_address, node_manager_port)

    channel = utils.init_grpc_channel(
        raylet_address,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )

    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    reply = stub.FormatGlobalMemoryInfo(
        node_manager_pb2.FormatGlobalMemoryInfoRequest(include_memory_info=False),
        timeout=30.0,
    )
    return store_stats_summary(reply)


def node_stats(
    node_manager_address=None, node_manager_port=None, include_memory_info=True
):
    """Returns NodeStats object describing memory usage in the cluster."""

    from ray.core.generated import node_manager_pb2
    from ray.core.generated import node_manager_pb2_grpc

    # We can ask any Raylet for the global memory info.
    assert node_manager_address is not None and node_manager_port is not None
    raylet_address = "{}:{}".format(node_manager_address, node_manager_port)
    channel = utils.init_grpc_channel(
        raylet_address,
        options=[
            ("grpc.max_send_message_length", MAX_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_MESSAGE_LENGTH),
        ],
    )

    stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
    node_stats = stub.GetNodeStats(
        node_manager_pb2.GetNodeStatsRequest(include_memory_info=include_memory_info),
        timeout=30.0,
    )
    return node_stats


def store_stats_summary(reply):
    """Returns formatted string describing object store stats in all nodes."""
    store_summary = "--- Aggregate object store stats across all nodes ---\n"
    # TODO(ekl) it would be nice if we could provide a full memory usage
    # breakdown by type (e.g., pinned by worker, primary, etc.)
    store_summary += (
        "Plasma memory usage {} MiB, {} objects, {}% full, {}% "
        "needed\n".format(
            int(reply.store_stats.object_store_bytes_used / (1024 * 1024)),
            reply.store_stats.num_local_objects,
            round(
                100
                * reply.store_stats.object_store_bytes_used
                / reply.store_stats.object_store_bytes_avail,
                2,
            ),
            round(
                100
                * reply.store_stats.object_store_bytes_primary_copy
                / reply.store_stats.object_store_bytes_avail,
                2,
            ),
        )
    )
    if reply.store_stats.object_store_bytes_fallback > 0:
        store_summary += "Plasma filesystem mmap usage: {} MiB\n".format(
            int(reply.store_stats.object_store_bytes_fallback / (1024 * 1024))
        )
    if reply.store_stats.spill_time_total_s > 0:
        store_summary += (
            "Spilled {} MiB, {} objects, avg write throughput {} MiB/s\n".format(
                int(reply.store_stats.spilled_bytes_total / (1024 * 1024)),
                reply.store_stats.spilled_objects_total,
                int(
                    reply.store_stats.spilled_bytes_total
                    / (1024 * 1024)
                    / reply.store_stats.spill_time_total_s
                ),
            )
        )
    if reply.store_stats.restore_time_total_s > 0:
        store_summary += (
            "Restored {} MiB, {} objects, avg read throughput {} MiB/s\n".format(
                int(reply.store_stats.restored_bytes_total / (1024 * 1024)),
                reply.store_stats.restored_objects_total,
                int(
                    reply.store_stats.restored_bytes_total
                    / (1024 * 1024)
                    / reply.store_stats.restore_time_total_s
                ),
            )
        )
    if reply.store_stats.consumed_bytes > 0:
        store_summary += "Objects consumed by Ray tasks: {} MiB.\n".format(
            int(reply.store_stats.consumed_bytes / (1024 * 1024))
        )
    if reply.store_stats.object_pulls_queued:
        store_summary += "Object fetches queued, waiting for available memory."

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
        raise TypeError(
            "free() expects a list of ObjectRef, got {}".format(type(object_refs))
        )

    # Make sure that the values are object refs.
    for object_ref in object_refs:
        if not isinstance(object_ref, ray.ObjectRef):
            raise TypeError(
                "Attempting to call `free` on the value {}, "
                "which is not an ray.ObjectRef.".format(object_ref)
            )

    worker.check_connected()
    with profiling.profile("ray.free"):
        if len(object_refs) == 0:
            return

        worker.core_worker.free_objects(object_refs, local_only)


def _get_dashboard_url():
    gcs_addr = services.get_ray_address_from_environment()
    dashboard_addr = services.get_dashboard_url(gcs_addr)
    if not dashboard_addr:
        return f"Cannot find the dashboard of the cluster of address {gcs_addr}."

    def format_web_url(url):
        """Format web url."""
        url = url.replace("localhost", "http://127.0.0.1")
        if not url.startswith("http://"):
            return "http://" + url
        return url

    url = format_web_url(dashboard_addr)
    return url


def ray_nodes(node_id: str, node_ip: str, debug=False):
    import requests

    if node_id and node_ip:
        raise ValueError(
            f"Node id {node_id} and node ip {node_ip} are given at the same time. Please provide only one of them."
        )
    gcs_addr = services.get_ray_address_from_environment()
    url = _get_dashboard_url()
    nodes = []
    if node_id:
        nodes.append(
            json.loads(requests.get(f"{url}/nodes/{node_id}").text)["data"]["detail"]
        )
    else:
        nodes = json.loads(requests.get(f"{url}/nodes?view=details").text)["data"][
            "clients"
        ]
    parsed_node_info = []
    state = GlobalState()
    if use_gcs_for_bootstrap():
        options = GcsClientOptions.from_gcs_address(gcs_addr)
    else:
        options = GcsClientOptions.from_redis_address(
            gcs_addr, ray_constants.REDIS_DEFAULT_PASSWORD
        )
    state._initialize_global_state(options)
    resources = state._available_resources_per_node()
    for node in nodes:
        if node["raylet"] != {}:
            info = {
                "id": node["raylet"]["nodeId"],
                "ip": node["ip"],
                "state": node["raylet"]["state"],
                "available_resources": resources[node["raylet"]["nodeId"]],
                "top_10_process_mem_usage": node["top10ProcMemUsage"],
            }
            if debug:
                info["logUrl"] = node["logUrl"]
            parsed_node_info.append(info)
    results = []
    # Filtering.
    if node_ip:
        for node in parsed_node_info:
            if node["ip"] == node_ip:
                results.append(node)
    else:
        results = parsed_node_info
    return results


def ray_actors(actor_id: str):
    import requests

    url = _get_dashboard_url()
    result = json.loads(requests.get(f"{url}/logical/actors").text)["data"]["actors"]
    if actor_id:
        result = {actor_id: result[actor_id]}
    # Parsing.
    output = {}
    for actor_id, data in result.items():
        func_desc = data["taskSpec"]["functionDescriptor"]
        actor_info = {}
        # TODO: this breaks for non-python?
        actor_info["class_name"] = func_desc[next(iter(func_desc))]["className"]
        actor_info["actor_id"] = data["actorId"]
        actor_info["node_id"] = data["address"]["rayletId"]
        actor_info["job_id"] = data["jobId"]
        actor_info["name"] = data["name"]
        # actor_info["namespace"] = data["rayNamespace"]
        actor_info["pid"] = data["pid"]
        actor_info["ip"] = data["address"]["ipAddress"]
        # actor_info["resource_req"] = data["resourceMapping"]
        actor_info["state"] = data["state"]
        output[actor_id] = actor_info
    return output


def ray_log(
    ip_address: str,
    node_id: str,
    filters: List[str],
    limit: int = 100,
):
    """Return the `limit` number of lines of logs."""
    import requests

    dashboard_url = _get_dashboard_url()
    nodes = ray_nodes(node_id, ip_address, debug=True)
    if len(nodes) > 0:
        log_url = nodes[0]["logUrl"]
    else:
        node_id_str = f"Input Node ID: {node_id}. " if node_id else ""
        addr_str = f"Input IP Address: {ip_address}. " if ip_address else ""
        raise Exception(f"Could not find node. {node_id_str}{addr_str}")
    log_html = requests.get(f"{dashboard_url}/log_proxy?url={log_url}").text

    def get_link(s):
        s = s[len('<li><a href="/logs/') :]
        path = s[: s.find('"')]
        s = f"{dashboard_url}/log_proxy?url={log_url}/{path}"
        return s

    logs = {}
    filtered = list(
        filter(
            lambda s: '<li><a href="/logs/' in s
            and (len(filters) == 0 or any([f in s for f in filters])),
            log_html.splitlines(),
        )
    )
    links = list(map(get_link, filtered))
    logs["worker_errors"] = list(
        filter(lambda s: "worker" in s and s.endswith(".err"), links)
    )
    logs["worker_outs"] = list(
        filter(lambda s: "worker" in s and s.endswith(".out"), links)
    )
    for lang in ray_constants.LANGUAGE_WORKER_TYPES:
        logs[f"{lang}_core_worker_logs"] = list(
            filter(lambda s: f"{lang}-core-worker" in s and s.endswith(".log"), links)
        )
        logs[f"{lang}_driver_logs"] = list(
            filter(lambda s: f"{lang}-core-driver" in s and s.endswith(".log"), links)
        )
    logs["raylet_logs"] = list(filter(lambda s: "raylet" in s, links))
    logs["gcs_logs"] = list(filter(lambda s: "gcs" in s, links))
    logs["misc"] = list(filter(lambda s: all([s not in logs[k] for k in logs]), links))
    return logs


def ray_actor_log(actor_id):
    actor_info = ray_actors(actor_id)[actor_id]
    node_id = actor_info["node_id"]
    pid = actor_info["pid"]
    return ray_log(None, node_id, [str(pid)])
