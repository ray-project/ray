import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

import aiohttp.web

from ray.dashboard.utils import (
    compose_state_message,
)

from ray import WorkerID, NodeID
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    env_integer,
)
from ray.dashboard.modules.node import NodeDataIndex
from ray._private.utils import get_or_create_event_loop
from ray.autoscaler._private.util import (
    LoadMetricsSummary,
    get_per_node_breakdown_as_dict,
    parse_usage,
)
from ray.core.generated import gcs_pb2
from ray.dashboard.consts import (
    GCS_RPC_TIMEOUT_SECONDS,
    DEFAULT_LANGUAGE,
    DEFAULT_JOB_ID,
)
from ray.dashboard.datacenter import DataOrganizer

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


# NOTE: Executor in this head is intentionally constrained to just 1 thread by
#       default to limit its concurrency, therefore reducing potential for
#       GIL contention
RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS = env_integer(
    "RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS", 1
)


def value_or_default(value, default):
    # `default`, if value is False-y.
    return default if not value else value


def _gcs_node_info_to_dict(message: gcs_pb2.GcsNodeInfo) -> dict:
    return dashboard_utils.message_to_dict(
        message, {"nodeId"}, always_print_fields_with_no_presence=True
    )


NODE_STATS_DECODE_KEYS = {
    "actorId",
    "jobId",
    "taskId",
    "parentTaskId",
    "sourceActorId",
    "callerId",
    "rayletId",
    "workerId",
    "placementGroupId",
}


def core_worker_stats_to_dict(message):

    return dashboard_utils.message_to_dict(
        message, NODE_STATS_DECODE_KEYS, always_print_fields_with_no_presence=True
    )


def node_stats_to_dict(message):
    core_workers_stats = message.core_workers_stats
    message.ClearField("core_workers_stats")
    try:
        result = dashboard_utils.message_to_dict(message, NODE_STATS_DECODE_KEYS)
        result["coreWorkersStats"] = [
            core_worker_stats_to_dict(m) for m in core_workers_stats
        ]
        return result
    finally:
        message.core_workers_stats.extend(core_workers_stats)


class NodeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)

        self._executor = ThreadPoolExecutor(
            max_workers=RAY_DASHBOARD_NODE_HEAD_TPE_MAX_WORKERS,
            thread_name_prefix="node_head_executor",
        )
        self._data_index = NodeDataIndex(
            loop=get_or_create_event_loop(),
            executor=self._executor,
            gcs_address=self.gcs_address,
            module_start_time=time.time(),
        )

    def get_internal_states(self):
        return {
            "head_node_registration_time_s": self._data_index.head_node_registration_time_s,
            # Contains DEAD nodes! For ALIVE nodes only, use `self._data_index.nodes`.
            "registered_nodes": len(self._data_index.nodes),
            "module_lifetime_s": self._data_index.module_lifetime_s,
        }

    async def get_nodes_logical_resources(self) -> dict:

        from ray.autoscaler.v2.utils import is_autoscaler_v2

        if is_autoscaler_v2():
            from ray.autoscaler.v2.sdk import ClusterStatusParser
            from ray.autoscaler.v2.schema import Stats

            try:
                # here we have a sync request
                req_time = time.time()
                cluster_status = await self.gcs_aio_client.get_cluster_status()
                reply_time = time.time()
                cluster_status = ClusterStatusParser.from_get_cluster_status_reply(
                    cluster_status,
                    stats=Stats(
                        gcs_request_time_s=reply_time - req_time, request_ts_s=req_time
                    ),
                )
            except Exception:
                logger.exception("Error getting cluster status")
                return {}

            per_node_resources = {}
            # TODO(rickyx): we should just return structure data rather than strings.
            for node in chain(cluster_status.active_nodes, cluster_status.idle_nodes):
                if not node.resource_usage:
                    continue

                usage_dict = {
                    r.resource_name: (r.used, r.total)
                    for r in node.resource_usage.usage
                }
                per_node_resources[node.node_id] = "\n".join(
                    parse_usage(usage_dict, verbose=True)
                )

            return per_node_resources

        # Legacy autoscaler status code.
        (status_string, error) = await asyncio.gather(
            *[
                self.gcs_aio_client.internal_kv_get(
                    key.encode(), namespace=None, timeout=GCS_RPC_TIMEOUT_SECONDS
                )
                for key in [
                    DEBUG_AUTOSCALING_STATUS,
                    DEBUG_AUTOSCALING_ERROR,
                ]
            ]
        )
        if not status_string:
            return {}
        status_dict = json.loads(status_string)

        lm_summary_dict = status_dict.get("load_metrics_report")
        if lm_summary_dict:
            lm_summary = LoadMetricsSummary(**lm_summary_dict)

        node_logical_resources = get_per_node_breakdown_as_dict(lm_summary)
        return node_logical_resources if error is None else {}

    @routes.get("/nodes")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_nodes(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            all_node_summary_task = DataOrganizer.get_all_node_summary()
            nodes_logical_resource_task = self.get_nodes_logical_resources()

            all_node_summary, nodes_logical_resources = await asyncio.gather(
                all_node_summary_task, nodes_logical_resource_task
            )

            return dashboard_optional_utils.rest_response(
                success=True,
                message="Node summary fetched.",
                summary=all_node_summary,
                node_logical_resources=nodes_logical_resources,
            )
        elif view is not None and view.lower() == "hostNameList".lower():
            alive_hostnames = {
                node_info.node_manager_hostname
                for node_info in self._data_index.nodes.values()
                if node_info.state == gcs_pb2.GcsNodeInfo.ALIVE
            }
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Node hostname list fetched.",
                host_name_list=list(alive_hostnames),
            )
        else:
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Unknown view {view}"
            )

    def get_worker_by_id(self, node_id: NodeID, worker_id: WorkerID) -> dict:
        worker_info = self._data_index.workers.get(worker_id, {})
        if not worker_info:
            return {}
        node_physical_stats = self._data_index.node_physical_stats.get(node_id, {})

        result = {}
        core_worker_stats = worker_info.core_worker_stats
        actor_constructor = value_or_default(
            core_worker_stats.actor_title, "Unknown actor constructor"
        )
        result["actorConstructor"] = actor_constructor
        result.update(core_worker_stats_to_dict(core_worker_stats))

        pid = core_worker_stats.pid

        actor_process_stats = worker_info.node_physical_stats_for_worker
        actor_process_gpu_stats = []
        if pid:
            for gpu_stats in node_physical_stats.get("gpus", []):
                # gpu_stats.get("processes") can be None, an empty list or a
                # list of dictionaries.
                for process in gpu_stats.get("processesPids", []):
                    if process["pid"] == pid:
                        actor_process_gpu_stats.append(gpu_stats)
                        break

        result["gpus"] = actor_process_gpu_stats
        result["processStats"] = actor_process_stats
        result["mem"] = node_physical_stats.get("mem", [])
        return result

    @routes.get("/nodes/{node_id}/workers/{worker_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_worker(self, req) -> aiohttp.web.Response:
        node_id_hex = req.match_info.get("node_id")
        worker_id_hex = req.match_info.get("worker_id")
        node_id = NodeID(node_id_hex)
        worker_id = WorkerID(worker_id_hex)

        worker = await self.loop.run_in_executor(
            self.executor, self.get_worker_by_id, node_id, worker_id
        )

        return dashboard_optional_utils.rest_response(
            success=True, message="Worker fetched.", worker=worker
        )

    @routes.get("/nodes/{node_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        """
        Get node details. 3 steps:
        1. In async, convert node proto to dict.
        2. Make a HTTP request to ActorHead to get actors on this node.
        3. In async, add worker info to actors dict.
        """
        node_id_hex = req.match_info.get("node_id")
        node_id = NodeID(node_id_hex)
        node = await self.loop.run_in_executor(
            self.executor, self.get_node_by_id, node_id
        )
        # Add actors by making a request to ActorHead.
        async with self.http_session.get(
            f"{self.dashboard_url}/nodes/{node_id_hex}/actors_without_worker_info"
        ) as resp:
            if resp.status != 200:
                # Error fetching actors. Don't add them to the result.
                logger.error(
                    f"Error fetching actors. The result will not contain actors for node {node_id}: {resp.text()}"
                )
            else:
                actors = await resp.json()
                # Add worker info to actors.
                def add_worker_infos(actors):
                    for actor in actors:
                        worker_id_hex = actor["address"]["workerId"]
                        worker_id = WorkerID(worker_id_hex)
                        worker = self.get_worker_by_id(node_id, worker_id)
                        actor["worker"] = worker
                    return actors

                actors_with_worker_infos = await self.loop.run_in_executor(
                    self.executor, add_worker_infos, actors
                )
                node["actors"] = actors_with_worker_infos

        return dashboard_optional_utils.rest_response(
            success=True, message="Node details fetched.", detail=node
        )

    def get_node_by_id(self, node_id: NodeID) -> dict:
        """
        Merges all data about a node_id into a single dictionary.

        Note: we don't have "actors" in the result.
        """
        node_info = self._data_index.nodes.get(node_id, {})
        node_physical_stats = self._data_index.node_physical_stats.get(node_id, {})
        node_stats_without_core_worker_stats = (
            self._data_index.node_stats_without_core_worker_stats.get(node_id, {})
        )

        used = node_stats_without_core_worker_stats.store_stats.object_store_bytes_used
        total = (
            node_stats_without_core_worker_stats.store_stats.object_store_bytes_avail
        )
        ray_stats = {
            "object_store_used_memory": used,
            "object_store_available_memory": total - used,
        }

        # Copy node physical stats.
        result = dict(node_physical_stats)

        # Merge data from node_stats.
        result["raylet"] = node_stats_to_dict(node_stats_without_core_worker_stats)
        result["raylet"].update(ray_stats)

        # Merge data from node_info.
        result["raylet"].update(_gcs_node_info_to_dict(node_info))
        death_info = node_info.death_info
        result["raylet"]["stateMessage"] = compose_state_message(
            death_info.reason, death_info.reason_message
        )

        # Merge actors.
        node_info = await DataOrganizer.get_node_info(node_id)

        # Merge workers.
        workers = []
        for worker_id in self._data_index.node_id_pid_worker_id.get(
            node_id, []
        ).values():
            worker_info = self._data_index.workers.get(worker_id, None)
            if worker_info is None:
                continue
            node_physical_stats = worker_info.node_physical_stats_for_worker
            if node_physical_stats is None:
                continue
            # Copies node physical stats for worker.
            worker_result = dict(node_physical_stats)
            # Merges core worker stats for worker.
            worker_result["coreWorkerStats"] = []
            if worker_info.core_worker_stats is not None:
                worker_result["coreWorkerStats"].append(
                    core_worker_stats_to_dict(worker_info.core_worker_stats)
                )
            worker_result["language"] = value_or_default(
                worker_info.core_worker_stats.language,
                DEFAULT_LANGUAGE,
            )
            worker_result["jobId"] = value_or_default(
                worker_info.core_worker_stats.job_id, DEFAULT_JOB_ID
            )
            workers.append(worker_result)

        result["workers"] = workers
        return result

    async def run(self, server):
        await asyncio.gather(
            self._update_nodes(),
            self._update_node_stats(),
        )

    @staticmethod
    def is_minimal_module():
        return False
