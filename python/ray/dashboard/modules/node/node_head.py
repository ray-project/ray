import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from typing import List
from ray._private.utils import parse_pg_formatted_resources_to_original
import aiohttp.web
from ray.core.generated import node_manager_pb2
from ray.dashboard.utils import (
    compose_state_message,
)

from ray import WorkerID, NodeID, ActorID
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    env_integer,
)
from ray.dashboard.modules.node.node_data_index import NodeDataIndex
from ray.dashboard.modules.node.actor_data_index import (
    ActorDataIndex,
    actor_table_data_to_dict,
)
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
        self._node_data_index = NodeDataIndex(
            loop=get_or_create_event_loop(),
            executor=self._executor,
            gcs_aio_client=self.gcs_aio_client,
            module_start_time=time.time(),
        )
        self._actor_data_index = ActorDataIndex(
            loop=get_or_create_event_loop(),
            executor=self._executor,
            gcs_aio_client=self.gcs_aio_client,
        )

    def get_internal_states(self):
        return {
            "head_node_registration_time_s": self._node_data_index.head_node_registration_time_s,
            # Contains DEAD nodes! For ALIVE nodes only, use `self._node_data_index.nodes`.
            "registered_nodes": len(self._node_data_index.nodes),
            "module_lifetime_s": self._node_data_index.module_lifetime_s,
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

            all_node_summary_task = get_or_create_event_loop().run_in_executor(
                self._executor, self.get_all_node_summary
            )
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
                for node_info in self._node_data_index.nodes.values()
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
        # Worker info as used for actor info.
        worker_info = self._node_data_index.workers.get(worker_id, {})
        if not worker_info:
            return {}
        node_physical_stats = self._node_data_index.node_physical_stats.get(node_id, {})

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

    @routes.get("/nodes/{node_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        """
        Get node details. It's pretty compute heavy so we do it in executor.
        """
        node_id_hex = req.match_info.get("node_id")
        node_id = NodeID.from_hex(node_id_hex)
        node = await get_or_create_event_loop().run_in_executor(
            self._executor, self.get_node_by_id, node_id
        )
        return dashboard_optional_utils.rest_response(
            success=True, message="Node details fetched.", detail=node
        )

    @routes.get("/logical/actors")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_actors(self, req) -> aiohttp.web.Response:
        actors = await get_or_create_event_loop().run_in_executor(
            self._executor, self._get_all_actors
        )
        return dashboard_optional_utils.rest_response(
            success=True,
            message="All actors fetched.",
            actors=actors,
            # False to avoid converting Ray resource name to google style.
            # It's not necessary here because the fields are already
            # google formatted when protobuf was converted into dict.
            convert_google_style=False,
        )

    @routes.get("/logical/actors/{actor_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_actor(self, req) -> aiohttp.web.Response:
        actor_id_hex = req.match_info.get("actor_id")
        actor_id = ActorID(actor_id_hex)

        actor = await get_or_create_event_loop().run_in_executor(
            self._executor, self.get_actor_by_id, actor_id
        )
        return dashboard_optional_utils.rest_response(
            success=True, message="Actor details fetched.", detail=actor
        )

    def get_all_node_summary(self) -> List[dict]:
        result = [
            self.get_node_summary_by_id(node_id)
            for node_id in self._node_data_index.nodes.keys()
        ]
        return result

    def get_node_summary_by_id(self, node_id: NodeID) -> dict:
        """
        Get node summary by node_id.

        - node_info
        - node_physical_stats: all fields except "workers"
        - node_stats_without_core_worker_stats
        """
        node_info = self._node_data_index.nodes.get(node_id, {})
        node_physical_stats = {
            k: v
            for k, v in self._node_data_index.node_physical_stats.get(
                node_id, {}
            ).items()
            if k != "workers"
        }
        node_stats_without_core_worker_stats = (
            self._node_data_index.node_stats_without_core_worker_stats.get(
                node_id, node_manager_pb2.GetNodeStatsReply()
            )
        )

        total = (
            node_stats_without_core_worker_stats.store_stats.object_store_bytes_avail
        )
        used = node_stats_without_core_worker_stats.store_stats.object_store_bytes_used
        ray_stats = {
            "object_store_used_memory": used,
            "object_store_available_memory": total - used,
        }

        result = node_physical_stats
        result["raylet"] = node_stats_to_dict(node_stats_without_core_worker_stats)
        result["raylet"].update(ray_stats)
        result["raylet"].update(_gcs_node_info_to_dict(node_info))
        death_info = node_info.death_info
        result["raylet"]["stateMessage"] = compose_state_message(
            death_info.reason, death_info.reason_message
        )
        return result

    def _get_all_actors(self) -> List[dict]:
        return [
            self.get_actor_by_id(actor_id)
            for actor_id in self._actor_data_index.actors.keys()
        ]

    def get_actor_by_id(self, actor_id: ActorID) -> dict:
        """
        Gets a dict about an actor and its worker.
        Data from:
        - ActorDataIndex.actors for the actor_id.
        - NodeDataIndex.workers for the worker_id.
        - NodeDataIndex.node_physical_stats for gpus on that node.
        - field "requiredResources" updated.
        """
        actor_table_data = self._actor_data_index.actors.get(actor_id, {})
        actor_dict = actor_table_data_to_dict(actor_table_data)
        node_id = NodeID.from_hex(actor_dict["address"]["rayletId"])
        worker_id = WorkerID.from_hex(actor_dict["address"]["workerId"])
        worker = self.get_worker_by_id(node_id, worker_id)
        actor_dict.update(worker)
        required_resources = parse_pg_formatted_resources_to_original(
            actor_dict["requiredResources"]
        )
        actor_dict["requiredResources"] = required_resources
        return actor_dict

    def get_node_by_id(self, node_id: NodeID) -> dict:
        """
        Merges all data about a node_id into a single dictionary.

        - get_node_summary_by_id
        - workers on that node
        - actors on that node
        """
        result = self.get_node_summary_by_id(node_id)

        # Merge actors. Each actor dict comes from:
        # 1. ActorDataIndex.actors for the actor_id.
        # 2. NodeDataIndex.workers for the worker_id.
        # 3. NodeDataIndex.node_physical_stats for gpus on that node.
        # 4. field "requiredResources" updated.
        actor_ids = self._actor_data_index.node_actors.get(node_id, [])
        # add worker info to each actor dict.
        result["actors"] = [self.get_actor_by_id(actor_id) for actor_id in actor_ids]

        # Merge workers.
        workers = []
        for worker_id in self._node_data_index.node_pid_worker_id.get(
            node_id, {}
        ).values():
            worker_info = self._node_data_index.workers.get(worker_id, None)
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
            self._actor_data_index.run(),
            self._node_data_index.run(),
        )

    @staticmethod
    def is_minimal_module():
        return False
