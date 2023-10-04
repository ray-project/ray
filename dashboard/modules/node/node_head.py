import asyncio
import json
import logging
import time
import grpc

import aiohttp.web

import ray._private.utils
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS

from ray.autoscaler._private.util import (
    LoadMetricsSummary,
    get_per_node_breakdown_as_dict,
    parse_usage,
)
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray.core.generated import (
    gcs_service_pb2,
    gcs_service_pb2_grpc,
    node_manager_pb2,
    node_manager_pb2_grpc,
)
from ray.dashboard.datacenter import DataOrganizer, DataSource
from ray.dashboard.modules.node import node_consts
from ray.dashboard.modules.node.node_consts import (
    FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS,
    FREQUENT_UPDATE_TIMEOUT_SECONDS,
)
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
)
from ray.dashboard.utils import async_loop_forever

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


def gcs_node_info_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {"nodeId"}, including_default_value_fields=True
    )


def gcs_stats_to_dict(message):
    decode_keys = {
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
    return dashboard_utils.message_to_dict(message, decode_keys)


def node_stats_to_dict(message):
    decode_keys = {
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
    core_workers_stats = message.core_workers_stats
    message.ClearField("core_workers_stats")
    try:
        result = dashboard_utils.message_to_dict(message, decode_keys)
        result["coreWorkersStats"] = [
            dashboard_utils.message_to_dict(
                m, decode_keys, including_default_value_fields=True
            )
            for m in core_workers_stats
        ]
        return result
    finally:
        message.core_workers_stats.extend(core_workers_stats)


class NodeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # NodeInfoGcsService
        self._gcs_node_info_stub = None
        # NodeResourceInfoGcsService
        self._gcs_node_resource_info_sub = None
        self._collect_memory_info = False
        DataSource.nodes.signal.append(self._update_stubs)
        # Total number of node updates happened.
        self._node_update_cnt = 0
        # The time where the module is started.
        self._module_start_time = time.time()
        # The time it takes until the head node is registered. None means
        # head node hasn't been registered.
        self._head_node_registration_time_s = None
        self._gcs_aio_client = dashboard_head.gcs_aio_client
        self._gcs_address = dashboard_head.gcs_address

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._stubs.pop(node_id)
        if change.new:
            # TODO(fyrestone): Handle exceptions.
            node_id, node_info = change.new
            address = "{}:{}".format(
                node_info["nodeManagerAddress"], int(node_info["nodeManagerPort"])
            )
            options = ray_constants.GLOBAL_GRPC_OPTIONS
            channel = ray._private.utils.init_grpc_channel(
                address, options, asynchronous=True
            )
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[node_id] = stub

    def get_internal_states(self):
        return {
            "head_node_registration_time_s": self._head_node_registration_time_s,
            "registered_nodes": len(DataSource.nodes),
            "registered_agents": len(DataSource.agents),
            "node_update_count": self._node_update_cnt,
            "module_lifetime_s": time.time() - self._module_start_time,
        }

    async def _get_nodes(self):
        """Read the client table.

        Returns:
            A dict of information about the nodes in the cluster.
        """
        request = gcs_service_pb2.GetAllNodeInfoRequest()
        reply = await self._gcs_node_info_stub.GetAllNodeInfo(request, timeout=2)
        if reply.status.code == 0:
            result = {}
            for node_info in reply.node_info_list:
                node_info_dict = gcs_node_info_to_dict(node_info)
                result[node_info_dict["nodeId"]] = node_info_dict
            return result
        else:
            logger.error("Failed to GetAllNodeInfo: %s", reply.status.message)

    async def _update_nodes(self):
        # TODO(fyrestone): Refactor code for updating actor / node / job.
        # Subscribe actor channel.
        while True:
            try:
                nodes = await self._get_nodes()

                alive_node_ids = []
                alive_node_infos = []
                node_id_to_ip = {}
                node_id_to_hostname = {}
                for node in nodes.values():
                    node_id = node["nodeId"]
                    ip = node["nodeManagerAddress"]
                    hostname = node["nodeManagerHostname"]
                    if (
                        ip == self._dashboard_head.ip
                        and not self._head_node_registration_time_s
                    ):
                        self._head_node_registration_time_s = (
                            time.time() - self._module_start_time
                        )
                        # Put head node ID in the internal KV to be read by JobAgent.
                        # TODO(architkulkarni): Remove once State API exposes which
                        # node is the head node.
                        await self._gcs_aio_client.internal_kv_put(
                            "head_node_id".encode(),
                            node_id.encode(),
                            overwrite=True,
                            namespace=ray_constants.KV_NAMESPACE_JOB,
                            timeout=2,
                        )
                    node_id_to_ip[node_id] = ip
                    node_id_to_hostname[node_id] = hostname
                    assert node["state"] in ["ALIVE", "DEAD"]
                    if node["state"] == "ALIVE":
                        alive_node_ids.append(node_id)
                        alive_node_infos.append(node)

                agents = dict(DataSource.agents)
                for node_id in alive_node_ids:
                    # Since the agent fate shares with a raylet,
                    # the agent port will never change once it is discovered.
                    if node_id not in agents:
                        key = (
                            f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}"
                            f"{node_id}"
                        )
                        agent_port = await self._gcs_aio_client.internal_kv_get(
                            key.encode(), namespace=ray_constants.KV_NAMESPACE_DASHBOARD
                        )
                        if agent_port:
                            agents[node_id] = json.loads(agent_port)
                for node_id in agents.keys() - set(alive_node_ids):
                    agents.pop(node_id, None)

                DataSource.node_id_to_ip.reset(node_id_to_ip)
                DataSource.node_id_to_hostname.reset(node_id_to_hostname)
                DataSource.agents.reset(agents)
                DataSource.nodes.reset(nodes)
            except Exception:
                logger.exception("Error updating nodes.")
            finally:
                self._node_update_cnt += 1
                # _head_node_registration_time_s == None if head node is not
                # registered.
                head_node_not_registered = not self._head_node_registration_time_s
                # Until the head node is registered, we update the
                # node status more frequently.
                # If the head node is not updated after 10 seconds, it just stops
                # doing frequent update to avoid unexpected edge case.
                if (
                    head_node_not_registered
                    and self._node_update_cnt * FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS
                    < FREQUENT_UPDATE_TIMEOUT_SECONDS
                ):
                    await asyncio.sleep(FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS)
                else:
                    if head_node_not_registered:
                        logger.warning(
                            "Head node is not registered even after "
                            f"{FREQUENT_UPDATE_TIMEOUT_SECONDS} seconds. "
                            "The API server might not work correctly. Please "
                            "report a Github issue. Internal states :"
                            f"{self.get_internal_states()}"
                        )
                    await asyncio.sleep(node_consts.UPDATE_NODES_INTERVAL_SECONDS)

    @routes.get("/internal/node_module")
    async def get_node_module_internal_state(self, req) -> aiohttp.web.Response:
        return dashboard_optional_utils.rest_response(
            success=True,
            message="",
            **self.get_internal_states(),
        )

    async def get_nodes_logical_resources(self) -> dict:

        from ray.autoscaler.v2.utils import is_autoscaler_v2

        if is_autoscaler_v2():
            from ray.autoscaler.v2.sdk import get_cluster_status

            try:
                cluster_status = get_cluster_status(self._gcs_address)
            except Exception:
                logger.exception("Error getting cluster status")
                return {}

            per_node_resources = {}
            # TODO(rickyx): we should just return structure data rather than strings.
            for node in cluster_status.healthy_nodes:
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
                self._gcs_aio_client.internal_kv_get(
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
            alive_hostnames = set()
            for node in DataSource.nodes.values():
                if node["state"] == "ALIVE":
                    alive_hostnames.add(node["nodeManagerHostname"])
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Node hostname list fetched.",
                host_name_list=list(alive_hostnames),
            )
        else:
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Unknown view {view}"
            )

    @routes.get("/nodes/{node_id}")
    @dashboard_optional_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        node_id = req.match_info.get("node_id")
        node_info = await DataOrganizer.get_node_info(node_id)
        return dashboard_optional_utils.rest_response(
            success=True, message="Node details fetched.", detail=node_info
        )

    @async_loop_forever(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def _update_node_stats(self):
        # Copy self._stubs to avoid `dictionary changed size during iteration`.
        get_node_stats_tasks = []
        nodes = list(self._stubs.items())
        TIMEOUT = node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS - 1

        for node_id, stub in nodes:
            node_info = DataSource.nodes.get(node_id)
            if node_info["state"] != "ALIVE":
                continue
            get_node_stats_tasks.append(
                stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(
                        include_memory_info=self._collect_memory_info
                    ),
                    timeout=min(2, TIMEOUT),
                )
            )

        replies = await asyncio.gather(
            *get_node_stats_tasks,
            return_exceptions=True,
        )

        for node_info, reply in zip(nodes, replies):
            node_id, _ = node_info
            if isinstance(reply, asyncio.CancelledError):
                pass
            elif isinstance(reply, grpc.RpcError):
                if reply.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logger.exception(
                        f"Cannot reach the node, {node_id}, after timeout {TIMEOUT}. "
                        "This node may have been overloaded, terminated, or "
                        "the network is slow."
                    )
                elif reply.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.exception(
                        f"Cannot reach the node, {node_id}. "
                        "The node may have been terminated."
                    )
                else:
                    logger.exception(f"Error updating node stats of {node_id}.")
                    logger.exception(reply)
            elif isinstance(reply, Exception):
                logger.exception(f"Error updating node stats of {node_id}.")
                logger.exception(reply)
            else:
                reply_dict = node_stats_to_dict(reply)
                DataSource.node_stats[node_id] = reply_dict

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_node_info_stub = gcs_service_pb2_grpc.NodeInfoGcsServiceStub(
            gcs_channel
        )
        self._gcs_node_resource_info_stub = (
            gcs_service_pb2_grpc.NodeResourceInfoGcsServiceStub(gcs_channel)
        )

        await asyncio.gather(
            self._update_nodes(),
            self._update_node_stats(),
        )

    @staticmethod
    def is_minimal_module():
        return False
