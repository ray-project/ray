import asyncio
import json
import logging
import re
import time

import aiohttp.web

import ray._private.utils
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
from ray.dashboard.memory_utils import GroupByType, SortingType
from ray.dashboard.modules.node import node_consts
from ray.dashboard.modules.node.node_consts import (
    LOG_PRUNE_THREASHOLD,
    MAX_LOGS_TO_CACHE,
    FREQUENTY_UPDATE_NODES_INTERVAL_SECONDS,
    FREQUENT_UPDATE_TIMEOUT_SECONDS,
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
                    node_id_to_ip[node_id] = ip
                    node_id_to_hostname[node_id] = hostname
                    assert node["state"] in ["ALIVE", "DEAD"]
                    if node["state"] == "ALIVE":
                        alive_node_ids.append(node_id)
                        alive_node_infos.append(node)

                agents = dict(DataSource.agents)
                for node_id in alive_node_ids:
                    key = f"{dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX}" f"{node_id}"
                    # TODO: Use async version if performance is an issue
                    agent_port = ray.experimental.internal_kv._internal_kv_get(
                        key, namespace=ray_constants.KV_NAMESPACE_DASHBOARD
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

    @routes.get("/nodes")
    @dashboard_optional_utils.aiohttp_cache
    async def get_all_nodes(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            all_node_summary = await DataOrganizer.get_all_node_summary()
            return dashboard_optional_utils.rest_response(
                success=True, message="Node summary fetched.", summary=all_node_summary
            )
        elif view == "details":
            all_node_details = await DataOrganizer.get_all_node_details()
            return dashboard_optional_utils.rest_response(
                success=True,
                message="All node details fetched",
                clients=all_node_details,
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

    @routes.get("/memory/memory_table")
    async def get_memory_table(self, req) -> aiohttp.web.Response:
        group_by = req.query.get("group_by")
        sort_by = req.query.get("sort_by")
        kwargs = {}
        if group_by:
            kwargs["group_by"] = GroupByType(group_by)
        if sort_by:
            kwargs["sort_by"] = SortingType(sort_by)

        memory_table = await DataOrganizer.get_memory_table(**kwargs)
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Fetched memory table",
            memory_table=memory_table.as_dict(),
        )

    @routes.get("/memory/set_fetch")
    async def set_fetch_memory_info(self, req) -> aiohttp.web.Response:
        should_fetch = req.query["shouldFetch"]
        if should_fetch == "true":
            self._collect_memory_info = True
        elif should_fetch == "false":
            self._collect_memory_info = False
        else:
            return dashboard_optional_utils.rest_response(
                success=False, message=f"Unknown argument to set_fetch {should_fetch}"
            )
        return dashboard_optional_utils.rest_response(
            success=True, message=f"Successfully set fetching to {should_fetch}"
        )

    @routes.get("/node_errors")
    async def get_errors(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = str(req.query.get("pid", ""))
        node_errors = DataSource.ip_and_pid_to_errors.get(ip, {})
        if pid:
            node_errors = {str(pid): node_errors.get(pid, [])}
        return dashboard_optional_utils.rest_response(
            success=True, message="Fetched errors.", errors=node_errors
        )

    @async_loop_forever(node_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def _update_node_stats(self):
        # Copy self._stubs to avoid `dictionary changed size during iteration`.
        for node_id, stub in list(self._stubs.items()):
            node_info = DataSource.nodes.get(node_id)
            if node_info["state"] != "ALIVE":
                continue
            try:
                reply = await stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(
                        include_memory_info=self._collect_memory_info
                    ),
                    timeout=2,
                )
                reply_dict = node_stats_to_dict(reply)
                DataSource.node_stats[node_id] = reply_dict
            except Exception:
                logger.exception(f"Error updating node stats of {node_id}.")

        # Update scheduling stats (e.g., pending actor creation tasks) of gcs.
        try:
            reply = await self._gcs_node_resource_info_stub.GetGcsSchedulingStats(
                gcs_service_pb2.GetGcsSchedulingStatsRequest(),
                timeout=2,
            )
            if reply.status.code == 0:
                DataSource.gcs_scheduling_stats = gcs_stats_to_dict(reply)
        except Exception:
            logger.exception("Error updating gcs stats.")

    async def _update_log_info(self):
        if ray_constants.DISABLE_DASHBOARD_LOG_INFO:
            return

        def process_log_batch(log_batch):
            ip = log_batch["ip"]
            pid = str(log_batch["pid"])
            if pid != "autoscaler":
                log_counts_for_ip = dict(
                    DataSource.ip_and_pid_to_log_counts.get(ip, {})
                )
                log_counts_for_pid = log_counts_for_ip.get(pid, 0)
                log_counts_for_pid += len(log_batch["lines"])
                log_counts_for_ip[pid] = log_counts_for_pid
                DataSource.ip_and_pid_to_log_counts[ip] = log_counts_for_ip
            logger.debug(f"Received a log for {ip} and {pid}")

        while True:
            try:
                log_batch = await self._dashboard_head.gcs_log_subscriber.poll()
                if log_batch is None:
                    continue
                process_log_batch(log_batch)
            except Exception:
                logger.exception("Error receiving log from GCS.")

    async def _update_error_info(self):
        def process_error(error_data):
            message = error_data.error_message
            message = re.sub(r"\x1b\[\d+m", "", message)
            match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
            if match:
                pid = match.group(1)
                ip = match.group(2)
                errs_for_ip = dict(DataSource.ip_and_pid_to_errors.get(ip, {}))
                pid_errors = list(errs_for_ip.get(pid, []))
                pid_errors.append(
                    {
                        "message": message,
                        "timestamp": error_data.timestamp,
                        "type": error_data.type,
                    }
                )

                # Only cache up to MAX_LOGS_TO_CACHE
                pid_errors_length = len(pid_errors)
                if pid_errors_length > MAX_LOGS_TO_CACHE * LOG_PRUNE_THREASHOLD:
                    offset = pid_errors_length - MAX_LOGS_TO_CACHE
                    del pid_errors[:offset]

                errs_for_ip[pid] = pid_errors
                DataSource.ip_and_pid_to_errors[ip] = errs_for_ip
                logger.info(f"Received error entry for {ip} {pid}")

        while True:
            try:
                (
                    _,
                    error_data,
                ) = await self._dashboard_head.gcs_error_subscriber.poll()
                if error_data is None:
                    continue
                process_error(error_data)
            except Exception:
                logger.exception("Error receiving error info from GCS.")

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
            self._update_log_info(),
            self._update_error_info(),
        )

    @staticmethod
    def is_minimal_module():
        return False
