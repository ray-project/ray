import asyncio
import re
import logging
import json
import aiohttp.web
from aioredis.pubsub import Receiver
from grpc.experimental import aio as aiogrpc

import ray.gcs_utils
import ray.new_dashboard.modules.stats_collector.stats_collector_consts \
    as stats_collector_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.actor_utils import actor_classname_from_task_spec
from ray.new_dashboard.utils import async_loop_forever
from ray.new_dashboard.memory_utils import GroupByType, SortingType
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import DataSource, DataOrganizer

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def node_stats_to_dict(message):
    decode_keys = {
        "actorId", "jobId", "taskId", "parentTaskId", "sourceActorId",
        "callerId", "rayletId", "workerId", "placementGroupId"
    }
    core_workers_stats = message.core_workers_stats
    message.ClearField("core_workers_stats")
    try:
        result = dashboard_utils.message_to_dict(message, decode_keys)
        result["coreWorkersStats"] = [
            dashboard_utils.message_to_dict(
                m, decode_keys, including_default_value_fields=True)
            for m in core_workers_stats
        ]
        return result
    finally:
        message.core_workers_stats.extend(core_workers_stats)


def actor_table_data_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {
            "actorId", "parentId", "jobId", "workerId", "rayletId",
            "actorCreationDummyObjectId", "callerId", "taskId", "parentTaskId",
            "sourceActorId", "placementGroupId"
        },
        including_default_value_fields=True)


class StatsCollector(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        # JobInfoGcsServiceStub
        self._gcs_job_info_stub = None
        # ActorInfoGcsService
        self._gcs_actor_info_stub = None
        self._collect_memory_info = False
        DataSource.nodes.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, node_info = change.old
            self._stubs.pop(node_id)
        if change.new:
            node_id, node_info = change.new
            address = "{}:{}".format(node_info["nodeManagerAddress"],
                                     int(node_info["nodeManagerPort"]))
            options = (("grpc.enable_http_proxy", 0), )
            channel = aiogrpc.insecure_channel(address, options=options)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[node_id] = stub

    @routes.get("/nodes")
    @dashboard_utils.aiohttp_cache
    async def get_all_nodes(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            all_node_summary = await DataOrganizer.get_all_node_summary()
            return dashboard_utils.rest_response(
                success=True,
                message="Node summary fetched.",
                summary=all_node_summary)
        elif view == "details":
            all_node_details = await DataOrganizer.get_all_node_details()
            return dashboard_utils.rest_response(
                success=True,
                message="All node details fetched",
                clients=all_node_details,
            )
        elif view is not None and view.lower() == "hostNameList".lower():
            alive_hostnames = set()
            for node in DataSource.nodes.values():
                if node["state"] == "ALIVE":
                    alive_hostnames.add(node["nodeManagerHostname"])
            return dashboard_utils.rest_response(
                success=True,
                message="Node hostname list fetched.",
                host_name_list=list(alive_hostnames))
        else:
            return dashboard_utils.rest_response(
                success=False, message=f"Unknown view {view}")

    @routes.get("/nodes/{node_id}")
    @dashboard_utils.aiohttp_cache
    async def get_node(self, req) -> aiohttp.web.Response:
        node_id = req.match_info.get("node_id")
        node_info = await DataOrganizer.get_node_info(node_id)
        return dashboard_utils.rest_response(
            success=True, message="Node details fetched.", detail=node_info)

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
        return dashboard_utils.rest_response(
            success=True,
            message="Fetched memory table",
            memory_table=memory_table.as_dict())

    @routes.get("/memory/set_fetch")
    async def set_fetch_memory_info(self, req) -> aiohttp.web.Response:
        should_fetch = req.query["shouldFetch"]
        if should_fetch == "true":
            self._collect_memory_info = True
        elif should_fetch == "false":
            self._collect_memory_info = False
        else:
            return dashboard_utils.rest_response(
                success=False,
                message=f"Unknown argument to set_fetch {should_fetch}")
        return dashboard_utils.rest_response(
            success=True,
            message=f"Successfully set fetching to {should_fetch}")

    @routes.get("/node_logs")
    async def get_logs(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = str(req.query.get("pid", ""))
        node_logs = DataSource.ip_and_pid_to_logs.get(ip, {})
        if pid:
            node_logs = {str(pid): node_logs.get(pid, [])}
        return dashboard_utils.rest_response(
            success=True, message="Fetched logs.", logs=node_logs)

    @routes.get("/node_errors")
    async def get_errors(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = str(req.query.get("pid", ""))
        node_errors = DataSource.ip_and_pid_to_errors.get(ip, {})
        if pid:
            node_errors = {str(pid): node_errors.get(pid, [])}
        return dashboard_utils.rest_response(
            success=True, message="Fetched errors.", errors=node_errors)

    async def _update_actors(self):
        # Subscribe actor channel.
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        key = "{}:*".format(stats_collector_consts.ACTOR_CHANNEL)
        pattern = receiver.pattern(key)
        await aioredis_client.psubscribe(pattern)
        logger.info("Subscribed to %s", key)

        def _process_actor_table_data(data):
            actor_class = actor_classname_from_task_spec(
                data.get("taskSpec", {}))
            data["actorClass"] = actor_class

        # Get all actor info.
        while True:
            try:
                logger.info("Getting all actor info from GCS.")
                request = gcs_service_pb2.GetAllActorInfoRequest()
                reply = await self._gcs_actor_info_stub.GetAllActorInfo(
                    request, timeout=5)
                if reply.status.code == 0:
                    actors = {}
                    for message in reply.actor_table_data:
                        actor_table_data = actor_table_data_to_dict(message)
                        _process_actor_table_data(actor_table_data)
                        actors[actor_table_data["actorId"]] = actor_table_data
                    # Update actors.
                    DataSource.actors.reset(actors)
                    # Update node actors and job actors.
                    job_actors = {}
                    node_actors = {}
                    for actor_id, actor_table_data in actors.items():
                        job_id = actor_table_data["jobId"]
                        node_id = actor_table_data["address"]["rayletId"]
                        job_actors.setdefault(job_id,
                                              {})[actor_id] = actor_table_data
                        # Update only when node_id is not Nil.
                        if node_id != stats_collector_consts.NIL_NODE_ID:
                            node_actors.setdefault(
                                node_id, {})[actor_id] = actor_table_data
                    DataSource.job_actors.reset(job_actors)
                    DataSource.node_actors.reset(node_actors)
                    logger.info("Received %d actor info from GCS.",
                                len(actors))
                    break
                else:
                    raise Exception(
                        f"Failed to GetAllActorInfo: {reply.status.message}")
            except Exception:
                logger.exception("Error Getting all actor info from GCS.")
                await asyncio.sleep(stats_collector_consts.
                                    RETRY_GET_ALL_ACTOR_INFO_INTERVAL_SECONDS)

        # Receive actors from channel.
        async for sender, msg in receiver.iter():
            try:
                _, actor_table_data = msg
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(
                    actor_table_data)
                message = ray.gcs_utils.ActorTableData.FromString(
                    pubsub_message.data)
                actor_table_data = actor_table_data_to_dict(message)
                _process_actor_table_data(actor_table_data)
                actor_id = actor_table_data["actorId"]
                job_id = actor_table_data["jobId"]
                node_id = actor_table_data["address"]["rayletId"]
                # Update actors.
                DataSource.actors[actor_id] = actor_table_data
                # Update node actors (only when node_id is not Nil).
                if node_id != stats_collector_consts.NIL_NODE_ID:
                    node_actors = dict(DataSource.node_actors.get(node_id, {}))
                    node_actors[actor_id] = actor_table_data
                    DataSource.node_actors[node_id] = node_actors
                # Update job actors.
                job_actors = dict(DataSource.job_actors.get(job_id, {}))
                job_actors[actor_id] = actor_table_data
                DataSource.job_actors[job_id] = job_actors
            except Exception:
                logger.exception("Error receiving actor info.")

    @async_loop_forever(
        stats_collector_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def _update_node_stats(self):
        # Copy self._stubs to avoid `dictionary changed size during iteration`.
        for node_id, stub in list(self._stubs.items()):
            node_info = DataSource.nodes.get(node_id)
            if node_info["state"] != "ALIVE":
                continue
            try:
                reply = await stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(
                        include_memory_info=self._collect_memory_info),
                    timeout=2)
                reply_dict = node_stats_to_dict(reply)
                DataSource.node_stats[node_id] = reply_dict
            except Exception:
                logger.exception(f"Error updating node stats of {node_id}.")

    async def _update_log_info(self):
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        channel = receiver.channel(ray.gcs_utils.LOG_FILE_CHANNEL)
        await aioredis_client.subscribe(channel)
        logger.info("Subscribed to %s", channel)

        async for sender, msg in receiver.iter():
            try:
                data = json.loads(ray.utils.decode(msg))
                ip = data["ip"]
                pid = str(data["pid"])
                logs_for_ip = dict(DataSource.ip_and_pid_to_logs.get(ip, {}))
                logs_for_pid = list(logs_for_ip.get(pid, []))
                logs_for_pid.extend(data["lines"])
                logs_for_ip[pid] = logs_for_pid
                DataSource.ip_and_pid_to_logs[ip] = logs_for_ip
                logger.info(f"Received a log for {ip} and {pid}")
            except Exception:
                logger.exception("Error receiving log info.")

    async def _update_error_info(self):
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        key = ray.gcs_utils.RAY_ERROR_PUBSUB_PATTERN
        pattern = receiver.pattern(key)
        await aioredis_client.psubscribe(pattern)
        logger.info("Subscribed to %s", key)

        async for sender, msg in receiver.iter():
            try:
                _, data = msg
                pubsub_msg = ray.gcs_utils.PubSubMessage.FromString(data)
                error_data = ray.gcs_utils.ErrorTableData.FromString(
                    pubsub_msg.data)
                message = error_data.error_message
                message = re.sub(r"\x1b\[\d+m", "", message)
                match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
                if match:
                    pid = match.group(1)
                    ip = match.group(2)
                    errs_for_ip = DataSource.ip_and_pid_to_errors.get(ip, {})
                    pid_errors = errs_for_ip.get(pid, [])
                    pid_errors.append({
                        "message": message,
                        "timestamp": error_data.timestamp,
                        "type": error_data.type
                    })
                    errs_for_ip[pid] = pid_errors
                    DataSource.ip_and_pid_to_errors[ip] = errs_for_ip
                    logger.info(f"Received error entry for {ip} {pid}")
            except Exception:
                logger.exception("Error receiving error info.")

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_job_info_stub = \
            gcs_service_pb2_grpc.JobInfoGcsServiceStub(gcs_channel)
        self._gcs_actor_info_stub = \
            gcs_service_pb2_grpc.ActorInfoGcsServiceStub(gcs_channel)

        await asyncio.gather(self._update_node_stats(), self._update_actors(),
                             self._update_log_info(),
                             self._update_error_info())
