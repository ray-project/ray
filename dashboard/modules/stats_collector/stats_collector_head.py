import asyncio
import logging

import aiohttp.web
from aioredis.pubsub import Receiver
from grpc.experimental import aio as aiogrpc

import ray.gcs_utils
import ray.new_dashboard.modules.stats_collector.stats_collector_consts \
    as stats_collector_consts
import ray.new_dashboard.utils as dashboard_utils
from ray.new_dashboard.utils import async_loop_forever
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.new_dashboard.datacenter import DataSource, DataOrganizer
from ray.utils import binary_to_hex

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


def node_stats_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {
            "actorId", "jobId", "taskId", "parentTaskId", "sourceActorId",
            "callerId", "rayletId", "workerId"
        })


def actor_table_data_to_dict(message):
    return dashboard_utils.message_to_dict(
        message, {
            "actorId", "parentId", "jobId", "workerId", "rayletId",
            "actorCreationDummyObjectId"
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
        DataSource.nodes.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            ip, port = change.old
            self._stubs.pop(ip)
        if change.new:
            ip, node_info = change.new
            address = "{}:{}".format(ip, int(node_info["nodeManagerPort"]))
            channel = aiogrpc.insecure_channel(address)
            stub = node_manager_pb2_grpc.NodeManagerServiceStub(channel)
            self._stubs[ip] = stub

    @routes.get("/nodes")
    async def get_all_nodes(self, req) -> aiohttp.web.Response:
        view = req.query.get("view")
        if view == "summary":
            all_node_summary = await DataOrganizer.get_all_node_summary()
            return await dashboard_utils.rest_response(
                success=True,
                message="Node summary fetched.",
                summary=all_node_summary)
        elif view is not None and view.lower() == "hostNameList".lower():
            return await dashboard_utils.rest_response(
                success=True,
                message="Node hostname list fetched.",
                host_name_list=list(DataSource.hostname_to_ip.keys()))
        else:
            return await dashboard_utils.rest_response(
                success=False, message=f"Unknown view {view}")

    @routes.get("/nodes/{hostname}")
    async def get_node(self, req) -> aiohttp.web.Response:
        hostname = req.match_info.get("hostname")
        node_info = await DataOrganizer.get_node_info(hostname)
        return await dashboard_utils.rest_response(
            success=True, message="Node detail fetched.", detail=node_info)

    async def _update_actors(self):
        # Subscribe actor channel.
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        key = "{}:*".format(stats_collector_consts.ACTOR_CHANNEL)
        pattern = receiver.pattern(key)
        await aioredis_client.psubscribe(pattern)
        logger.info("Subscribed to %s", key)

        # Get all actor info.
        while True:
            try:
                logger.info("Getting all actor info from GCS.")
                request = gcs_service_pb2.GetAllActorInfoRequest()
                reply = await self._gcs_actor_info_stub.GetAllActorInfo(
                    request, timeout=2)
                if reply.status.code == 0:
                    result = {}
                    for actor_info in reply.actor_table_data:
                        result[binary_to_hex(actor_info.actor_id)] = \
                            actor_table_data_to_dict(actor_info)
                    DataSource.actors.reset(result)
                    logger.info("Received %d actor info from GCS.",
                                len(result))
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
                _, data = msg
                pubsub_message = ray.gcs_utils.PubSubMessage.FromString(data)
                actor_info = ray.gcs_utils.ActorTableData.FromString(
                    pubsub_message.data)
                DataSource.actors[binary_to_hex(actor_info.actor_id)] = \
                    actor_table_data_to_dict(actor_info)
            except Exception:
                logger.exception("Error receiving actor info.")

    @async_loop_forever(
        stats_collector_consts.NODE_STATS_UPDATE_INTERVAL_SECONDS)
    async def _update_node_stats(self):
        for ip, stub in self._stubs.items():
            node_info = DataSource.nodes.get(ip)
            if node_info["state"] != "ALIVE":
                continue
            try:
                reply = await stub.GetNodeStats(
                    node_manager_pb2.GetNodeStatsRequest(), timeout=2)
                reply_dict = node_stats_to_dict(reply)
                DataSource.node_stats[ip] = reply_dict
            except Exception:
                logger.exception(f"Error updating node stats of {ip}.")

    async def run(self, server):
        gcs_channel = self._dashboard_head.aiogrpc_gcs_channel
        self._gcs_job_info_stub = \
            gcs_service_pb2_grpc.JobInfoGcsServiceStub(gcs_channel)
        self._gcs_actor_info_stub = \
            gcs_service_pb2_grpc.ActorInfoGcsServiceStub(gcs_channel)

        await asyncio.gather(self._update_node_stats(), self._update_actors())
