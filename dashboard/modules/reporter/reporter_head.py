import json
import logging

import aiohttp.web
from aioredis.pubsub import Receiver
from grpc.experimental import aio as aiogrpc

import ray
import ray.gcs_utils
import ray.new_dashboard.modules.reporter.reporter_consts as reporter_consts
import ray.new_dashboard.utils as dashboard_utils
import ray.services
import ray.utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
from ray.new_dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class ReportHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            ip, port = change.old
            self._stubs.pop(ip)
        if change.new:
            ip, ports = change.new
            channel = aiogrpc.insecure_channel(f"{ip}:{ports[1]}")
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            self._stubs[ip] = stub

    @routes.get("/api/launch_profiling")
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = int(req.query["pid"])
        duration = int(req.query["duration"])
        reporter_stub = self._stubs[ip]
        reply = await reporter_stub.GetProfilingStats(
            reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        profiling_info = (json.loads(reply.profiling_stats)
                          if reply.profiling_stats else reply.std_out)
        return await dashboard_utils.rest_response(
            success=True,
            message="Profiling success.",
            profiling_info=profiling_info)

    async def run(self, server):
        aioredis_client = self._dashboard_head.aioredis_client
        receiver = Receiver()

        reporter_key = "{}*".format(reporter_consts.REPORTER_PREFIX)
        await aioredis_client.psubscribe(receiver.pattern(reporter_key))
        logger.info(f"Subscribed to {reporter_key}")

        async for sender, msg in receiver.iter():
            try:
                _, data = msg
                data = json.loads(ray.utils.decode(data))
                DataSource.node_physical_stats[data["ip"]] = data
            except Exception:
                logger.exception(
                    "Error receiving node physical stats from reporter agent.")
