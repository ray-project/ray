import json
import logging
import uuid

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
        self._profiling_stats = {}
        DataSource.agents.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.new:
            ip, port = next(iter(change.new.items()))
            channel = aiogrpc.insecure_channel("{}:{}".format(ip, int(port)))
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            self._stubs[ip] = stub
        if change.old:
            ip, port = next(iter(change.old.items()))
            self._stubs.pop(ip)

    @routes.get("/api/launch_profiling")
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        node_id = req.query.get("node_id")
        pid = int(req.query.get("pid"))
        duration = int(req.query.get("duration"))
        profiling_id = str(uuid.uuid4())
        reporter_stub = self._stubs[node_id]
        reply = await reporter_stub.GetProfilingStats(
            reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        self._profiling_stats[profiling_id] = reply
        return await dashboard_utils.rest_response(
            success=True,
            message="Profiling launched.",
            profiling_id=profiling_id)

    @routes.get("/api/check_profiling_status")
    async def check_profiling_status(self, req) -> aiohttp.web.Response:
        profiling_id = req.query.get("profiling_id")
        is_present = profiling_id in self._profiling_stats
        if not is_present:
            status = {"status": "pending"}
        else:
            reply = self._profiling_stats[profiling_id]
            if reply.stderr:
                status = {"status": "error", "error": reply.stderr}
            else:
                status = {"status": "finished"}
        return await dashboard_utils.rest_response(
            success=True, message="Profiling status fetched.", status=status)

    @routes.get("/api/get_profiling_info")
    async def get_profiling_info(self, req) -> aiohttp.web.Response:
        profiling_id = req.query.get("profiling_id")
        profiling_stats = self._profiling_stats.get(profiling_id)
        assert profiling_stats, "profiling not finished"
        return await dashboard_utils.rest_response(
            success=True,
            message="Profiling info fetched.",
            profiling_info=json.loads(profiling_stats.profiling_stats))

    async def run(self):
        p = self._dashboard_head.aioredis_client
        mpsc = Receiver()

        reporter_key = "{}*".format(reporter_consts.REPORTER_PREFIX)
        await p.psubscribe(mpsc.pattern(reporter_key))
        logger.info("Subscribed to {}".format(reporter_key))

        async for sender, msg in mpsc.iter():
            try:
                _, data = msg
                data = json.loads(ray.utils.decode(data))
                DataSource.node_physical_stats[data["ip"]] = data
            except Exception as ex:
                logger.exception(ex)
