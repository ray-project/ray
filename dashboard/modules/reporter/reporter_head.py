import json
import logging
import yaml
import os
import aiohttp.web
from aioredis.pubsub import Receiver

import ray
import ray.dashboard.modules.reporter.reporter_consts as reporter_consts
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray._private.gcs_pubsub import gcs_pubsub_enabled, \
    GcsAioResourceUsageSubscriber
import ray._private.services
import ray._private.utils
from ray.ray_constants import (DEBUG_AUTOSCALING_STATUS,
                               DEBUG_AUTOSCALING_STATUS_LEGACY,
                               DEBUG_AUTOSCALING_ERROR)
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc
import ray.experimental.internal_kv as internal_kv
from ray.dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ReportHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        self._ray_config = None
        DataSource.agents.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]
            options = (("grpc.enable_http_proxy", 0), )
            channel = ray._private.utils.init_grpc_channel(
                f"{ip}:{ports[1]}", options=options, asynchronous=True)
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
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Profiling success.",
            profiling_info=profiling_info)

    @routes.get("/api/ray_config")
    async def get_ray_config(self, req) -> aiohttp.web.Response:
        if self._ray_config is None:
            try:
                config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
            except yaml.YAMLError:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message=f"No config found at {config_path}.",
                )
            except FileNotFoundError:
                return dashboard_optional_utils.rest_response(
                    success=False,
                    message="Invalid config, could not load YAML.")

            payload = {
                "min_workers": cfg.get("min_workers", "unspecified"),
                "max_workers": cfg.get("max_workers", "unspecified")
            }

            try:
                payload["head_type"] = cfg["head_node"]["InstanceType"]
            except KeyError:
                payload["head_type"] = "unknown"

            try:
                payload["worker_type"] = cfg["worker_nodes"]["InstanceType"]
            except KeyError:
                payload["worker_type"] = "unknown"

            self._ray_config = payload

        return dashboard_optional_utils.rest_response(
            success=True,
            message="Fetched ray config.",
            **self._ray_config,
        )

    @routes.get("/api/cluster_status")
    async def get_cluster_status(self, req):
        """Returns status information about the cluster.

        Currently contains two fields:
            autoscaling_status (str): a status message from the autoscaler.
            autoscaling_error (str): an error message from the autoscaler if
                anything has gone wrong during autoscaling.

        These fields are both read from the GCS, it's expected that the
        autoscaler writes them there.
        """

        assert ray.experimental.internal_kv._internal_kv_initialized()
        legacy_status = internal_kv._internal_kv_get(
            DEBUG_AUTOSCALING_STATUS_LEGACY)
        formatted_status_string = internal_kv._internal_kv_get(
            DEBUG_AUTOSCALING_STATUS)
        formatted_status = json.loads(formatted_status_string.decode()
                                      ) if formatted_status_string else {}
        error = internal_kv._internal_kv_get(DEBUG_AUTOSCALING_ERROR)
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Got cluster status.",
            autoscaling_status=legacy_status.decode()
            if legacy_status else None,
            autoscaling_error=error.decode() if error else None,
            cluster_status=formatted_status if formatted_status else None,
        )

    async def run(self, server):
        if gcs_pubsub_enabled():
            gcs_addr = await self._dashboard_head.get_gcs_address()
            subscriber = GcsAioResourceUsageSubscriber(gcs_addr)
            await subscriber.subscribe()

            while True:
                try:
                    # The key is b'RAY_REPORTER:{node id hex}',
                    # e.g. b'RAY_REPORTER:2b4fbd...'
                    key, data = await subscriber.poll()
                    if key is None:
                        continue
                    data = json.loads(data)
                    node_id = key.split(":")[-1]
                    DataSource.node_physical_stats[node_id] = data
                except Exception:
                    logger.exception("Error receiving node physical stats "
                                     "from reporter agent.")
        else:
            receiver = Receiver()
            aioredis_client = self._dashboard_head.aioredis_client
            reporter_key = "{}*".format(reporter_consts.REPORTER_PREFIX)
            await aioredis_client.psubscribe(receiver.pattern(reporter_key))
            logger.info(f"Subscribed to {reporter_key}")

            async for sender, msg in receiver.iter():
                try:
                    key, data = msg
                    data = json.loads(ray._private.utils.decode(data))
                    key = key.decode("utf-8")
                    node_id = key.split(":")[-1]
                    DataSource.node_physical_stats[node_id] = data
                except Exception:
                    logger.exception("Error receiving node physical stats "
                                     "from reporter agent.")
