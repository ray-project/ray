import json
import logging
import os
import asyncio
import aiohttp.web
import yaml

import ray
import ray._private.services
import ray._private.utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.consts import GCS_RPC_TIMEOUT_SECONDS
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioResourceUsageSubscriber
from ray._private.metrics_agent import PrometheusServiceDiscoveryWriter
from ray._private.ray_constants import (
    DEBUG_AUTOSCALING_ERROR,
    DEBUG_AUTOSCALING_STATUS,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
    GLOBAL_GRPC_OPTIONS,
)
from ray.core.generated import reporter_pb2, reporter_pb2_grpc
from ray.dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ReportHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._stubs = {}
        self._ray_config = None
        DataSource.agents.signal.append(self._update_stubs)
        # TODO(fyrestone): Avoid using ray.state in dashboard, it's not
        # asynchronous and will lead to low performance. ray disconnect()
        # will be hang when the ray.state is connected and the GCS is exit.
        # Please refer to: https://github.com/ray-project/ray/issues/16328
        assert dashboard_head.gcs_address or dashboard_head.redis_address
        gcs_address = dashboard_head.gcs_address
        temp_dir = dashboard_head.temp_dir
        self.service_discovery = PrometheusServiceDiscoveryWriter(gcs_address, temp_dir)
        self._gcs_aio_client = dashboard_head.gcs_aio_client

    async def _update_stubs(self, change):
        if change.old:
            node_id, port = change.old
            ip = DataSource.node_id_to_ip[node_id]
            self._stubs.pop(ip)
        if change.new:
            node_id, ports = change.new
            ip = DataSource.node_id_to_ip[node_id]
            options = GLOBAL_GRPC_OPTIONS
            channel = ray._private.utils.init_grpc_channel(
                f"{ip}:{ports[1]}", options=options, asynchronous=True
            )
            stub = reporter_pb2_grpc.ReporterServiceStub(channel)
            self._stubs[ip] = stub

    @routes.get("/api/launch_profiling")
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        ip = req.query["ip"]
        pid = int(req.query["pid"])
        duration = int(req.query["duration"])
        reporter_stub = self._stubs[ip]
        reply = await reporter_stub.GetProfilingStats(
            reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration)
        )
        profiling_info = (
            json.loads(reply.profiling_stats)
            if reply.profiling_stats
            else reply.std_out
        )
        return dashboard_optional_utils.rest_response(
            success=True, message="Profiling success.", profiling_info=profiling_info
        )

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
                    success=False, message="Invalid config, could not load YAML."
                )

            payload = {
                "min_workers": cfg.get("min_workers", "unspecified"),
                "max_workers": cfg.get("max_workers", "unspecified"),
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
            autoscaling_status (str)-- a status message from the autoscaler.
            autoscaling_error (str)-- an error message from the autoscaler if
                anything has gone wrong during autoscaling.

        These fields are both read from the GCS, it's expected that the
        autoscaler writes them there.
        """

        (legacy_status, formatted_status_string, error) = await asyncio.gather(
            *[
                self._gcs_aio_client.internal_kv_get(
                    key.encode(), namespace=None, timeout=GCS_RPC_TIMEOUT_SECONDS
                )
                for key in [
                    DEBUG_AUTOSCALING_STATUS_LEGACY,
                    DEBUG_AUTOSCALING_STATUS,
                    DEBUG_AUTOSCALING_ERROR,
                ]
            ]
        )

        formatted_status = (
            json.loads(formatted_status_string.decode())
            if formatted_status_string
            else {}
        )
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Got cluster status.",
            autoscaling_status=legacy_status.decode() if legacy_status else None,
            autoscaling_error=error.decode() if error else None,
            cluster_status=formatted_status if formatted_status else None,
        )

    async def run(self, server):
        # Need daemon True to avoid dashboard hangs at exit.
        self.service_discovery.daemon = True
        self.service_discovery.start()
        gcs_addr = self._dashboard_head.gcs_address
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
                logger.exception(
                    "Error receiving node physical stats from reporter agent."
                )

    @staticmethod
    def is_minimal_module():
        return False
