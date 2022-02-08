import json
import logging
import yaml
import os
import aiohttp.web

import ray.new_dashboard.utils as dashboard_utils
from ray.autoscaler._private.util import (
    DEBUG_AUTOSCALING_STATUS,
    DEBUG_AUTOSCALING_STATUS_LEGACY,
    DEBUG_AUTOSCALING_ERROR,
)

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class ClusterHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._ray_config = None

    @routes.get("/api/ray_config")
    async def get_ray_config(self, req) -> aiohttp.web.Response:
        if self._ray_config is None:
            config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
            try:
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
            except yaml.YAMLError:
                return dashboard_utils.rest_response(
                    success=False,
                    message=f"No config found at {config_path}.",
                )
            except FileNotFoundError:
                return dashboard_utils.rest_response(
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

        return dashboard_utils.rest_response(
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

        aioredis_client = self._dashboard_head.aioredis_client
        legacy_status = await aioredis_client.hget(
            DEBUG_AUTOSCALING_STATUS_LEGACY, "value"
        )
        formatted_status_string = await aioredis_client.hget(
            DEBUG_AUTOSCALING_STATUS, "value"
        )
        formatted_status = (
            json.loads(formatted_status_string.decode())
            if formatted_status_string
            else {}
        )
        error = await aioredis_client.hget(DEBUG_AUTOSCALING_ERROR, "value")
        return dashboard_utils.rest_response(
            success=True,
            message="Got cluster status.",
            autoscaling_status=legacy_status.decode() if legacy_status else None,
            autoscaling_error=error.decode() if error else None,
            cluster_status=formatted_status if formatted_status else None,
        )

    async def run(self, server):
        pass
