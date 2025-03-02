import json
import logging

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import DEBUG_AUTOSCALING_STATUS
from ray.dashboard.modules.accelerator.accelerator_consts import accelerators

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


class ResourcesHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._gcs_aio_client = dashboard_head.gcs_aio_client

    @routes.get("/accelerator/get_accelerators")
    async def get_accelerators(self, req) -> aiohttp.web.Response:
        formatted_status_string = await self._gcs_aio_client.internal_kv_get(
            DEBUG_AUTOSCALING_STATUS.encode(), namespace=None
        )
        status_dict = json.loads(formatted_status_string.decode("utf-8"))
        usage_dict = status_dict.get("load_metrics_report").get("usage")

        result = []
        for accelerator, (used, total) in usage_dict.items():
            if accelerator in accelerators:
                result.append(accelerators[accelerator])

        return dashboard_optional_utils.rest_response(
            success=True, message="", result=result
        )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
