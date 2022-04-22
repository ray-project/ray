import logging

import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class RuntimeEnvHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/v0/runtime_envs")
    @dashboard_optional_utils.aiohttp_cache
    async def list_runtime_envs(self, req) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_runtime_envs()
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
