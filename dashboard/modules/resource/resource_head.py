import logging

import aiohttp.web
import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ResourceHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/v0/resources/summary/cluster")
    async def get_resource_summary_cluster(self, request) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_resource_summary()
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    @routes.get("/api/v0/resources/summary/nodes")
    async def get_resource_summary_nodes(self, request) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_resource_summary(
            per_node=True
        )
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    @routes.get("/api/v0/resources/usage/cluster")
    async def get_task_resource_usage_cluster(self, request) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_task_resource_usage()
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    @routes.get("/api/v0/resources/usage/nodes")
    async def get_task_resource_usage_nodes(self, request) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_task_resource_usage(
            per_node=True
        )
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
