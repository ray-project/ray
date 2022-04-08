import logging

import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class PlacementGroupHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/v0/placement_groups")
    async def get_placement_groups(self, req) -> aiohttp.web.Response:
        pgs = await self._dashboard_head.gcs_state_aggregator.get_placement_groups()
        return rest_response(success=True, message="", result=pgs)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
