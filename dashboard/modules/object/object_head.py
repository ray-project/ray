import logging

import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class ObjectHead(dashboard_utils.DashboardHeadModule):
    """Module to obtain object information of the ray cluster."""

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/v0/objects")
    async def get_objects(self, req) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_objects()
        return rest_response(
            success=True, message="", result=data, convert_google_style=False
        )

    async def run(self, server):
        # Run method is required to implement for subclass of DashboardHead.
        # Since object module only includes the state api, we don't need to
        # do anything.
        pass

    @staticmethod
    def is_minimal_module():
        return False
