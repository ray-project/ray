import logging
import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class TaskHead(dashboard_utils.DashboardHeadModule):
    """Module to obtain task information of the ray cluster."""

    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/v0/tasks")
    async def get_tasks(self, req) -> aiohttp.web.Response:
        data = await self._dashboard_head.state_aggregator.get_tasks()
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
