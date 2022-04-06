import logging

import aiohttp.web

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as dashboard_optional_utils

from ray.dashboard.optional_utils import rest_response

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


class WorkerHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    @routes.get("/api/v0/workers")
    async def get_workers(self, req) -> aiohttp.web.Response:
        workers = await self._dashboard_head.gcs_state_aggregator.get_workers()
        return rest_response(success=True, message="", result=workers)

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
