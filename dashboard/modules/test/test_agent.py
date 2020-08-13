import logging

import aiohttp.web

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.test.test_utils as test_utils

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class HeadAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    @routes.get("/test/http_get_from_agent")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self._dashboard_agent.http_session,
                                           url)
        return aiohttp.web.json_response(result)

    async def run(self, server):
        pass
