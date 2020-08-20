import logging

import aiohttp.web

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.test.test_utils as test_utils

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class TestAgent(dashboard_utils.DashboardAgentModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    @routes.get("/test/http_get_from_agent")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self._dashboard_agent.http_session,
                                           url)
        return aiohttp.web.json_response(result)

    @routes.head("/test/route_head")
    async def route_head(self, req) -> aiohttp.web.Response:
        pass

    @routes.post("/test/route_post")
    async def route_post(self, req) -> aiohttp.web.Response:
        pass

    @routes.patch("/test/route_patch")
    async def route_patch(self, req) -> aiohttp.web.Response:
        pass

    async def run(self, server):
        pass
