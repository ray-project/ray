import logging
import time

import aiohttp.web

import ray.dashboard.modules.tests.test_consts as test_consts
import ray.dashboard.modules.tests.test_utils as test_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import env_bool

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.DashboardHeadRouteTable


@dashboard_utils.dashboard_module(
    enable=env_bool(test_consts.TEST_MODULE_ENVIRONMENT_KEY, False)
)
class TestHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, config: dashboard_utils.DashboardHeadModuleConfig):
        super().__init__(config)

    @staticmethod
    def is_minimal_module():
        return False

    @routes.get("/test/route_get")
    async def route_get(self, req) -> aiohttp.web.Response:
        pass

    @routes.put("/test/route_put")
    async def route_put(self, req) -> aiohttp.web.Response:
        pass

    @routes.delete("/test/route_delete")
    async def route_delete(self, req) -> aiohttp.web.Response:
        pass

    @routes.view("/test/route_view")
    async def route_view(self, req) -> aiohttp.web.Response:
        pass

    @routes.get("/test/http_get")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self.http_session, url)
        return aiohttp.web.json_response(result)

    @routes.get("/test/aiohttp_cache/{sub_path}")
    @dashboard_optional_utils.aiohttp_cache(ttl_seconds=1)
    async def test_aiohttp_cache(self, req) -> aiohttp.web.Response:
        value = req.query["value"]
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="OK",
            value=value,
            timestamp=time.time(),
        )

    @routes.get("/test/aiohttp_cache_lru/{sub_path}")
    @dashboard_optional_utils.aiohttp_cache(ttl_seconds=60, maxsize=5)
    async def test_aiohttp_cache_lru(self, req) -> aiohttp.web.Response:
        value = req.query.get("value")
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="OK",
            value=value,
            timestamp=time.time(),
        )

    @routes.get("/test/file")
    async def test_file(self, req) -> aiohttp.web.FileResponse:
        file_path = req.query.get("path")
        logger.info("test file: %s", file_path)
        return aiohttp.web.FileResponse(file_path)

    @routes.get("/test/block_event_loop")
    async def block_event_loop(self, req) -> aiohttp.web.Response:
        """
        Simulates a blocked event loop. To be used for testing purposes only. Creates a
        task that blocks the event loop for a specified number of seconds.
        """
        seconds = float(req.query.get("seconds", 0.0))
        time.sleep(seconds)

        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message=f"Blocked event loop for {seconds} seconds",
            timestamp=time.time(),
        )

    async def run(self):
        pass


if __name__ == "__main__":
    pass
