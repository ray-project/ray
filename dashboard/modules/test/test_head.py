import logging
import time

import aiohttp.web

import ray.dashboard.modules.test.test_consts as test_consts
import ray.dashboard.modules.test.test_utils as test_utils
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.ray_constants import env_bool
from ray.dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_optional_utils.ClassMethodRouteTable


@dashboard_utils.dashboard_module(
    enable=env_bool(test_consts.TEST_MODULE_ENVIRONMENT_KEY, False)
)
class TestHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._notified_agents = {}
        DataSource.agents.signal.append(self._update_notified_agents)

    async def _update_notified_agents(self, change):
        if change.old:
            ip, port = change.old
            self._notified_agents.pop(ip)
        if change.new:
            ip, ports = change.new
            self._notified_agents[ip] = ports

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

    @routes.get("/test/dump")
    async def dump(self, req) -> aiohttp.web.Response:
        key = req.query.get("key")
        if key is None:
            all_data = {
                k: dict(v)
                for k, v in DataSource.__dict__.items()
                if not k.startswith("_")
            }
            return dashboard_optional_utils.rest_response(
                success=True,
                message="Fetch all data from datacenter success.",
                **all_data,
            )
        else:
            data = dict(DataSource.__dict__.get(key))
            return dashboard_optional_utils.rest_response(
                success=True,
                message=f"Fetch {key} from datacenter success.",
                **{key: data},
            )

    @routes.get("/test/notified_agents")
    async def get_notified_agents(self, req) -> aiohttp.web.Response:
        return dashboard_optional_utils.rest_response(
            success=True,
            message="Fetch notified agents success.",
            **self._notified_agents,
        )

    @routes.get("/test/http_get")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self._dashboard_head.http_session, url)
        return aiohttp.web.json_response(result)

    @routes.get("/test/aiohttp_cache/{sub_path}")
    @dashboard_optional_utils.aiohttp_cache(ttl_seconds=1)
    async def test_aiohttp_cache(self, req) -> aiohttp.web.Response:
        value = req.query["value"]
        return dashboard_optional_utils.rest_response(
            success=True, message="OK", value=value, timestamp=time.time()
        )

    @routes.get("/test/aiohttp_cache_lru/{sub_path}")
    @dashboard_optional_utils.aiohttp_cache(ttl_seconds=60, maxsize=5)
    async def test_aiohttp_cache_lru(self, req) -> aiohttp.web.Response:
        value = req.query.get("value")
        return dashboard_optional_utils.rest_response(
            success=True, message="OK", value=value, timestamp=time.time()
        )

    @routes.get("/test/file")
    async def test_file(self, req) -> aiohttp.web.FileResponse:
        file_path = req.query.get("path")
        logger.info("test file: %s", file_path)
        return aiohttp.web.FileResponse(file_path)

    async def run(self, server):
        pass
