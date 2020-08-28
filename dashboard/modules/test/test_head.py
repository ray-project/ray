import logging

import aiohttp.web

import ray.new_dashboard.utils as dashboard_utils
import ray.new_dashboard.modules.test.test_utils as test_utils
from ray.new_dashboard.datacenter import DataSource

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class TestHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)
        self._notified_agents = {}
        DataSource.agents.signal.append(self._update_notified_agents)

    async def _update_notified_agents(self, change):
        if change.new:
            ip, ports = next(iter(change.new.items()))
            self._notified_agents[ip] = ports
        if change.old:
            ip, port = next(iter(change.old.items()))
            self._notified_agents.pop(ip)

    @routes.get("/test/dump")
    async def dump(self, req) -> aiohttp.web.Response:
        key = req.query.get("key")
        if key is None:
            all_data = {
                k: dict(v)
                for k, v in DataSource.__dict__.items()
                if not k.startswith("_")
            }
            return await dashboard_utils.rest_response(
                success=True,
                message="Fetch all data from datacenter success.",
                **all_data)
        else:
            data = dict(DataSource.__dict__.get(key))
            return await dashboard_utils.rest_response(
                success=True,
                message="Fetch {} from datacenter success.".format(key),
                **{key: data})

    @routes.get("/test/notified_agents")
    async def get_notified_agents(self, req) -> aiohttp.web.Response:
        return await dashboard_utils.rest_response(
            success=True,
            message="Fetch notified agents success.",
            **self._notified_agents)

    @routes.get("/test/http_get")
    async def get_url(self, req) -> aiohttp.web.Response:
        url = req.query.get("url")
        result = await test_utils.http_get(self._dashboard_head.http_session,
                                           url)
        return aiohttp.web.json_response(result)

    async def run(self, server):
        pass
