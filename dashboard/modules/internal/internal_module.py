import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils
from aiohttp.web import Request, Response
import logging
import asyncio
import time

logger = logging.getLogger(__name__)

routes = optional_utils.ClassMethodRouteTable

LOAD_HEARBEAT_INTERVAL_S = 2

class InternalStateModule:
    # @routes.get("/internal_api/v0/memory_snapshot")
    # async def memory_snapshot(self, req: Request) -> Response:
    #     from pympler import summary, muppy
    #     sum1 = summary.summarize(muppy.get_objects())
    #     await asyncio.sleep(5)
    #     sum2 = summary.summarize(muppy.get_objects())
    #     diff = summary.get_diff(sum1, sum2)
    #     logger.info(diff)
    #     return dashboard_utils.rest_response(
    #         success=True, message="", output=diff
    #     )

    @dashboard_utils.async_loop_forever(LOAD_HEARBEAT_INTERVAL_S)
    async def _check_load(self):
        now = time.monotonic()
        await asyncio.sleep(LOAD_HEARBEAT_INTERVAL_S)
        if time.monotonic() - now > LOAD_HEARBEAT_INTERVAL_S * 1.5:
            logger.info("SANG-TODO Trouble {}".format(time.monotonic() - now))

    async def loop(self):
        await asyncio.gather(self._check_load())

class InternalStateHead(dashboard_utils.DashboardHeadModule, InternalStateModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    async def run(self, server):
        await super().loop()

    @staticmethod
    def is_minimal_module():
        return False


class InternalStateAgent(dashboard_utils.DashboardAgentModule, InternalStateModule):
    def __init__(self, dashboard_agent):
        super().__init__(dashboard_agent)

    async def run(self, server):
        await super().loop()

    @staticmethod
    def is_minimal_module():
        return False
