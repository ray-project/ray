import asyncio

import aiohttp

from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable


class TestModule(SubprocessModule):
    """
    For some reason you can't put this inline with the pytest that calls pytest.main.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_finished = False

    async def run(self):
        print("TestModule is running")
        self.run_finished = True

    @SubprocessRouteTable.get("/test")
    async def test(self, request_body: bytes) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            text="Hello, World from GET /test, run_finished: " + str(self.run_finished)
        )

    @SubprocessRouteTable.post("/echo")
    async def echo(self, request_body: bytes) -> aiohttp.web.Response:
        # await works
        await asyncio.sleep(0.1)
        return aiohttp.web.Response(
            body=b"Hello, World from POST /echo from " + request_body
        )

    @SubprocessRouteTable.put("/error")
    async def make_error(self, request_body: bytes) -> aiohttp.web.Response:
        raise ValueError("This is an error")

    @SubprocessRouteTable.put("/error_403")
    async def make_error_403(self, request_body: bytes) -> aiohttp.web.Response:
        raise aiohttp.web.HTTPForbidden(reason="you shall not pass")

    # TODO(ryw): test streaming response.
