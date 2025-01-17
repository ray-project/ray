import asyncio
import logging
import os
import signal
from typing import AsyncIterator

from ray.dashboard.optional_deps import aiohttp

from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable

logger = logging.getLogger(__name__)


class TestModule(SubprocessModule):
    """
    For some reason you can't put this inline with the pytest that calls pytest.main.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_finished = False

    async def init(self):
        logger.info("TestModule is initing")
        self.run_finished = True
        await asyncio.sleep(0.1)
        logger.info("TestModule is done initing")

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
        # For an ascii art of Gandalf the Grey, see:
        # https://github.com/ray-project/ray/pull/49732#discussion_r1919292428
        raise aiohttp.web.HTTPForbidden(reason="you shall not pass")

    @SubprocessRouteTable.post("/streamed_iota", streaming=True)
    async def streamed_iota(self, request_body: bytes) -> AsyncIterator[bytes]:
        """
        Streams the numbers 0 to N.
        """
        n = int(request_body)
        for i in range(n):
            await asyncio.sleep(0.001)
            yield f"{i}\n".encode()

    @SubprocessRouteTable.post("/streamed_iota_with_error", streaming=True)
    async def streamed_iota_with_error(
        self, request_body: bytes
    ) -> AsyncIterator[bytes]:
        """
        Streams the numbers 0 to N, then raises an error.
        """
        n = int(request_body)
        for i in range(n):
            await asyncio.sleep(0.001)
            yield f"{i}\n".encode()
        raise ValueError("This is an error")

    @SubprocessRouteTable.post("/logging_in_module")
    async def logging_in_module(self, request_body: bytes) -> aiohttp.web.Response:
        logger.info("In /logging_in_module, Not all those who wander are lost.")
        return aiohttp.web.Response(text="done!")

    @SubprocessRouteTable.post("/kill_self")
    async def kill_self(self, request_body: bytes) -> aiohttp.web.Response:
        logger.error("Crashing by sending myself a sigkill")
        os.kill(os.getpid(), signal.SIGKILL)
        asyncio.sleep(1000)
        return aiohttp.web.Response(text="done!")
