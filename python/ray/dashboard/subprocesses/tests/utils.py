import asyncio
import logging
import os
import signal
import sys
from typing import AsyncIterator

from ray.dashboard import optional_utils
from ray.dashboard.optional_deps import aiohttp
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes
from ray.dashboard.subprocesses.utils import ResponseType

logger = logging.getLogger(__name__)


class BaseTestModule(SubprocessModule):
    @property
    def gcs_client(self):
        return None

    @property
    def aiogrpc_gcs_channel(self):
        return None


class TestModule(BaseTestModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_finished = False
        self.not_cached_count = 0
        self.cached_count = 0

    async def run(self):
        await super().run()
        logger.info("TestModule is initing")
        self.run_finished = True
        await asyncio.sleep(0.1)
        logger.info("TestModule is done initing")

    @routes.get("/test")
    async def test(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(
            text="Hello, World from GET /test, run_finished: " + str(self.run_finished)
        )

    @routes.post("/echo")
    async def echo(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        # await works
        await asyncio.sleep(0.1)
        body = await req.text()
        return aiohttp.web.Response(text="Hello, World from POST /echo from " + body)

    @routes.get("/not_cached")
    async def not_cached(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        self.not_cached_count += 1
        return aiohttp.web.Response(
            text=f"Hello, World from GET /not_cached, count: {self.not_cached_count}"
        )

    @routes.get("/cached")
    @optional_utils.aiohttp_cache
    async def cached(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        self.cached_count += 1
        return aiohttp.web.Response(
            text=f"Hello, World from GET /cached, count: {self.cached_count}"
        )

    @routes.put("/error")
    async def make_error(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        raise ValueError("This is an error")

    @routes.put("/error_403")
    async def make_error_403(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        # For an ascii art of Gandalf the Grey, see:
        # https://github.com/ray-project/ray/pull/49732#discussion_r1919292428
        raise aiohttp.web.HTTPForbidden(reason="you shall not pass")

    @routes.post("/streamed_iota", resp_type=ResponseType.STREAM)
    async def streamed_iota(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        """
        Streams the numbers 0 to N.
        """
        request_body = await req.text()
        n = int(request_body)
        resp = aiohttp.web.StreamResponse()
        await resp.prepare(req)
        for i in range(n):
            await asyncio.sleep(0.001)
            await resp.write(f"{i}\n".encode())
        await resp.write_eof()
        return resp

    @routes.post("/streamed_401", resp_type=ResponseType.STREAM)
    async def streamed_401(self, req: aiohttp.web.Request) -> AsyncIterator[bytes]:
        """
        Raise a 401 error instead of streaming.
        """
        raise aiohttp.web.HTTPUnauthorized(
            reason="Unauthorized although I am not a teapot"
        )

    @routes.get("/websocket_one_to_five_bytes", resp_type=ResponseType.WEBSOCKET)
    async def websocket_one_to_five_bytes(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.WebSocketResponse:
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(req)
        for i in range(1, 6):
            await asyncio.sleep(0.001)
            await ws.send_bytes(f"{i}\n".encode())
        await ws.close()
        return ws

    @routes.get("/websocket_one_to_five_strs", resp_type=ResponseType.WEBSOCKET)
    async def websocket_one_to_five_strs(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.WebSocketResponse:
        ws = aiohttp.web.WebSocketResponse()
        await ws.prepare(req)
        for i in range(1, 6):
            await asyncio.sleep(0.001)
            await ws.send_str(f"{i}\n")
        await ws.close()
        return ws

    @routes.get("/websocket_raise_http_error", resp_type=ResponseType.WEBSOCKET)
    async def websocket_raise_http_error(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.WebSocketResponse:
        raise aiohttp.web.HTTPBadRequest(reason="Hello this is a bad request")

    @routes.get("/websocket_raise_non_http_error", resp_type=ResponseType.WEBSOCKET)
    async def websocket_raise_non_http_error(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.WebSocketResponse:
        raise ValueError("Hello world")

    @routes.post("/run_forever")
    async def run_forever(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        while True:
            await asyncio.sleep(1)
        return aiohttp.web.Response(text="done in the infinite future!")

    @routes.post("/logging_in_module")
    async def logging_in_module(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        request_body_str = await req.text()
        logger.info(f"In /logging_in_module, {request_body_str}.")
        print("In /logging_in_module, stdout")
        print("In /logging_in_module, stderr", file=sys.stderr)
        return aiohttp.web.Response(text="done!")

    @routes.post("/kill_self")
    async def kill_self(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        logger.error("Crashing by sending myself a sigkill")
        os.kill(os.getpid(), signal.SIGKILL)
        asyncio.sleep(1000)
        return aiohttp.web.Response(text="done!")


class TestModule1(BaseTestModule):
    @routes.get("/test1")
    async def test(self, req: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.Response(text="Hello from TestModule1")

    @routes.get("/redirect_between_modules")
    async def redirect_between_modules(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        # Redirect to the /test route in TestModule
        raise aiohttp.web.HTTPFound(location="/test")

    @routes.get("/redirect_within_module")
    async def redirect_within_module(
        self, req: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        raise aiohttp.web.HTTPFound(location="/test1")
