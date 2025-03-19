import sys
import asyncio
import logging
import multiprocessing
import os
from typing import Optional, Union
import multidict

from ray.dashboard.optional_deps import aiohttp

from ray.dashboard.subprocesses.module import (
    SubprocessModule,
    SubprocessModuleConfig,
    run_module,
)
from ray.dashboard.subprocesses.utils import (
    module_logging_filename,
    ResponseType,
)

"""
This file contains code run in the parent process. It can start a subprocess and send
messages to it. Requires non-minimal Ray.
"""

logger = logging.getLogger(__name__)


def filter_hop_by_hop_headers(
    headers: Union[dict[str, str], multidict.CIMultiDictProxy[str]]
) -> dict[str, str]:
    """
    Filter out hop-by-hop headers from the headers dict.
    """
    HOP_BY_HOP_HEADERS = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
    if isinstance(headers, multidict.CIMultiDictProxy):
        headers = dict(headers)
    filtered_headers = {
        key: value
        for key, value in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }
    return filtered_headers


class SubprocessModuleHandle:
    """
    A handle to a module created as a subprocess. Can send messages to the module and
    receive responses. It only acts as a proxy to the aiohttp server running in the
    subprocess. On destruction, the subprocess is terminated.

    Lifecycle:
    1. In SubprocessModuleHandle creation, the subprocess is started and runs an aiohttp
       server.
    2. User must call start_module() and wait_for_module_ready() first.
    3. SubprocessRouteTable.bind(handle)
    4. app.add_routes(routes=SubprocessRouteTable.bound_routes())
    5. Run the app.

    Health check (_do_periodic_health_check):
    Every 1s, do a health check by _do_once_health_check. If the module is
    unhealthy:
      1. log the exception
      2. log the last N lines of the log file
      3. fail all active requests
      4. restart the module

    TODO(ryw): define policy for health check:
    - check period (Now: 1s)
    - define unhealthy. (Now: process exits. TODO: check_health() for event loop hang)
    - check number of failures in a row before we deem it unhealthy (Now: N/A)
    - "max number of restarts"? (Now: infinite)
    """

    # Class variable. Force using spawn because Ray C bindings have static variables
    # that need to be re-initialized for a new process.
    mp_context = multiprocessing.get_context("spawn")

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        module_cls: type[SubprocessModule],
        config: SubprocessModuleConfig,
    ):
        self.loop = loop
        self.module_cls = module_cls
        self.config = config

        # Increment this when the module is restarted.
        self.incarnation = 0
        # Runtime states, set by start_module() and wait_for_module_ready(),
        # reset by destroy_module().
        self.process_ready_event = self.mp_context.Event()
        self.process = None
        self.http_client_session: Optional[aiohttp.ClientSession] = None
        self.health_check_task = None

    def str_for_state(self, incarnation: int, pid: Optional[int]):
        return f"SubprocessModuleHandle(module_cls={self.module_cls.__name__}, incarnation={incarnation}, pid={pid})"

    def __str__(self):
        return self.str_for_state(
            self.incarnation, self.process.pid if self.process else None
        )

    def start_module(self):
        """
        Start the module. Should be non-blocking.
        """
        self.process = self.mp_context.Process(
            target=run_module,
            args=(
                self.module_cls,
                self.config,
                self.process_ready_event,
            ),
            daemon=True,
        )
        self.process.start()

    def wait_for_module_ready(self):
        """
        Wait for the module to be ready. This is called after start_module()
        and can be blocking.
        """
        self.process_ready_event.wait()

        socket_path = os.path.join(
            self.config.socket_dir, "dashboard_" + self.module_cls.__name__
        )
        if sys.platform == "win32":
            connector = aiohttp.NamedPipeConnector(socket_path)
        else:
            connector = aiohttp.UnixConnector(socket_path)
        self.http_client_session = aiohttp.ClientSession(connector=connector)

        self.health_check_task = self.loop.create_task(self._do_periodic_health_check())

    async def destroy_module(self):
        """
        Destroy the module. This is called when the module is unhealthy.
        """
        self.incarnation += 1
        self.process_ready_event.clear()

        if self.process:
            self.process.kill()
            self.process.join()
            self.process = None

        if self.http_client_session:
            await self.http_client_session.close()
            self.http_client_session = None

        if self.health_check_task:
            self.health_check_task.cancel()
            self.health_check_task = None

    async def _health_check(self) -> aiohttp.web.Response:
        """
        Do internal health check. The module should respond immediately with a 200 OK.
        This can be used to measure module responsiveness in RTT, it also indicates
        subprocess event loop lag.

        Currently you get a 200 OK with body = b'success'. Later if we want we can add more
        observability payloads.
        """
        resp = await self.http_client_session.get("http://localhost/api/healthz")
        return aiohttp.web.Response(
            status=resp.status,
            headers=filter_hop_by_hop_headers(resp.headers),
            body=await resp.read(),
        )

    async def _do_once_health_check(self):
        """
        Do a health check once. We check for:
        1. if the process exits, it's considered died.
        2. if the health check endpoint returns non-200, it's considered unhealthy.

        """
        if self.process.exitcode is not None:
            raise RuntimeError(f"Process exited with code {self.process.exitcode}")
        resp = await self._health_check()
        if resp.status != 200:
            raise RuntimeError(f"Health check failed: status code is {resp.status}")

    async def _do_periodic_health_check(self):
        """
        Every 1s, do a health check. If the module is unhealthy:
        1. log the exception
        2. log the last N lines of the log file
        3. restart the module
        """
        while True:
            try:
                await self._do_once_health_check()
            except Exception:
                filename = module_logging_filename(
                    self.module_cls.__name__, self.config.logging_filename
                )
                logger.exception(
                    f"Module {self.module_cls.__name__} is unhealthy. Please refer to "
                    f"{self.config.log_dir}/{filename} for more details. Failing all "
                    "active requests."
                )
                await self.destroy_module()
                self.start_module()
                self.wait_for_module_ready()
                return
            await asyncio.sleep(1)

    async def proxy_request(
        self, request: aiohttp.web.Request, resp_type: ResponseType = ResponseType.HTTP
    ) -> aiohttp.web.StreamResponse:
        """
        Sends a new request to the subprocess and returns the response.
        """
        if resp_type == ResponseType.HTTP:
            return await self.proxy_http(request)
        if resp_type == ResponseType.STREAM:
            return await self.proxy_stream(request)
        if resp_type == ResponseType.WEBSOCKET:
            return await self.proxy_websocket(request)
        raise ValueError(f"Unknown response type: {resp_type}")

    async def proxy_http(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """
        Proxy handler for non-streaming HTTP API
        It forwards the method, query string, headers, and body to the backend.
        """
        url = f"http://localhost{request.path_qs}"
        body = await request.read()

        async with self.http_client_session.request(
            request.method,
            url,
            data=body,
            headers=filter_hop_by_hop_headers(request.headers),
        ) as backend_resp:
            resp_body = await backend_resp.read()
            return aiohttp.web.Response(
                status=backend_resp.status,
                headers=filter_hop_by_hop_headers(backend_resp.headers),
                body=resp_body,
            )

    async def proxy_stream(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        """
        Proxy handler for streaming HTTP API.
        It forwards the method, query string, and body to the backend.
        """
        url = f"http://localhost{request.path_qs}"
        body = await request.read()
        async with self.http_client_session.request(
            request.method,
            url,
            data=body,
            headers=filter_hop_by_hop_headers(request.headers),
        ) as backend_resp:
            proxy_resp = aiohttp.web.StreamResponse(status=backend_resp.status)
            await proxy_resp.prepare(request)

            async for chunk, _ in backend_resp.content.iter_chunks():
                await proxy_resp.write(chunk)
            await proxy_resp.write_eof()
            return proxy_resp

    async def proxy_websocket(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.WebSocketResponse:
        """
        Proxy handler for WebSocket API
        It establishes a WebSocket connection with the client and simultaneously connects
        to the backend server's WebSocket endpoint. Messages are forwarded in single
        direction from the backend to the client.

        TODO: Support bidirectional communication if needed. We only support one direction
              because it's sufficient for the current use case.
        """
        ws_from_client = aiohttp.web.WebSocketResponse()
        await ws_from_client.prepare(request)

        url = f"http://localhost{request.path_qs}"

        async with self.http_client_session.ws_connect(
            url, headers=filter_hop_by_hop_headers(request.headers)
        ) as ws_to_backend:
            async for msg in ws_to_backend:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await ws_from_client.send_str(msg.data)
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    await ws_from_client.send_bytes(msg.data)
                else:
                    logger.error(f"Unknown msg type: {msg.type}")
                    await ws_from_client.close()

        await ws_from_client.close()
        return ws_from_client
