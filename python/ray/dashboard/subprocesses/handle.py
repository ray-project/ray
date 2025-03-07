import sys
import asyncio
import logging
import multiprocessing
import os
from typing import Optional

from ray.dashboard.optional_deps import aiohttp

from ray.dashboard.subprocesses.module import (
    SubprocessModule,
    SubprocessModuleConfig,
    run_module,
)
from ray.dashboard.subprocesses.utils import (
    module_logging_filename,
)

"""
This file contains code run in the parent process. It can start a subprocess and send
messages to it. Requires non-minimal Ray.
"""

logger = logging.getLogger(__name__)


class SubprocessModuleHandle:
    """
    A handle to a module created as a subprocess. Can send messages to the module and
    receive responses. It only acts as a proxy to the aiohttp server running in the
    subprocess. On destruction, the subprocess is terminated.

    Lifecycle:
    1. In SubprocessModuleHandle creation, the subprocess is started and runs an aiohttp
       server.
    2. User must call SubprocessModuleHandle.start_module() before it can handle parent
       bound messages.
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
        # Runtime states, set by start_module(), reset by destroy_module().
        self.process = None
        self.session = None
        self.health_check_task = None

    def str_for_state(self, incarnation: int, pid: Optional[int]):
        return f"SubprocessModuleHandle(module_cls={self.module_cls.__name__}, incarnation={incarnation}, pid={pid})"

    def __str__(self):
        return self.str_for_state(
            self.incarnation, self.process.pid if self.process else None
        )

    def __del__(self):
        logger.info("SubprocessModuleHandle is being deleted")
        if self.loop.is_running():
            self.loop.run_until_complete(self.destroy_module())

    def start_module(self):
        self.process = self.mp_context.Process(
            target=run_module,
            args=(
                self.module_cls,
                self.config,
                self.incarnation,
                os.getpid(),
            ),
            daemon=True,
            name=f"{self.module_cls.__name__}-{self.incarnation}",
        )
        self.process.start()

        socket_path = os.path.join(self.config.socket_dir, self.module_cls.__name__)
        if sys.platform == "win32":
            connector = aiohttp.NamedPipeConnector(socket_path)
        else:
            connector = aiohttp.UnixConnector(socket_path)
        self.session = aiohttp.ClientSession(connector=connector)

        self.health_check_task = self.loop.create_task(self._do_periodic_health_check())

    async def destroy_module(self):
        """
        Destroy the module. This is called when the module is unhealthy.
        """
        self.incarnation += 1
        if self.process:
            self.process.kill()
            self.process.join()
            self.process = None

        if self.session:
            await self.session.close()

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
        async with self.session.get("http://localhost/api/healthz") as resp:
            return resp

    async def _do_once_health_check(self):
        """
        Do a health check once. We check for:
        1. if the process exits, it's considered died.

        # TODO(ryw): also do `await self._health_check()` and define a policy to
        # determine if the process is dead.
        """
        if self.process.exitcode is not None:
            raise RuntimeError(f"Process exited with code {self.process.exitcode}")

    async def _do_periodic_health_check(self):
        """
        Every 1s, do a health check. If the module is unhealthy:
        1. log the exception
        2. log the last N lines of the log file
        3. restart the module
        """
        incarnation = self.incarnation
        while True:
            try:
                await self._do_once_health_check()
            except Exception:
                filename = module_logging_filename(
                    self.module_cls.__name__, incarnation, self.config.logging_filename
                )
                if filename is None:
                    filename = "stderr"
                logger.exception(
                    f"Module {self.module_cls.__name__} is unhealthy. Please refer to "
                    f"{self.config.log_dir}/{filename} for more details. Failing all "
                    "active requests."
                )
                await self.destroy_module()
                self.start_module()
                return
            await asyncio.sleep(1)

    async def proxy_request(
        self, request: aiohttp.web.Request, websocket=False
    ) -> aiohttp.web.StreamResponse:
        """
        Sends a new request to the subprocess and returns the response.
        """
        if not websocket:
            return await self.proxy_http(request)
        return await self.proxy_websocket(request)

    async def proxy_http(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        """
        Proxy handler for HTTP API
        It forwards the method, query string, headers, and body to the backend.
        """
        url = f"http://localhost{request.path_qs}"
        body = await request.read()

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

        async with self.session.request(
            request.method, url, data=body, headers=request.headers
        ) as resp:
            resp_body = await resp.read()
            filtered_headers = {
                key: value
                for key, value in resp.headers.items()
                if key.lower() not in HOP_BY_HOP_HEADERS
            }
            return aiohttp.web.Response(
                status=resp.status, headers=filtered_headers, body=resp_body
            )

    async def proxy_websocket(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.StreamResponse:
        """
        Proxy handler for WebSocket API
        It establishes a WebSocket connection with the client and simultaneously connects
        to the backend server's WebSocket endpoint. Messages are forwarded in both directions.
        """
        ws_from_client = aiohttp.web.WebSocketResponse()
        await ws_from_client.prepare(request)

        url = f"http://localhost{request.path_qs}"

        async with self.session.ws_connect(
            url, headers=request.headers
        ) as ws_to_backend:

            async def forward(ws_source, ws_target):
                async for msg in ws_source:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await ws_target.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        await ws_target.send_bytes(msg.data)
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        await ws_target.close()
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

            # Set up two tasks to forward messages in both directions.
            task_client_to_backend = self.loop.create_task(
                forward(ws_from_client, ws_to_backend)
            )
            task_backend_to_client = self.loop.create_task(
                forward(ws_to_backend, ws_from_client)
            )

            # Wait until one direction is done, then cancel the other.
            done, pending = await asyncio.wait(
                [task_client_to_backend, task_backend_to_client],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()

        return ws_from_client
