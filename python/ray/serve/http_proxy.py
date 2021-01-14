import asyncio
import socket
from typing import List

import uvicorn
import starlette.responses

import ray
from ray.exceptions import RayTaskError
from ray.serve.constants import LongPollKey
from ray.util import metrics
from ray.serve.utils import _get_logger
from ray.serve.http_util import Response, build_starlette_request
from ray.serve.long_poll import LongPollAsyncClient
from ray.serve.router import Router
from ray.serve.handle import DEFAULT

logger = _get_logger()


class HTTPProxy:
    """This class is meant to be instantiated and run by an ASGI HTTP server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(controller_name))
    """

    def __init__(self, controller_name):
        # Set the controller name so that serve.connect() will connect to the
        # controller instance this proxy is running in.
        ray.serve.api._set_internal_replica_context(None, None,
                                                    controller_name)
        self.client = ray.serve.connect()

        controller = ray.get_actor(controller_name)
        self.route_table = {}  # Should be updated via long polling.
        self.router = Router(controller)
        self.long_poll_client = LongPollAsyncClient(controller, {
            LongPollKey.ROUTE_TABLE: self._update_route_table,
        })

        self.request_counter = metrics.Count(
            "serve_num_http_requests",
            description="The number of HTTP requests processed.",
            tag_keys=("route", ))

    async def setup(self):
        await self.router.setup_in_async_loop()

    async def _update_route_table(self, route_table):
        logger.debug(f"HTTP Proxy: Get updated route table: {route_table}.")
        self.route_table = route_table

    async def receive_http_body(self, scope, receive, send):
        body_buffer = []
        more_body = True
        while more_body:
            message = await receive()
            assert message["type"] == "http.request"

            more_body = message["more_body"]
            body_buffer.append(message["body"])

        return b"".join(body_buffer)

    def _make_error_sender(self, scope, receive, send):
        async def sender(error_message, status_code):
            response = Response(error_message, status_code=status_code)
            await response.send(scope, receive, send)

        return sender

    async def _handle_system_request(self, scope, receive, send):
        current_path = scope["path"]
        if current_path == "/-/routes":
            await Response(self.route_table).send(scope, receive, send)
        else:
            await Response(
                "System path {} not found".format(current_path),
                status_code=404).send(scope, receive, send)

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """

        error_sender = self._make_error_sender(scope, receive, send)

        assert self.route_table is not None, (
            "Route table must be set via set_route_table.")
        assert scope["type"] == "http"
        current_path = scope["path"]

        self.request_counter.record(1, tags={"route": current_path})

        if current_path.startswith("/-/"):
            await self._handle_system_request(scope, receive, send)
            return

        try:
            endpoint_name, methods_allowed = self.route_table[current_path]
        except KeyError:
            error_message = (
                "Path {} not found. "
                "Please ping http://.../-/routes for routing table"
            ).format(current_path)
            await error_sender(error_message, 404)
            return

        if scope["method"] not in methods_allowed:
            error_message = ("Methods {} not allowed. "
                             "Available HTTP methods are {}.").format(
                                 scope["method"], methods_allowed)
            await error_sender(error_message, 405)
            return

        http_body_bytes = await self.receive_http_body(scope, receive, send)

        headers = {k.decode(): v.decode() for k, v in scope["headers"]}

        handle = self.client.get_handle(
            endpoint_name, sync=False).options(
                method_name=headers.get("X-SERVE-CALL-METHOD".lower(),
                                        DEFAULT.VALUE),
                shard_key=headers.get("X-SERVE-SHARD-KEY".lower(),
                                      DEFAULT.VALUE),
                http_method=scope["method"].upper(),
                http_headers=headers)

        request = build_starlette_request(scope, http_body_bytes)
        object_ref = await handle.remote(request)
        result = await object_ref

        if isinstance(result, RayTaskError):
            error_message = "Task Error. Traceback: {}.".format(result)
            await error_sender(error_message, 500)
        elif isinstance(result, starlette.responses.Response):
            await result(scope, receive, send)
        else:
            await Response(result).send(scope, receive, send)


@ray.remote
class HTTPProxyActor:
    async def __init__(
            self,
            host,
            port,
            controller_name,
            http_middlewares: List[
                "starlette.middleware.Middleware"] = []):  # noqa: F821
        self.host = host
        self.port = port

        self.setup_complete = asyncio.Event()

        self.app = HTTPProxy(controller_name)
        await self.app.setup()

        self.wrapped_app = self.app
        for middleware in http_middlewares:
            self.wrapped_app = middleware.cls(self.wrapped_app,
                                              **middleware.options)

        # Start running the HTTP server on the event loop.
        # This task should be running forever. We track it in case of failure.
        self.running_task = asyncio.get_event_loop().create_task(self.run())

    async def ready(self):
        """Returns when HTTP proxy is ready to serve traffic.
        Or throw exception when it is not able to serve traffic.
        """
        done_set, _ = await asyncio.wait(
            [
                # Either the HTTP setup has completed.
                # The event is set inside self.run.
                self.setup_complete.wait(),
                # Or self.run errored.
                self.running_task,
            ],
            return_when=asyncio.FIRST_COMPLETED)

        # Return None, or re-throw the exception from self.running_task.
        return await done_set.pop()

    async def run(self):
        sock = socket.socket()
        # These two socket options will allow multiple process to bind the the
        # same port. Kernel will evenly load balance among the port listeners.
        # Note: this will only work on Linux.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind((self.host, self.port))

        # Note(simon): we have to use lower level uvicorn Config and Server
        # class because we want to run the server as a coroutine. The only
        # alternative is to call uvicorn.run which is blocking.
        config = uvicorn.Config(
            self.wrapped_app,
            host=self.host,
            port=self.port,
            lifespan="off",
            access_log=False)
        server = uvicorn.Server(config=config)
        # TODO(edoakes): we need to override install_signal_handlers here
        # because the existing implementation fails if it isn't running in
        # the main thread and uvicorn doesn't expose a way to configure it.
        server.install_signal_handlers = lambda: None

        self.setup_complete.set()
        await server.serve(sockets=[sock])
