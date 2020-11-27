import asyncio
import socket
from typing import List

import uvicorn

import ray
from ray.exceptions import RayTaskError
from ray.serve.context import TaskContext
from ray.util import metrics
from ray.serve.utils import _get_logger, get_random_letters
from ray.serve.http_util import Response
from ray.serve.router import Router, RequestMetadata

# The maximum number of times to retry a request due to actor failure.
# TODO(edoakes): this should probably be configurable.
MAX_ACTOR_DEAD_RETRIES = 10

logger = _get_logger()


class HTTPProxy:
    """
    This class should be instantiated and ran by ASGI server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(kv_store_actor_handle, router_handle))
    # blocks forever
    """

    async def fetch_config_from_controller(self, controller_name):
        assert ray.is_initialized()
        controller = ray.get_actor(controller_name)

        self.route_table = await controller.get_router_config.remote()

        self.request_counter = metrics.Count(
            "num_http_requests",
            description="The number of HTTP requests processed",
            tag_keys=("route", ))

        self.router = Router(controller)
        await self.router.setup_in_async_loop()

    def set_route_table(self, route_table):
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
        # NOTE: This implements ASGI protocol specified in
        #       https://asgi.readthedocs.io/en/latest/specs/index.html

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
        request_metadata = RequestMetadata(
            get_random_letters(10),  # Used for debugging.
            endpoint_name,
            TaskContext.Web,
            http_method=scope["method"].upper(),
            call_method=headers.get("X-SERVE-CALL-METHOD".lower(), "__call__"),
            shard_key=headers.get("X-SERVE-SHARD-KEY".lower(), None),
        )

        ref = await self.router.assign_request(request_metadata, scope,
                                               http_body_bytes)
        result = await ref

        if isinstance(result, RayTaskError):
            error_message = "Task Error. Traceback: {}.".format(result)
            await error_sender(error_message, 500)
        else:
            await Response(result).send(scope, receive, send)


@ray.remote
class HTTPProxyActor:
    async def __init__(
            self,
            host,
            port,
            controller_name,
            http_middlewares: List["starlette.middleware.Middleware"] = []):
        self.host = host
        self.port = port

        self.app = HTTPProxy()
        await self.app.fetch_config_from_controller(controller_name)

        self.wrapped_app = self.app
        for middleware in http_middlewares:
            self.wrapped_app = middleware.cls(self.wrapped_app,
                                              **middleware.options)

        # Start running the HTTP server on the event loop.
        asyncio.get_event_loop().create_task(self.run())

    def ready(self):
        return True

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
        await server.serve(sockets=[sock])

    async def set_route_table(self, route_table):
        self.app.set_route_table(route_table)

    # ------ Proxy router logic ------ #
    async def assign_request(self, request_meta, *request_args,
                             **request_kwargs):
        return await (await self.app.router.assign_request(
            request_meta, *request_args, **request_kwargs))
