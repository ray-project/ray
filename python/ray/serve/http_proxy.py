import asyncio
import socket
from typing import List, Dict, Tuple

import uvicorn
import starlette.responses
import starlette.routing

import ray
from ray import serve
from ray.exceptions import RayTaskError
from ray.serve.common import EndpointTag
from ray.serve.long_poll import LongPollNamespace
from ray.util import metrics
from ray.serve.utils import logger
from ray.serve.http_util import Response, build_starlette_request
from ray.serve.long_poll import LongPollClient
from ray.serve.handle import DEFAULT


class ServeStarletteEndpoint:
    """Wraps the given Serve endpoint in a Starlette endpoint.

    Implements the ASGI protocol.  Constructs a Starlette endpoint for use by
    a Starlette app or Starlette Router which calls the given Serve endpoint
    using the given Serve client.

    Usage:
        route = starlette.routing.Route(
                "/api",
                ServeStarletteEndpoint(self.client, endpoint_tag),
                methods=methods)
        app = starlette.applications.Starlette(routes=[route])
    """

    def __init__(self, client, endpoint_tag: EndpointTag):
        self.client = client
        self.endpoint_tag = endpoint_tag
        # This will be lazily populated when the first request comes in.
        # TODO(edoakes): we should be able to construct the handle here, but
        # that currently breaks pytest. This seems like a bug.
        self.handle = None

    async def __call__(self, scope, receive, send):
        http_body_bytes = await self.receive_http_body(scope, receive, send)

        headers = {k.decode(): v.decode() for k, v in scope["headers"]}
        if self.handle is None:
            self.handle = serve.get_handle(self.endpoint_tag, sync=False)

        object_ref = await self.handle.options(
            method_name=headers.get("X-SERVE-CALL-METHOD".lower(),
                                    DEFAULT.VALUE),
            shard_key=headers.get("X-SERVE-SHARD-KEY".lower(), DEFAULT.VALUE),
            http_method=scope["method"].upper(),
            http_headers=headers).remote(
                build_starlette_request(scope, http_body_bytes))

        result = await object_ref

        if isinstance(result, RayTaskError):
            error_message = "Task Error. Traceback: {}.".format(result)
            await Response(
                error_message, status_code=500).send(scope, receive, send)
        elif isinstance(result, starlette.responses.Response):
            await result(scope, receive, send)
        else:
            await Response(result).send(scope, receive, send)

    async def receive_http_body(self, scope, receive, send):
        body_buffer = []
        more_body = True
        while more_body:
            message = await receive()
            assert message["type"] == "http.request"

            more_body = message["more_body"]
            body_buffer.append(message["body"])

        return b"".join(body_buffer)


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

        self.router = starlette.routing.Router(default=self._not_found)

        # route -> (endpoint_tag, methods).  Updated via long polling.
        self.route_table: Dict[str, Tuple[EndpointTag, List[str]]] = {}

        self.long_poll_client = LongPollClient(controller, {
            LongPollNamespace.ROUTE_TABLE: self._update_route_table,
        })

        self.request_counter = metrics.Counter(
            "serve_num_http_requests",
            description="The number of HTTP requests processed.",
            tag_keys=("route", ))

    def _update_route_table(self, route_table):
        logger.debug(f"HTTP Proxy: Get updated route table: {route_table}.")
        self.route_table = route_table

        routes = [
            starlette.routing.Route(
                route,
                ServeStarletteEndpoint(self.client, endpoint_tag),
                methods=methods)
            for route, (endpoint_tag, methods) in route_table.items()
            if not self._is_headless(route)
        ]

        routes.append(
            starlette.routing.Route("/-/routes", self._display_route_table))

        self.router.routes = routes

    async def _not_found(self, scope, receive, send):
        current_path = scope["path"]
        error_message = ("Path {} not found. "
                         "Please ping http://.../-/routes for route table."
                         ).format(current_path)
        response = Response(error_message, status_code=404)
        await response.send(scope, receive, send)

    async def _display_route_table(self, request):
        return starlette.responses.JSONResponse(self.route_table)

    def _is_headless(self, route: str):
        """Returns True if `route` corresponds to a headless endpoint."""
        return not route.startswith("/")

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """

        assert self.route_table is not None, (
            "Route table must be set via set_route_table.")
        assert scope["type"] == "http"
        current_path = scope["path"]

        self.request_counter.inc(tags={"route": current_path})

        await self.router(scope, receive, send)


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

        try:
            sock.bind((self.host, self.port))
        except OSError:
            # The OS failed to bind a socket to the given host and port.
            raise ValueError(
                f"""Failed to bind Ray Serve HTTP proxy to '{self.host}:{self.port}'.
Please make sure your http-host and http-port are specified correctly.""")

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
