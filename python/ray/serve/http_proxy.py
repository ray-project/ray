import socket

import uvicorn

import ray
from ray.serve.context import TaskContext
from ray.serve.request_params import RequestMetadata
from ray.serve.http_util import Response

from urllib.parse import parse_qs


class HTTPProxy:
    """
    This class should be instantiated and ran by ASGI server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(kv_store_actor_handle, router_handle))
    # blocks forever
    """

    def __init__(self):
        assert ray.is_initialized()
        # Must be set via set_route_table.
        self.route_table = dict()
        # Must be set via set_router_handle.
        self.router_handle = None

    def set_route_table(self, route_table):
        self.route_table = route_table

    def set_router_handle(self, router_handle):
        self.router_handle = router_handle

    async def handle_lifespan_message(self, scope, receive, send):
        assert scope["type"] == "lifespan"

        message = await receive()
        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})

    async def receive_http_body(self, scope, receive, send):
        body_buffer = []
        more_body = True
        while more_body:
            message = await receive()
            assert message["type"] == "http.request"

            more_body = message["more_body"]
            body_buffer.append(message["body"])

        return b"".join(body_buffer)

    def _parse_latency_slo(self, scope):
        query_string = scope["query_string"].decode("ascii")
        query_kwargs = parse_qs(query_string)

        relative_slo_ms = query_kwargs.pop("relative_slo_ms", None)
        absolute_slo_ms = query_kwargs.pop("absolute_slo_ms", None)
        relative_slo_ms = self._validate_slo_ms(relative_slo_ms)
        absolute_slo_ms = self._validate_slo_ms(absolute_slo_ms)
        if relative_slo_ms is not None and absolute_slo_ms is not None:
            raise ValueError("Both relative and absolute slo's"
                             "cannot be specified.")
        return relative_slo_ms, absolute_slo_ms

    def _validate_slo_ms(self, request_slo_ms):
        if request_slo_ms is None:
            return None
        if len(request_slo_ms) != 1:
            raise ValueError(
                "Multiple SLO specified, please specific only one.")
        request_slo_ms = request_slo_ms[0]
        request_slo_ms = float(request_slo_ms)
        if request_slo_ms < 0:
            raise ValueError("Request SLO must be positive, it is {}".format(
                request_slo_ms))
        return request_slo_ms

    def _make_error_sender(self, scope, receive, send):
        async def sender(error_message, status_code):
            response = Response(error_message, status_code=status_code)
            await response.send(scope, receive, send)

        return sender

    async def __call__(self, scope, receive, send):
        # NOTE: This implements ASGI protocol specified in
        #       https://asgi.readthedocs.io/en/latest/specs/index.html

        if scope["type"] == "lifespan":
            await self.handle_lifespan_message(scope, receive, send)
            return

        error_sender = self._make_error_sender(scope, receive, send)

        assert self.route_table is not None, (
            "Route table must be set via set_route_table.")
        assert scope["type"] == "http"
        current_path = scope["path"]
        if current_path == "/-/routes":
            await Response(self.route_table).send(scope, receive, send)
            return

        # TODO(simon): Use werkzeug route mapper to support variable path
        if current_path not in self.route_table:
            error_message = (
                "Path {} not found. "
                "Please ping http://.../-/routes for routing table"
            ).format(current_path)
            await error_sender(error_message, 404)
            return

        endpoint_name, methods_allowed = self.route_table[current_path]

        if scope["method"] not in methods_allowed:
            error_message = ("Methods {} not allowed. "
                             "Avaiable HTTP methods are {}.").format(
                                 scope["method"], methods_allowed)
            await error_sender(error_message, 405)
            return

        http_body_bytes = await self.receive_http_body(scope, receive, send)

        # get slo_ms before enqueuing the query
        try:
            relative_slo_ms, absolute_slo_ms = self._parse_latency_slo(scope)
        except ValueError as e:
            await error_sender(str(e), 400)
            return

        headers = {k.decode(): v.decode() for k, v in scope["headers"]}
        request_metadata = RequestMetadata(
            endpoint_name,
            TaskContext.Web,
            relative_slo_ms=relative_slo_ms,
            absolute_slo_ms=absolute_slo_ms,
            call_method=headers.get("X-SERVE-CALL-METHOD".lower(), "__call__"))

        assert self.route_table is not None, (
            "Router handle must be set via set_router_handle.")
        try:
            result = await self.router_handle.enqueue_request.remote(
                request_metadata, scope, http_body_bytes)
            await Response(result).send(scope, receive, send)
        except Exception as e:
            error_message = "Internal Error. Traceback: {}.".format(e)
            await error_sender(error_message, 500)


@ray.remote
class HTTPProxyActor:
    def __init__(self):
        self.app = HTTPProxy()

    async def run(self, host="0.0.0.0", port=8000):
        sock = socket.socket()
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.set_inheritable(True)

        config = uvicorn.Config(self.app, lifespan="on", access_log=False)
        server = uvicorn.Server(config=config)
        # TODO(edoakes): we need to override install_signal_handlers here
        # because the existing implementation fails if it isn't running in
        # the main thread and uvicorn doesn't expose a way to configure it.
        server.install_signal_handlers = lambda: None
        await server.serve(sockets=[sock])

    async def set_route_table(self, route_table):
        self.app.set_route_table(route_table)

    async def set_router_handle(self, router_handle):
        self.app.set_router_handle(router_handle)
