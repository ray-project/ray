import asyncio

import uvicorn

import ray
from ray.experimental.async_api import _async_init
from ray.serve.constants import HTTP_ROUTER_CHECKER_INTERVAL_S
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

        # Delay import due to GlobalState depends on HTTP actor
        from ray.serve.global_state import GlobalState

        self.serve_global_state = GlobalState()
        self.route_table_cache = dict()

        self.route_checker_task = None
        self.route_checker_should_shutdown = False

    async def route_checker(self, interval):
        while True:
            if self.route_checker_should_shutdown:
                return

            self.route_table_cache = (
                self.serve_global_state.route_table.list_service(
                    include_methods=True, include_headless=False))

            await asyncio.sleep(interval)

    async def handle_lifespan_message(self, scope, receive, send):
        assert scope["type"] == "lifespan"

        message = await receive()
        if message["type"] == "lifespan.startup":
            await _async_init()
            self.route_checker_task = asyncio.get_event_loop().create_task(
                self.route_checker(interval=HTTP_ROUTER_CHECKER_INTERVAL_S))
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            self.route_checker_task.cancel()
            self.route_checker_should_shutdown = True
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

        assert scope["type"] == "http"
        current_path = scope["path"]
        if current_path == "/-/routes":
            await Response(self.route_table_cache).send(scope, receive, send)
            return

        # TODO(simon): Use werkzeug route mapper to support variable path
        if current_path not in self.route_table_cache:
            error_message = (
                "Path {} not found. "
                "Please ping http://.../-/routes for routing table"
            ).format(current_path)
            await error_sender(error_message, 404)
            return

        endpoint_name, methods_allowed = self.route_table_cache[current_path]

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

        try:
            result = await (self.serve_global_state.init_or_get_router()
                            .enqueue_request.remote(request_metadata, scope,
                                                    http_body_bytes))
            await Response(result).send(scope, receive, send)
        except Exception as e:
            error_message = "Internal Error. Traceback: {}.".format(e)
            await error_sender(error_message, 500)


@ray.remote
class HTTPActor:
    def __init__(self):
        self.app = HTTPProxy()

    def run(self, host="0.0.0.0", port=8000):
        uvicorn.run(
            self.app, host=host, port=port, lifespan="on", access_log=False)
