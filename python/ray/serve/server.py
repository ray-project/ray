import asyncio
import json

import uvicorn

import ray
from ray.experimental.async_api import _async_init
from ray.serve.constants import HTTP_ROUTER_CHECKER_INTERVAL_S
from ray.serve.context import TaskContext
from ray.serve.utils import BytesEncoder
from ray.serve.request_params import RequestMetadata

from urllib.parse import parse_qs


class JSONResponse:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> await JSONResponse({"k": "v"})(scope, receive, send)
    """

    def __init__(self, content=None, status_code=200):
        """Construct a JSON HTTP Response.

        Args:
            content (optional): Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self.body = self.render(content)
        self.status_code = status_code
        self.raw_headers = [[b"content-type", b"application/json"]]

    def render(self, content):
        if content is None:
            return b""
        if isinstance(content, bytes):
            return content
        return json.dumps(content, cls=BytesEncoder, indent=2).encode()

    async def __call__(self, scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })
        await send({"type": "http.response.body", "body": self.body})


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
                self.serve_global_state.route_table.list_service())

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

    def _check_slo_ms(self, request_slo_ms):
        if request_slo_ms is not None:
            if len(request_slo_ms) != 1:
                raise ValueError(
                    "Multiple SLO specified, please specific only one.")
            request_slo_ms = request_slo_ms[0]
            request_slo_ms = float(request_slo_ms)
            if request_slo_ms < 0:
                raise ValueError(
                    "Request SLO must be positive, it is {}".format(
                        request_slo_ms))
            return request_slo_ms
        return None

    async def __call__(self, scope, receive, send):
        # NOTE: This implements ASGI protocol specified in
        #       https://asgi.readthedocs.io/en/latest/specs/index.html

        if scope["type"] == "lifespan":
            await self.handle_lifespan_message(scope, receive, send)
            return

        assert scope["type"] == "http"
        current_path = scope["path"]
        if current_path == "/":
            await JSONResponse(self.route_table_cache)(scope, receive, send)
            return

        # TODO(simon): Use werkzeug route mapper to support variable path
        if current_path not in self.route_table_cache:
            error_message = ("Path {} not found. "
                             "Please ping http://.../ for routing table"
                             ).format(current_path)
            await JSONResponse(
                {
                    "error": error_message
                }, status_code=404)(scope, receive, send)
            return

        endpoint_name = self.route_table_cache[current_path]
        http_body_bytes = await self.receive_http_body(scope, receive, send)

        # get slo_ms before enqueuing the query
        query_string = scope["query_string"].decode("ascii")
        query_kwargs = parse_qs(query_string)
        relative_slo_ms = query_kwargs.pop("relative_slo_ms", None)
        absolute_slo_ms = query_kwargs.pop("absolute_slo_ms", None)
        try:
            relative_slo_ms = self._check_slo_ms(relative_slo_ms)
            absolute_slo_ms = self._check_slo_ms(absolute_slo_ms)
            if relative_slo_ms is not None and absolute_slo_ms is not None:
                raise ValueError("Both relative and absolute slo's"
                                 "cannot be specified.")
        except ValueError as e:
            await JSONResponse({"error": str(e)})(scope, receive, send)
            return

        # create objects necessary for enqueue
        # enclosing http_body_bytes to list due to
        # https://github.com/ray-project/ray/issues/6944
        # TODO(alind):  remove list enclosing after issue is fixed
        args = (scope, [http_body_bytes])
        request_in_object = RequestMetadata(
            endpoint_name,
            TaskContext.Web,
            relative_slo_ms=relative_slo_ms,
            absolute_slo_ms=absolute_slo_ms)

        actual_result = await (self.serve_global_state.init_or_get_router()
                               .enqueue_request.remote(request_in_object,
                                                       *args))
        result = actual_result

        if isinstance(result, ray.exceptions.RayTaskError):
            await JSONResponse({
                "error": "internal error, please use python API to debug"
            })(scope, receive, send)
        else:
            await JSONResponse({"result": result})(scope, receive, send)


@ray.remote
class HTTPActor:
    def __init__(self):
        self.app = HTTPProxy()

    def run(self, host="0.0.0.0", port=8000):
        uvicorn.run(
            self.app, host=host, port=port, lifespan="on", access_log=False)
