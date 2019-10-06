import asyncio
import json

import uvicorn

import ray
from ray.experimental.async_api import _async_init, as_future
from ray.experimental.serve.utils import BytesEncoder
from ray.experimental.serve.constants import HTTP_ROUTER_CHECKER_INTERVAL_S
from ray.experimental.serve.context import TaskContext


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

    def __init__(self, kv_store_actor_handle, router_handle):
        """
        Args:
            kv_store_actor_handle (ray.actor.ActorHandle): handle to routing
               table actor. It will be used to populate routing table. It
               should implement `handle.list_service()`
            router_handle (ray.actor.ActorHandle): actor handle to push request
               to. It should implement
               `handle.enqueue_request.remote(endpoint, body)`
        """
        assert ray.is_initialized()

        self.admin_actor = kv_store_actor_handle
        self.router = router_handle
        self.route_table = dict()

        self.route_checker_should_shutdown = False

    async def route_checker(self, interval):
        while True:
            if self.route_checker_should_shutdown:
                return

            try:
                self.route_table = await as_future(
                    self.admin_actor.list_service.remote())
            except ray.exceptions.RayletError:  # Gracefully handle termination
                return

            await asyncio.sleep(interval)

    async def handle_lifespan_message(self, scope, receive, send):
        assert scope["type"] == "lifespan"

        message = await receive()
        if message["type"] == "lifespan.startup":
            await _async_init()
            asyncio.ensure_future(
                self.route_checker(interval=HTTP_ROUTER_CHECKER_INTERVAL_S))
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
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

    async def __call__(self, scope, receive, send):
        # NOTE: This implements ASGI protocol specified in
        #       https://asgi.readthedocs.io/en/latest/specs/index.html

        if scope["type"] == "lifespan":
            await self.handle_lifespan_message(scope, receive, send)
            return

        assert scope["type"] == "http"
        current_path = scope["path"]
        if current_path == "/":
            await JSONResponse(self.route_table)(scope, receive, send)
            return

        if current_path not in self.route_table:
            error_message = ("Path {} not found. "
                             "Please ping http://.../ for routing table"
                             ).format(current_path)
            await JSONResponse(
                {
                    "error": error_message
                }, status_code=404)(scope, receive, send)
            return

        endpoint_name = self.route_table[current_path]
        http_body_bytes = await self.receive_http_body(scope, receive, send)

        result_object_id_bytes = await as_future(
            self.router.enqueue_request.remote(
                service=endpoint_name,
                request_args=(scope, http_body_bytes),
                request_kwargs=dict(),
                request_context=TaskContext.Web))

        result = await as_future(ray.ObjectID(result_object_id_bytes))

        if isinstance(result, ray.exceptions.RayTaskError):
            await JSONResponse({
                "error": "internal error, please use python API to debug"
            })(scope, receive, send)
        else:
            await JSONResponse({"result": result})(scope, receive, send)


@ray.remote
class HTTPActor:
    def __init__(self, kv_store_actor_handle, router_handle):
        self.app = HTTPProxy(kv_store_actor_handle, router_handle)

    def run(self, host="0.0.0.0", port=8000):
        uvicorn.run(
            self.app, host=host, port=port, lifespan="on", access_log=False)
