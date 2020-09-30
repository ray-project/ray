import asyncio
import socket
from typing import List

import uvicorn

import ray
from ray.exceptions import RayTaskError
from ray.serve.context import TaskContext
from ray.util import metrics
from ray.serve.http_util import Response
from ray.serve.router import Router, RequestMetadata

# The maximum number of times to retry a request due to actor failure.
# TODO(edoakes): this should probably be configurable.
MAX_ACTOR_DEAD_RETRIES = 10


class HTTPProxy:
    """
    This class should be instantiated and ran by ASGI server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(kv_store_actor_handle, router_handle))
    # blocks forever
    """

    def __init__(self, name, controller_name):
        self.name = name
        self.controller_name = controller_name
        self.route_table = {}
        self.routers = {}

    async def fetch_config_from_controller(self):
        assert ray.is_initialized()
        controller = ray.get_actor(self.controller_name)

        await self.set_route_table(await controller.get_router_config.remote())

        self.request_counter = metrics.Count(
            "num_http_requests",
            description="The number of HTTP requests processed",
            tag_keys=("route", ))

    async def drain_router(self, router):
        while router.num_inflight_requests != 0:
            asyncio.sleep(1)

    async def set_route_table(self, route_table):
        new_endpoints = set()
        # Create routers for any new endpoints.
        for path, (endpoint, _) in route_table.items():
            if endpoint not in self.routers:
                self.routers[endpoint] = Router()
                await self.routers[endpoint].setup(endpoint, self.name,
                                                   self.controller_name)
            new_endpoints.add(endpoint)

        # Delete routers for any endpoints that were removed.
        for endpoint in list(self.routers.keys()):
            if endpoint not in new_endpoints:
                router = self.routers.pop(endpoint)
                asyncio.get_event_loop().create_task(self.drain_router(router))
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

        router = self.routers[endpoint_name]
        http_body_bytes = await self.receive_http_body(scope, receive, send)

        headers = {k.decode(): v.decode() for k, v in scope["headers"]}
        request_metadata = RequestMetadata(
            TaskContext.Web,
            http_method=scope["method"].upper(),
            call_method=headers.get("X-SERVE-CALL-METHOD".lower(), "__call__"),
            shard_key=headers.get("X-SERVE-SHARD-KEY".lower(), None),
        )

        result = await router.enqueue_request(request_metadata, scope,
                                              http_body_bytes)

        if isinstance(result, RayTaskError):
            error_message = "Task Error. Traceback: {}.".format(result)
            await error_sender(error_message, 500)
        else:
            await Response(result).send(scope, receive, send)


@ray.remote
class HTTPProxyActor:
    async def __init__(
            self,
            name,
            host,
            port,
            controller_name,
            http_middlewares: List["starlette.middleware.Middleware"] = []):
        self.host = host
        self.port = port

        self.app = HTTPProxy(name, controller_name)
        await self.app.fetch_config_from_controller()

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
        return await self.app.set_route_table(route_table)

    # ------ Proxy router logic ------ #
    async def add_new_worker(self, backend_tag, replica_tag, worker_handle):
        for router in self.app.routers.values():
            await router.add_new_worker(backend_tag, replica_tag,
                                        worker_handle)

    async def set_traffic(self, endpoint, traffic_policy):
        await self.app.routers[endpoint].set_traffic(traffic_policy)

    async def set_backend_config(self, backend, config):
        for router in self.app.routers.values():
            await router.set_backend_config(backend, config)

    async def remove_backend(self, backend):
        for router in self.app.routers.values():
            await router.remove_backend(backend)

    async def remove_worker(self, backend_tag, replica_tag):
        for router in self.app.routers.values():
            await router.remove_worker(backend_tag, replica_tag)

    async def enqueue_request(self, endpoint, request_meta, *request_args,
                              **request_kwargs):
        return await self.app.routers[endpoint].enqueue_request(
            request_meta, *request_args, **request_kwargs)
