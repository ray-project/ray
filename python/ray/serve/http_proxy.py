import asyncio
from urllib.parse import parse_qs
import socket

import uvicorn

import ray
from ray.exceptions import RayTaskError
from ray import serve
from ray.serve.context import TaskContext
from ray.serve.metric import MetricClient
from ray.serve.request_params import RequestMetadata
from ray.serve.http_util import Response
from ray.serve.router import Router

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

    async def fetch_config_from_controller(self, instance_name=None):
        assert ray.is_initialized()
        controller = serve.api._get_controller()

        self.route_table = await controller.get_router_config.remote()

        # The exporter is required to return results for /-/metrics endpoint.
        [self.metric_exporter] = await controller.get_metric_exporter.remote()

        self.metric_client = MetricClient(self.metric_exporter)
        self.request_counter = self.metric_client.new_counter(
            "num_http_requests",
            description="The number of requests processed",
            label_names=("route", ))

        self.router = Router()
        await self.router.setup(instance_name)

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

    async def _handle_system_request(self, scope, receive, send):
        current_path = scope["path"]
        if current_path == "/-/routes":
            await Response(self.route_table).send(scope, receive, send)
        elif current_path == "/-/metrics":
            metric_info = await self.metric_exporter.inspect_metrics.remote()
            await Response(metric_info).send(scope, receive, send)
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

        self.request_counter.labels(route=current_path).add()

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
            call_method=headers.get("X-SERVE-CALL-METHOD".lower(), "__call__"),
            shard_key=headers.get("X-SERVE-SHARD-KEY".lower(), None),
        )

        result = await self.router.enqueue_request(request_metadata, scope,
                                                   http_body_bytes)

        if isinstance(result, RayTaskError):
            error_message = "Task Error. Traceback: {}.".format(result)
            await error_sender(error_message, 500)
        else:
            await Response(result).send(scope, receive, send)


@ray.remote
class HTTPProxyActor:
    async def __init__(self, host, port, instance_name=None):
        serve.init(name=instance_name)
        self.app = HTTPProxy()
        await self.app.fetch_config_from_controller(instance_name)
        self.host = host
        self.port = port

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
            self.app,
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
    async def add_new_worker(self, backend_tag, replica_tag, worker_handle):
        return await self.app.router.add_new_worker(backend_tag, replica_tag,
                                                    worker_handle)

    async def set_traffic(self, endpoint, traffic_policy):
        return await self.app.router.set_traffic(endpoint, traffic_policy)

    async def set_backend_config(self, backend, config):
        return await self.app.router.set_backend_config(backend, config)

    async def remove_backend(self, backend):
        return await self.app.router.remove_backend(backend)

    async def remove_endpoint(self, endpoint):
        return await self.app.router.remove_endpoint(endpoint)

    async def remove_worker(self, backend_tag, replica_tag):
        return await self.app.router.remove_worker(backend_tag, replica_tag)

    async def enqueue_request(self, request_meta, *request_args,
                              **request_kwargs):
        return await self.app.router.enqueue_request(
            request_meta, *request_args, **request_kwargs)
