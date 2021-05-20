import asyncio
import socket
import time
import pickle
from typing import List, Dict, Optional, Tuple

import uvicorn
import starlette.responses
import starlette.routing

import ray
from ray import serve
from ray.exceptions import RayActorError, RayTaskError
from ray.serve.common import EndpointInfo, EndpointTag
from ray.serve.long_poll import LongPollNamespace
from ray.util import metrics
from ray.serve.utils import logger
from ray.serve.handle import RayServeHandle
from ray.serve.http_util import HTTPRequestWrapper, receive_http_body, Response
from ray.serve.long_poll import LongPollClient
from ray.serve.handle import DEFAULT

MAX_REPLICA_FAILURE_RETRIES = 10


async def _send_request_to_handle(handle, scope, receive, send):
    http_body_bytes = await receive_http_body(scope, receive, send)

    headers = {k.decode(): v.decode() for k, v in scope["headers"]}
    handle = handle.options(
        method_name=headers.get("X-SERVE-CALL-METHOD".lower(), DEFAULT.VALUE),
        shard_key=headers.get("X-SERVE-SHARD-KEY".lower(), DEFAULT.VALUE),
        http_method=scope["method"].upper(),
        http_headers=headers,
    )

    # scope["router"] and scope["endpoint"] contain references to a router
    # and endpoint object, respectively, which each in turn contain a
    # reference to the Serve client, which cannot be serialized.
    # The solution is to delete these from scope, as they will not be used.
    # TODO(edoakes): this can be removed once we deprecate the old API.
    if "router" in scope:
        del scope["router"]
    if "endpoint" in scope:
        del scope["endpoint"]

    # NOTE(edoakes): it's important that we defer building the starlette
    # request until it reaches the backend replica to avoid unnecessary
    # serialization cost, so we use a simple dataclass here.
    request = HTTPRequestWrapper(scope, http_body_bytes)
    # Perform a pickle here to improve latency. Stdlib pickle for simple
    # dataclasses are 10-100x faster than cloudpickle.
    request = pickle.dumps(request)

    retries = 0
    backoff_time_s = 0.05
    while retries < MAX_REPLICA_FAILURE_RETRIES:
        object_ref = await handle.remote(request)
        try:
            result = await object_ref
            break
        except RayActorError:
            logger.warning("Request failed due to replica failure. There are "
                           f"{MAX_REPLICA_FAILURE_RETRIES - retries} retries "
                           "remaining.")
            await asyncio.sleep(backoff_time_s)
            backoff_time_s *= 2
            retries += 1

    if isinstance(result, RayTaskError):
        error_message = "Task Error. Traceback: {}.".format(result)
        await Response(
            error_message, status_code=500).send(scope, receive, send)
    elif isinstance(result, starlette.responses.Response):
        await result(scope, receive, send)
    else:
        await Response(result).send(scope, receive, send)


class ServeStarletteEndpoint:
    """Wraps the given Serve endpoint in a Starlette endpoint.

    Implements the ASGI protocol.  Constructs a Starlette endpoint for use by
    a Starlette app or Starlette Router which calls the given Serve endpoint.
    Usage:
        route = starlette.routing.Route(
                "/api",
                ServeStarletteEndpoint(endpoint_tag),
                methods=methods)
        app = starlette.applications.Starlette(routes=[route])
    """

    def __init__(self, endpoint_tag: EndpointTag, path_prefix: str):
        self.endpoint_tag = endpoint_tag
        self.path_prefix = path_prefix
        self.handle = serve.get_handle(
            self.endpoint_tag,
            sync=False,
            missing_ok=True,
            _internal_pickled_http_request=True,
        )

    async def __call__(self, scope, receive, send):
        # Modify the path and root path so that reverse lookups and redirection
        # work as expected. We do this here instead of in replicas so it can be
        # changed without restarting the replicas.
        scope["path"] = scope["path"].replace(self.path_prefix, "", 1)
        scope["root_path"] = self.path_prefix

        await _send_request_to_handle(self.handle, scope, receive, send)


class LongestPrefixRouter:
    """Router that performs longest prefix matches on incoming routes."""

    def __init__(self):
        # Routes sorted in order of decreasing length.
        self.sorted_routes: List[str] = list()
        # Endpoints and methods associated with the routes.
        self.route_info: Dict[str, Tuple[EndpointTag, List[str]]] = dict()
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[str, RayServeHandle] = dict()

    def endpoint_exists(self, endpoint: EndpointTag) -> bool:
        return endpoint in self.handles

    def update_routes(self,
                      endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        logger.debug(f"Got updated endpoints: {endpoints}.")

        existing_handles = set(self.handles.keys())
        routes = []
        route_info = {}
        for endpoint, info in endpoints.items():
            # Default case where the user did not specify a route prefix.
            if info.route is None:
                route = f"/{endpoint}"
            else:
                route = info.route

            routes.append(route)
            route_info[route] = (endpoint, info.http_methods)
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                self.handles[endpoint] = serve.get_handle(
                    endpoint,
                    sync=False,
                    missing_ok=True,
                    _internal_pickled_http_request=True,
                )

        # Clean up any handles that are no longer used.
        for endpoint in existing_handles:
            del self.handles[endpoint]

        # Routes are sorted in order of decreasing length to enable longest
        # prefix matching.
        self.sorted_routes = sorted(routes, key=lambda x: len(x), reverse=True)
        self.route_info = route_info

    def match_route(self, target_route: str, target_method: str
                    ) -> Tuple[Optional[str], Optional[RayServeHandle]]:
        """Return the longest prefix match among existing routes for the route.

        Args:
            target_route (str): route to match against.
            target_method (str): method to match against.

        Returns:
            (matched_route (str), serve_handle (RayServeHandle)) if found,
            else (None, None).
        """

        for route in self.sorted_routes:
            if target_route.startswith(route):
                matched = False
                # If the route we matched on ends in a '/', then so does the
                # target route and this must be a match.
                if route.endswith("/"):
                    matched = True
                # If the route we matched on doesn't end in a '/', we need to
                # do another check to ensure that either this is an exact match
                # or the next character in the target route is a '/'. This is
                # to guard against the scenario where we have '/route' as a
                # prefix and there's a request to '/routesuffix'. In this case,
                # it should *not* be a match.
                elif (len(target_route) == len(route)
                      or target_route[len(route)] == "/"):
                    matched = True

                if matched:
                    endpoint, methods = self.route_info[route]
                    if target_method in methods:
                        return route, self.handles[endpoint]

        return None, None


class HTTPProxy:
    """This class is meant to be instantiated and run by an ASGI HTTP server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(controller_name))
    """

    def __init__(self, controller_name: str):
        # Set the controller name so that serve will connect to the
        # controller instance this proxy is running in.
        ray.serve.api._set_internal_replica_context(None, None,
                                                    controller_name, None)

        # Used only for displaying the route table.
        self.route_info: Dict[str, Tuple[EndpointTag, List[str]]] = dict()

        # NOTE(edoakes): we currently have both a Starlette router and a
        # longest-prefix router to maintain compatibility with the old API.
        # We first match on the Starlette router (which contains routes using
        # the old API) and then fall back to the prefix router. The Starlette
        # router can be removed once we deprecate the old API.
        self.starlette_router = starlette.routing.Router(
            default=self._fallback_to_prefix_router)
        self.prefix_router = LongestPrefixRouter()
        self.long_poll_client = LongPollClient(
            ray.get_actor(controller_name), {
                LongPollNamespace.ROUTE_TABLE: self._update_routes,
            },
            call_in_event_loop=asyncio.get_event_loop())
        self.request_counter = metrics.Counter(
            "serve_num_http_requests",
            description="The number of HTTP requests processed.",
            tag_keys=("route", ))

    def _split_routes(
            self, endpoints: Dict[EndpointTag, EndpointInfo]) -> Tuple[Dict[
                EndpointTag, EndpointInfo], Dict[EndpointTag, EndpointInfo]]:
        starlette_routes = {}
        prefix_routes = {}
        for endpoint, info in endpoints.items():
            if info.legacy:
                starlette_routes[endpoint] = info
            else:
                prefix_routes[endpoint] = info

        return starlette_routes, prefix_routes

    def _update_routes(self,
                       endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        self.route_info: Dict[str, Tuple[EndpointTag, List[str]]] = dict()
        for endpoint, info in endpoints.items():
            if not info.legacy and info.route is None:
                route = f"/{endpoint}"
            else:
                route = info.route
            self.route_info[route] = (endpoint, info.http_methods)

        starlette_routes, prefix_routes = self._split_routes(endpoints)
        self.starlette_router.routes = [
            starlette.routing.Route(
                info.route,
                ServeStarletteEndpoint(endpoint, info.route),
                methods=info.http_methods)
            for endpoint, info in starlette_routes.items()
            if info.route is not None
        ]

        self.prefix_router.update_routes(prefix_routes)

    async def block_until_endpoint_exists(self, endpoint: EndpointTag,
                                          timeout_s: float):
        start = time.time()
        while True:
            if time.time() - start > timeout_s:
                raise TimeoutError(
                    f"Waited {timeout_s} for {endpoint} to propagate.")
            for existing_endpoint, _ in self.route_info.values():
                if existing_endpoint == endpoint:
                    return
            await asyncio.sleep(0.2)

    async def _not_found(self, scope, receive, send):
        current_path = scope["path"]
        response = Response(
            f"Path '{current_path}' not found. "
            "Please ping http://.../-/routes for route table.",
            status_code=404)
        await response.send(scope, receive, send)

    async def _fallback_to_prefix_router(self, scope, receive, send):
        route_prefix, handle = self.prefix_router.match_route(
            scope["path"], scope["method"])
        if route_prefix is None:
            return await self._not_found(scope, receive, send)

        # Modify the path and root path so that reverse lookups and redirection
        # work as expected. We do this here instead of in replicas so it can be
        # changed without restarting the replicas.
        if route_prefix != "/":
            assert not route_prefix.endswith("/")
            scope["path"] = scope["path"].replace(route_prefix, "", 1)
            scope["root_path"] = route_prefix

        await _send_request_to_handle(handle, scope, receive, send)

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """

        assert scope["type"] == "http"
        self.request_counter.inc(tags={"route": scope["path"]})

        if scope["path"] == "/-/routes":
            return await starlette.responses.JSONResponse(self.route_info)(
                scope, receive, send)

        await self.starlette_router(scope, receive, send)


@ray.remote(num_cpus=0)
class HTTPProxyActor:
    def __init__(self,
                 host: str,
                 port: int,
                 controller_name: str,
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

    async def block_until_endpoint_exists(self, endpoint: EndpointTag,
                                          timeout_s: float):
        await self.app.block_until_endpoint_exists(endpoint, timeout_s)

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
