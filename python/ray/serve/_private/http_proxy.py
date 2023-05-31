import asyncio
from asyncio.tasks import FIRST_COMPLETED
import json
import os
import logging
import pickle
import socket
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from ray._private.utils import get_or_create_event_loop

import uvicorn
import starlette.responses
import starlette.routing
from starlette.types import Receive, Scope, Send

import ray
from ray.exceptions import RayActorError, RayTaskError
from ray.util import metrics

from ray import serve
from ray.serve.handle import RayServeHandle
from ray.serve._private.http_util import (
    HTTPRequestWrapper,
    RawASGIResponse,
    receive_http_body,
    Response,
    set_socket_reuse_port,
)
from ray.serve._private.common import EndpointInfo, EndpointTag, ApplicationName
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
    SERVE_NAMESPACE,
    DEFAULT_LATENCY_BUCKET_MS,
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_logger,
    get_component_logger_file_path,
)

from ray.serve._private.utils import get_random_letters

logger = logging.getLogger(SERVE_LOGGER_NAME)

HTTP_REQUEST_MAX_RETRIES = int(os.environ.get("RAY_SERVE_HTTP_REQUEST_MAX_RETRIES", 10))
assert HTTP_REQUEST_MAX_RETRIES >= 0, (
    f"Got unexpected value {HTTP_REQUEST_MAX_RETRIES} for "
    "RAY_SERVE_HTTP_REQUEST_MAX_RETRIES environment variable. "
    "RAY_SERVE_HTTP_REQUEST_MAX_RETRIES cannot be negative."
)

DISCONNECT_ERROR_CODE = "disconnection"
SOCKET_REUSE_PORT_ENABLED = (
    os.environ.get("SERVE_SOCKET_REUSE_PORT_ENABLED", "1") == "1"
)

# TODO (shrekris-anyscale): Deprecate SERVE_REQUEST_PROCESSING_TIMEOUT_S env var
RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S = (
    float(os.environ.get("RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S", 0))
    or float(os.environ.get("SERVE_REQUEST_PROCESSING_TIMEOUT_S", 0))
    or None
)

if os.environ.get("SERVE_REQUEST_PROCESSING_TIMEOUT_S") is not None:
    logger.warning(
        "The `SERVE_REQUEST_PROCESSING_TIMEOUT_S` environment variable has "
        "been deprecated. Please use `RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S` "
        "instead. `SERVE_REQUEST_PROCESSING_TIMEOUT_S` will be ignored in "
        "future versions."
    )


async def _handle_streaming_response(
    asgi_response_generator: "ray._raylet.StreamingObjectRefGenerator",
    scope: Scope,
    receive: Receive,
    send: Send,
) -> str:
    """Consumes the `asgi_response_generator` and sends its data over `send`.

    This function is a proxy for a downstream ASGI response. The passed
    generator is expected to return a stream of pickled ASGI messages
    (dictionaries) that are sent using the provided ASGI interface.

    Exception handling depends on whether the first message has already been sent:
        - if an exception happens *before* the first message, a 500 status is sent.
        - if an exception happens *after* the first message, the response stream is
          terminated.

    The difference in behavior is because once the first message has been sent, the
    client has already received the status code so we cannot send a `500` (internal
    server error).

    Returns:
        status_code
    """

    status_code = ""
    try:
        async for obj_ref in asgi_response_generator:
            asgi_messages: List[Dict[str, Any]] = pickle.loads(await obj_ref)
            for asgi_message in asgi_messages:
                # There must be exactly one "http.response.start" message that
                # always contains the "status" field.
                if not status_code:
                    assert asgi_message["type"] == "http.response.start", (
                        "First response message must be 'http.response.start'",
                    )
                    assert "status" in asgi_message, (
                        "'http.response.start' message must contain 'status'",
                    )
                    status_code = str(asgi_message["status"])

                await send(asgi_message)
    except Exception as e:
        error_message = f"Unexpected error, traceback: {e}."
        logger.warning(error_message)

        if status_code == "":
            # If first message hasn't been sent, return 500 status.
            await Response(error_message, status_code=500).send(scope, receive, send)
            return "500"
        else:
            # If first message has been sent, terminate the response stream.
            return status_code

    return status_code


async def _send_request_to_handle(handle, scope, receive, send) -> str:
    http_body_bytes = await receive_http_body(scope, receive, send)

    # NOTE(edoakes): it's important that we defer building the starlette
    # request until it reaches the replica to avoid unnecessary
    # serialization cost, so we use a simple dataclass here.
    request = HTTPRequestWrapper(scope, http_body_bytes)
    # Perform a pickle here to improve latency. Stdlib pickle for simple
    # dataclasses are 10-100x faster than cloudpickle.
    request = pickle.dumps(request)

    retries = 0
    backoff_time_s = 0.05
    backoff = False
    loop = get_or_create_event_loop()
    # We have received all the http request conent. The next `receive`
    # call might never arrive; if it does, it can only be `http.disconnect`.
    client_disconnection_task = loop.create_task(receive())
    while retries < HTTP_REQUEST_MAX_RETRIES + 1:
        assignment_task: asyncio.Task = handle.remote(request)
        done, _ = await asyncio.wait(
            [assignment_task, client_disconnection_task],
            return_when=FIRST_COMPLETED,
        )
        if client_disconnection_task in done:
            message = await client_disconnection_task
            assert message["type"] == "http.disconnect", (
                "Received additional request payload that's not disconnect. "
                "This is an invalid HTTP state."
            )
            logger.warning(
                f"Client from {scope['client']} disconnected, cancelling the "
                "request.",
                extra={"log_to_stderr": False},
            )
            # This will make the .result() to raise cancelled error.
            assignment_task.cancel()
        try:
            object_ref = await assignment_task

            if isinstance(object_ref, ray._raylet.StreamingObjectRefGenerator):
                return await _handle_streaming_response(
                    object_ref, scope, receive, send
                )

            # NOTE (shrekris-anyscale): when the gcs, Serve controller, and
            # some replicas crash simultaneously (e.g. if the head node crashes),
            # requests to the dead replicas hang until the gcs recovers.
            # This asyncio.wait can kill those hanging requests and retry them
            # at another replica. Release tests should kill the head node and
            # check if latency drops significantly. See
            # https://github.com/ray-project/ray/pull/29534 for more info.

            _, request_timed_out = await asyncio.wait(
                [object_ref], timeout=RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
            )
            if request_timed_out:
                logger.info(
                    "Request didn't finish within "
                    f"{RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S} seconds. Retrying "
                    "with another replica. You can modify this timeout by "
                    'setting the "RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S" env var.'
                )
                backoff = True
            else:
                result = await object_ref
                client_disconnection_task.cancel()
                break
        except asyncio.CancelledError:
            # Here because the client disconnected, we will return a custom
            # error code for metric tracking.
            return DISCONNECT_ERROR_CODE
        except RayTaskError as e:
            error_message = f"Unexpected error, traceback: {e}."
            await Response(error_message, status_code=500).send(scope, receive, send)
            return "500"
        except RayActorError:
            logger.info(
                "Request failed due to replica failure. There are "
                f"{HTTP_REQUEST_MAX_RETRIES - retries} retries "
                "remaining."
            )
            backoff = True
        if backoff:
            await asyncio.sleep(backoff_time_s)
            # Be careful about the expotential backoff scaling here.
            # Assuming 10 retries, 1.5x scaling means the last retry is 38x the
            # initial backoff time, while 2x scaling means 512x the initial.
            backoff_time_s *= 1.5
            retries += 1
            backoff = False
    else:
        error_message = f"Task failed with {HTTP_REQUEST_MAX_RETRIES} retries."
        await Response(error_message, status_code=500).send(scope, receive, send)
        return "500"

    if isinstance(result, (starlette.responses.Response, RawASGIResponse)):
        await result(scope, receive, send)
        return str(result.status_code)
    else:
        await Response(result).send(scope, receive, send)
        return "200"


class LongestPrefixRouter:
    """Router that performs longest prefix matches on incoming routes."""

    def __init__(self, get_handle: Callable):
        # Function to get a handle given a name. Used to mock for testing.
        self._get_handle = get_handle
        # Routes sorted in order of decreasing length.
        self.sorted_routes: List[str] = list()
        # Endpoints associated with the routes.
        self.route_info: Dict[str, Tuple[EndpointTag, ApplicationName]] = dict()
        # Contains a ServeHandle for each endpoint.
        self.handles: Dict[str, RayServeHandle] = dict()

    def endpoint_exists(self, endpoint: EndpointTag) -> bool:
        return endpoint in self.handles

    def update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        logger.info(
            f"Got updated endpoints: {endpoints}.", extra={"log_to_stderr": False}
        )

        existing_handles = set(self.handles.keys())
        routes = []
        route_info = {}
        for endpoint, info in endpoints.items():
            routes.append(info.route)
            route_info[info.route] = (endpoint, info.app_name)
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                self.handles[endpoint] = self._get_handle(endpoint)

        # Clean up any handles that are no longer used.
        if len(existing_handles) > 0:
            logger.info(
                f"Deleting {len(existing_handles)} unused handles.",
                extra={"log_to_stderr": False},
            )
        for endpoint in existing_handles:
            del self.handles[endpoint]

        # Routes are sorted in order of decreasing length to enable longest
        # prefix matching.
        self.sorted_routes = sorted(routes, key=lambda x: len(x), reverse=True)
        self.route_info = route_info

    def match_route(
        self, target_route: str
    ) -> Tuple[Optional[str], Optional[RayServeHandle]]:
        """Return the longest prefix match among existing routes for the route.

        Args:
            target_route: route to match against.

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
                elif len(target_route) == len(route) or target_route[len(route)] == "/":
                    matched = True

                if matched:
                    endpoint, app_name = self.route_info[route]
                    return route, self.handles[endpoint], app_name

        return None, None, None


class HTTPProxy:
    """This class is meant to be instantiated and run by an ASGI HTTP server.

    >>> import uvicorn
    >>> controller_name = ... # doctest: +SKIP
    >>> uvicorn.run(HTTPProxy(controller_name)) # doctest: +SKIP
    """

    def __init__(self, controller_name: str):
        # Set the controller name so that serve will connect to the
        # controller instance this proxy is running in.
        ray.serve.context._set_internal_replica_context(
            None, None, controller_name, None, None
        )

        # Used only for displaying the route table.
        self.route_info: Dict[str, EndpointTag] = dict()

        def get_handle(name):
            return serve.context.get_global_client().get_handle(
                name,
                sync=False,
                missing_ok=True,
                _internal_pickled_http_request=True,
                _stream=RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
            )

        self.prefix_router = LongestPrefixRouter(get_handle)
        self.long_poll_client = LongPollClient(
            ray.get_actor(controller_name, namespace=SERVE_NAMESPACE),
            {
                LongPollNamespace.ROUTE_TABLE: self._update_routes,
            },
            call_in_event_loop=get_or_create_event_loop(),
        )
        self.request_counter = metrics.Counter(
            "serve_num_http_requests",
            description="The number of HTTP requests processed.",
            tag_keys=("route", "method", "application", "status_code"),
        )

        self.request_error_counter = metrics.Counter(
            "serve_num_http_error_requests",
            description="The number of non-200 HTTP responses.",
            tag_keys=(
                "route",
                "error_code",
                "method",
            ),
        )

        self.deployment_request_error_counter = metrics.Counter(
            "serve_num_deployment_http_error_requests",
            description=(
                "The number of non-200 HTTP responses returned by each deployment."
            ),
            tag_keys=(
                "deployment",
                "error_code",
                "method",
                "route",
                "application",
            ),
        )
        self.processing_latency_tracker = metrics.Histogram(
            "serve_http_request_latency_ms",
            description=(
                "The end-to-end latency of HTTP requests "
                "(measured from the Serve HTTP proxy)."
            ),
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=(
                "route",
                "application",
                "status_code",
            ),
        )

    def _update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        self.route_info: Dict[str, Tuple[EndpointTag, List[str]]] = dict()
        for endpoint, info in endpoints.items():
            route = info.route
            self.route_info[route] = endpoint

        self.prefix_router.update_routes(endpoints)

    async def block_until_endpoint_exists(
        self, endpoint: EndpointTag, timeout_s: float
    ):
        start = time.time()
        while True:
            if time.time() - start > timeout_s:
                raise TimeoutError(f"Waited {timeout_s} for {endpoint} to propagate.")
            for existing_endpoint in self.route_info.values():
                if existing_endpoint == endpoint:
                    return
            await asyncio.sleep(0.2)

    async def _not_found(self, scope, receive, send):
        current_path = scope["path"]
        response = Response(
            f"Path '{current_path}' not found. "
            "Please ping http://.../-/routes for route table.",
            status_code=404,
        )
        await response.send(scope, receive, send)

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """

        assert scope["type"] == "http"

        # only use the non-root part of the path for routing
        root_path = scope["root_path"]
        route_path = scope["path"][len(root_path) :]

        if route_path == "/-/routes":
            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": scope["method"].upper(),
                    "application": "",
                    "status_code": "200",
                }
            )
            return await starlette.responses.JSONResponse(self.route_info)(
                scope, receive, send
            )

        if route_path == "/-/healthz":
            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": scope["method"].upper(),
                    "application": "",
                    "status_code": "200",
                }
            )
            return await starlette.responses.PlainTextResponse("success")(
                scope, receive, send
            )

        route_prefix, handle, app_name = self.prefix_router.match_route(route_path)
        if route_prefix is None:
            self.request_error_counter.inc(
                tags={
                    "route": route_path,
                    "error_code": "404",
                    "method": scope["method"].upper(),
                }
            )
            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": scope["method"].upper(),
                    "application": "",
                    "status_code": "404",
                }
            )
            return await self._not_found(scope, receive, send)

        # Modify the path and root path so that reverse lookups and redirection
        # work as expected. We do this here instead of in replicas so it can be
        # changed without restarting the replicas.
        if route_prefix != "/":
            assert not route_prefix.endswith("/")
            scope["path"] = route_path.replace(route_prefix, "", 1)
            scope["root_path"] = root_path + route_prefix

        request_context_info = {
            "route": route_path,
            "request_id": get_random_letters(10),
            "app_name": app_name,
        }
        start_time = time.time()
        for key, value in scope["headers"]:
            if key.decode() == SERVE_MULTIPLEXED_MODEL_ID:
                request_context_info["multiplexed_model_id"] = value.decode()
                break
        ray.serve.context._serve_request_context.set(
            ray.serve.context.RequestContext(**request_context_info)
        )
        status_code = await _send_request_to_handle(handle, scope, receive, send)
        self.request_counter.inc(
            tags={
                "route": route_path,
                "method": scope["method"].upper(),
                "application": app_name,
                "status_code": status_code,
            }
        )

        latency_ms = (time.time() - start_time) * 1000.0
        self.processing_latency_tracker.observe(
            latency_ms,
            tags={
                "route": route_path,
                "application": app_name,
                "status_code": status_code,
            },
        )
        logger.info(
            access_log_msg(
                method=scope["method"],
                status=str(status_code),
                latency_ms=latency_ms,
            ),
            extra={"log_to_stderr": False},
        )
        if status_code != "200":
            self.request_error_counter.inc(
                tags={
                    "route": route_path,
                    "error_code": status_code,
                    "method": scope["method"].upper(),
                }
            )
            self.deployment_request_error_counter.inc(
                tags={
                    "deployment": handle.deployment_name,
                    "error_code": status_code,
                    "method": scope["method"].upper(),
                    "route": route_path,
                    "application": app_name,
                }
            )


@ray.remote(num_cpus=0)
class HTTPProxyActor:
    def __init__(
        self,
        host: str,
        port: int,
        root_path: str,
        controller_name: str,
        node_ip_address: str,
        http_middlewares: Optional[List["starlette.middleware.Middleware"]] = None,
    ):  # noqa: F821
        configure_component_logger(
            component_name="http_proxy", component_id=node_ip_address
        )

        if http_middlewares is None:
            http_middlewares = []

        self.host = host
        self.port = port
        self.root_path = root_path

        self.setup_complete = asyncio.Event()

        self.app = HTTPProxy(controller_name)

        self.wrapped_app = self.app
        for middleware in http_middlewares:
            self.wrapped_app = middleware.cls(self.wrapped_app, **middleware.options)

        # Start running the HTTP server on the event loop.
        # This task should be running forever. We track it in case of failure.
        self.running_task = get_or_create_event_loop().create_task(self.run())

    async def ready(self):
        """Returns when HTTP proxy is ready to serve traffic.
        Or throw exception when it is not able to serve traffic.
        """
        setup_task = get_or_create_event_loop().create_task(self.setup_complete.wait())
        done_set, _ = await asyncio.wait(
            [
                # Either the HTTP setup has completed.
                # The event is set inside self.run.
                setup_task,
                # Or self.run errored.
                self.running_task,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Return metadata, or re-throw the exception from self.running_task.
        if self.setup_complete.is_set():
            # NOTE(zcin): We need to convert the metadata to a json string because
            # of cross-language scenarios. Java can't deserialize a Python tuple.
            return json.dumps(
                [
                    ray._private.worker.global_worker.worker_id.hex(),
                    get_component_logger_file_path(),
                ]
            )

        return await done_set.pop()

    async def block_until_endpoint_exists(
        self, endpoint: EndpointTag, timeout_s: float
    ):
        await self.app.block_until_endpoint_exists(endpoint, timeout_s)

    async def run(self):
        sock = socket.socket()
        if SOCKET_REUSE_PORT_ENABLED:
            set_socket_reuse_port(sock)
        try:
            sock.bind((self.host, self.port))
        except OSError:
            # The OS failed to bind a socket to the given host and port.
            raise ValueError(
                f"""Failed to bind Ray Serve HTTP proxy to '{self.host}:{self.port}'.
Please make sure your http-host and http-port are specified correctly."""
            )

        # Note(simon): we have to use lower level uvicorn Config and Server
        # class because we want to run the server as a coroutine. The only
        # alternative is to call uvicorn.run which is blocking.
        config = uvicorn.Config(
            self.wrapped_app,
            host=self.host,
            port=self.port,
            root_path=self.root_path,
            lifespan="off",
            access_log=False,
        )
        server = uvicorn.Server(config=config)
        # TODO(edoakes): we need to override install_signal_handlers here
        # because the existing implementation fails if it isn't running in
        # the main thread and uvicorn doesn't expose a way to configure it.
        server.install_signal_handlers = lambda: None

        self.setup_complete.set()
        await server.serve(sockets=[sock])

    async def check_health(self):
        """No-op method to check on the health of the HTTP Proxy.
        Make sure the async event loop is not blocked.
        """
        pass
