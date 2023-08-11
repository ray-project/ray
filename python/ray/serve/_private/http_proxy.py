import asyncio
from asyncio.tasks import FIRST_COMPLETED
import json
import os
import logging
import pickle
import socket
import time
from typing import Callable, Dict, List, Optional, Tuple, Any
import uuid

import uvicorn
import starlette.responses
import starlette.routing
from starlette.types import Message, Receive, Scope, Send
from starlette.datastructures import MutableHeaders
from starlette.middleware import Middleware

import ray
from ray.exceptions import RayActorError, RayTaskError
from ray.util import metrics
from ray._private.utils import get_or_create_event_loop
from ray._raylet import StreamingObjectRefGenerator

from ray import serve
from ray.serve.handle import RayServeHandle
from ray.serve._private.http_util import (
    ASGIMessageQueue,
    HTTPRequestWrapper,
    RawASGIResponse,
    receive_http_body,
    Response,
    set_socket_reuse_port,
    validate_http_proxy_callback_return,
)
from ray.serve._private.common import (
    ApplicationName,
    EndpointInfo,
    EndpointTag,
    NodeId,
    StreamingHTTPRequest,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
    SERVE_NAMESPACE,
    DEFAULT_LATENCY_BUCKET_MS,
    PROXY_MIN_DRAINING_PERIOD_S,
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    RAY_SERVE_REQUEST_ID_HEADER,
    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_logger,
    configure_component_memory_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.utils import (
    calculate_remaining_timeout,
    call_function_from_import_path,
)
from ray.serve.exceptions import RayServeTimeout


logger = logging.getLogger(SERVE_LOGGER_NAME)

HTTP_REQUEST_MAX_RETRIES = int(os.environ.get("RAY_SERVE_HTTP_REQUEST_MAX_RETRIES", 10))
assert HTTP_REQUEST_MAX_RETRIES >= 0, (
    f"Got unexpected value {HTTP_REQUEST_MAX_RETRIES} for "
    "RAY_SERVE_HTTP_REQUEST_MAX_RETRIES environment variable. "
    "RAY_SERVE_HTTP_REQUEST_MAX_RETRIES cannot be negative."
)

TIMEOUT_ERROR_CODE = "timeout"
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
        "been deprecated. Please set `request_timeout_s` in your Serve config's "
        "`http_options` field instead. `SERVE_REQUEST_PROCESSING_TIMEOUT_S` will be "
        "ignored in future versions. See: https://docs.ray.io/en/releases-2.5.1/serve/a"
        "pi/doc/ray.serve.schema.HTTPOptionsSchema.html#ray.serve.schema.HTTPOptionsSch"
        "ema.request_timeout_s"
    )


INITIAL_BACKOFF_PERIOD_SEC = 0.05
MAX_BACKOFF_PERIOD_SEC = 5
BACKOFF_FACTOR = 2


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
        # Map of application name to is_cross_language.
        self.app_to_is_cross_language: Dict[ApplicationName, bool] = dict()

    def endpoint_exists(self, endpoint: EndpointTag) -> bool:
        return endpoint in self.handles

    def update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        logger.info(
            f"Got updated endpoints: {endpoints}.", extra={"log_to_stderr": False}
        )

        existing_handles = set(self.handles.keys())
        routes = []
        route_info = {}
        app_to_is_cross_language = {}
        for endpoint, info in endpoints.items():
            routes.append(info.route)
            route_info[info.route] = (endpoint, info.app_name)
            app_to_is_cross_language[info.app_name] = info.app_is_cross_language
            if endpoint in self.handles:
                existing_handles.remove(endpoint)
            else:
                self.handles[endpoint] = self._get_handle(endpoint).options(
                    # Streaming codepath isn't supported for Java.
                    stream=(
                        RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING
                        and not info.app_is_cross_language
                    ),
                )

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
        self.app_to_is_cross_language = app_to_is_cross_language

    def match_route(
        self, target_route: str
    ) -> Optional[Tuple[str, RayServeHandle, str, bool]]:
        """Return the longest prefix match among existing routes for the route.

        Args:
            target_route: route to match against.

        Returns:
            (route, handle, app_name, is_cross_language) if found, else None.
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
                    return (
                        route,
                        self.handles[endpoint],
                        app_name,
                        self.app_to_is_cross_language[app_name],
                    )

        return None


class HTTPProxy:
    """This class is meant to be instantiated and run by an ASGI HTTP server.

    >>> import uvicorn
    >>> controller_name = ... # doctest: +SKIP
    >>> uvicorn.run(HTTPProxy(controller_name)) # doctest: +SKIP
    """

    def __init__(
        self,
        controller_name: str,
        node_id: NodeId,
        request_timeout_s: Optional[float] = None,
    ):
        self.request_timeout_s = request_timeout_s
        if self.request_timeout_s is not None and self.request_timeout_s < 0:
            self.request_timeout_s = None

        self._node_id = node_id

        # Set the controller name so that serve will connect to the
        # controller instance this proxy is running in.
        ray.serve.context._set_internal_replica_context(
            None, None, controller_name, None, None
        )

        # Used only for displaying the route table.
        self.route_info: Dict[str, EndpointTag] = dict()

        self.self_actor_handle = ray.get_runtime_context().current_actor
        self.asgi_receive_queues: Dict[str, ASGIMessageQueue] = dict()

        if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
            logger.info(
                "Experimental streaming feature flag enabled.",
                extra={"log_to_stderr": False},
            )

        def get_handle(name):
            return serve.context.get_global_client().get_handle(
                name,
                sync=False,
                missing_ok=True,
                _is_for_http_requests=True,
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
        # `self._prevent_node_downscale_ref` is used to prevent the node from being
        # downscaled when there are ongoing requests
        self._prevent_node_downscale_ref = ray.put("prevent_node_downscale_object")
        # `self._ongoing_requests` is used to count the number of ongoing requests
        self._ongoing_requests = 0
        # The time when the node starts to drain.
        # The node is not draining if it's None.
        self._draining_start_time: Optional[float] = None

    def _is_draining(self) -> bool:
        """Whether is proxy actor is in the draining status or not."""
        return self._draining_start_time is not None

    def _update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        self.route_info: Dict[str, Tuple[EndpointTag, List[str]]] = dict()
        for endpoint, info in endpoints.items():
            route = info.route
            self.route_info[route] = endpoint

        self.prefix_router.update_routes(endpoints)

    def is_drained(self):
        """Check whether the proxy actor is drained or not.

        A proxy actor is drained if it has no ongoing requests
        AND it has been draining for more than
        `PROXY_MIN_DRAINING_PERIOD_S` seconds.
        """
        if not self._is_draining():
            return False

        return (not self._ongoing_requests) and (
            (time.time() - self._draining_start_time) > PROXY_MIN_DRAINING_PERIOD_S
        )

    def update_draining(self, draining: bool):
        """Update the draining status of the http proxy.

        This is called by the http proxy state manager
        to drain or un-drain the proxy actor.
        """

        if draining and (not self._is_draining()):
            logger.info(f"Start to drain the proxy actor on node {self._node_id}.")
            self._draining_start_time = time.time()
        if (not draining) and self._is_draining():
            logger.info(f"Stop draining the proxy actor on node {self._node_id}.")
            self._draining_start_time = None

    async def _not_found(self, scope, receive, send):
        current_path = scope["path"]
        response = Response(
            f"Path '{current_path}' not found. "
            "Please ping http://.../-/routes for route table.",
            status_code=404,
        )
        await response.send(scope, receive, send)

    async def _draining_response(self, scope, receive, send):
        response = Response(
            "This node is being drained.",
            status_code=503,
        )
        await response.send(scope, receive, send)

    async def _timeout_response(self, scope, receive, send, request_id):
        response = Response(
            f"Request {request_id} timed out after {self.request_timeout_s}s.",
            status_code=408,
        )
        await response.send(scope, receive, send)

    async def receive_asgi_messages(self, request_id: str) -> List[Message]:
        queue = self.asgi_receive_queues.get(request_id, None)
        if queue is None:
            raise KeyError(f"Request ID {request_id} not found.")

        await queue.wait_for_message()
        return queue.get_messages_nowait()

    def _ongoing_requests_start(self):
        """Ongoing requests start.

        The current autoscale logic can downscale nodes with ongoing requests if the
        node doesn't have replicas and has no primary copies of objects in the object
        store. The counter and the dummy object reference will help to keep the node
        alive while draining requests, so they are not dropped unintentionally.
        """
        self._ongoing_requests += 1

    def _ongoing_requests_end(self):
        """Ongoing requests end.

        Decrement the ongoing request counter and drop the dummy object reference
        signaling that the node can be downscaled safely.
        """
        self._ongoing_requests -= 1

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """
        assert scope["type"] in {"http", "websocket"}

        method = scope.get("method", "websocket").upper()

        # only use the non-root part of the path for routing
        root_path = scope["root_path"]
        route_path = scope["path"][len(root_path) :]

        if route_path == "/-/routes":
            if self._is_draining():
                return await self._draining_response(scope, receive, send)

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": "",
                    "status_code": "200",
                }
            )
            return await starlette.responses.JSONResponse(self.route_info)(
                scope, receive, send
            )

        if route_path == "/-/healthz":
            if self._is_draining():
                return await self._draining_response(scope, receive, send)

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": "",
                    "status_code": "200",
                }
            )
            return await starlette.responses.PlainTextResponse("success")(
                scope, receive, send
            )

        try:
            self._ongoing_requests_start()

            matched_route = self.prefix_router.match_route(route_path)
            if matched_route is None:
                self.request_error_counter.inc(
                    tags={
                        "route": route_path,
                        "error_code": "404",
                        "method": method,
                    }
                )
                self.request_counter.inc(
                    tags={
                        "route": route_path,
                        "method": method,
                        "application": "",
                        "status_code": "404",
                    }
                )
                return await self._not_found(scope, receive, send)

            route_prefix, handle, app_name, app_is_cross_language = matched_route

            # Modify the path and root path so that reverse lookups and redirection
            # work as expected. We do this here instead of in replicas so it can be
            # changed without restarting the replicas.
            if route_prefix != "/":
                assert not route_prefix.endswith("/")
                scope["path"] = route_path.replace(route_prefix, "", 1)
                scope["root_path"] = root_path + route_prefix

            request_context_info = {
                "route": route_path,
                "app_name": app_name,
            }
            start_time = time.time()
            for key, value in scope.get("headers", []):
                if key.decode() == SERVE_MULTIPLEXED_MODEL_ID:
                    multiplexed_model_id = value.decode()
                    handle = handle.options(multiplexed_model_id=multiplexed_model_id)
                    request_context_info["multiplexed_model_id"] = multiplexed_model_id
                if key.decode() == "x-request-id":
                    request_context_info["request_id"] = value.decode()
                if (
                    key.decode() == RAY_SERVE_REQUEST_ID_HEADER.lower()
                    and "request_id" not in request_context_info
                ):
                    # "x-request-id" has higher priority than "RAY_SERVE_REQUEST_ID".
                    request_context_info["request_id"] = value.decode()
            ray.serve.context._serve_request_context.set(
                ray.serve.context.RequestContext(**request_context_info)
            )

            # Streaming codepath isn't supported for Java.
            if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING and not app_is_cross_language:
                status_code = await self.send_request_to_replica_streaming(
                    request_context_info["request_id"],
                    handle,
                    scope,
                    receive,
                    send,
                )
            else:
                status_code = await self.send_request_to_replica_unary(
                    handle,
                    scope,
                    receive,
                    send,
                )

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
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
                    method=method,
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
                        "method": method,
                    }
                )
                self.deployment_request_error_counter.inc(
                    tags={
                        "deployment": handle.deployment_name,
                        "error_code": status_code,
                        "method": method,
                        "route": route_path,
                        "application": app_name,
                    }
                )
        finally:
            # If anything during the request failed, we still want to ensure the ongoing
            # request counter is decremented and possibly reset the keep alive object.
            self._ongoing_requests_end()

    async def send_request_to_replica_unary(
        self,
        handle: RayServeHandle,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> str:
        http_body_bytes = await receive_http_body(scope, receive, send)

        # NOTE(edoakes): it's important that we defer building the starlette
        # request until it reaches the replica to avoid unnecessary
        # serialization cost, so we use a simple dataclass here.
        request = HTTPRequestWrapper(scope, http_body_bytes)

        # Perform a pickle here to improve latency. Stdlib pickle for simple
        # dataclasses are 10-100x faster than cloudpickle.
        request = pickle.dumps(request)

        retries = 0
        loop = get_or_create_event_loop()
        # We have received all the http request content. The next `receive`
        # call might never arrive; if it does, it can only be `http.disconnect`.
        while retries < HTTP_REQUEST_MAX_RETRIES + 1:
            should_backoff = False
            assignment_task: asyncio.Task = handle.remote(request)
            client_disconnection_task = loop.create_task(receive())
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
            else:
                client_disconnection_task.cancel()

            try:
                object_ref = await assignment_task

                # NOTE (shrekris-anyscale): when the gcs, Serve controller, and
                # some replicas crash simultaneously (e.g. if the head node crashes),
                # requests to the dead replicas hang until the gcs recovers.
                # This asyncio.wait can kill those hanging requests and retry them
                # at another replica. Release tests should kill the head node and
                # check if latency drops significantly. See
                # https://github.com/ray-project/ray/pull/29534 for more info.
                _, request_timed_out = await asyncio.wait(
                    [object_ref], timeout=self.request_timeout_s
                )
                if request_timed_out:
                    logger.info(
                        f"Request didn't finish within {self.request_timeout_s} seconds"
                        ". Retrying with another replica. You can modify this timeout "
                        'by setting "request_timeout_s" in your Serve config\'s '
                        "`http_options` field."
                    )
                    should_backoff = True
                else:
                    result = await object_ref
                    break
            except asyncio.CancelledError:
                # Here because the client disconnected, we will return a custom
                # error code for metric tracking.
                return DISCONNECT_ERROR_CODE
            except RayTaskError as e:
                error_message = f"Unexpected error, traceback: {e}."
                await Response(error_message, status_code=500).send(
                    scope, receive, send
                )
                return "500"
            except RayActorError:
                logger.info(
                    "Request failed due to replica failure. There are "
                    f"{HTTP_REQUEST_MAX_RETRIES - retries} retries "
                    "remaining."
                )
                should_backoff = True

            if should_backoff:
                backoff_period = min(
                    INITIAL_BACKOFF_PERIOD_SEC * pow(2, retries), MAX_BACKOFF_PERIOD_SEC
                )
                retries += 1
                await asyncio.sleep(backoff_period)
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

    async def proxy_asgi_receive(
        self, receive: Receive, queue: ASGIMessageQueue
    ) -> Optional[int]:
        """Proxies the `receive` interface, placing its messages into the queue.

        Once a disconnect message is received, the call exits and `receive` is no longer
        called.

        For HTTP messages, `None` is always returned.
        For websocket messages, the disconnect code is returned if a disconnect code is
        received.
        """
        try:
            while True:
                msg = await receive()
                await queue(msg)

                if msg["type"] == "http.disconnect":
                    return None

                if msg["type"] == "websocket.disconnect":
                    return msg["code"]
        finally:
            queue.close()

    async def _assign_request_with_timeout(
        self,
        handle: RayServeHandle,
        scope: Scope,
        disconnected_task: asyncio.Task,
        timeout_s: Optional[float] = None,
    ) -> Optional[StreamingObjectRefGenerator]:
        """Attempt to send a request on the handle within the timeout.

        If `timeout_s` is exceeded while trying to assign a replica, `TimeoutError`
        will be raised.

        `disconnected_task` is expected to be done if the client disconnects; in this
        case, we will abort assigning a replica and return `None`.
        """
        assignment_task = handle.remote(
            StreamingHTTPRequest(pickle.dumps(scope), self.self_actor_handle)
        )
        done, _ = await asyncio.wait(
            [assignment_task, disconnected_task],
            return_when=FIRST_COMPLETED,
            timeout=timeout_s,
        )
        if assignment_task in done:
            return assignment_task.result()
        elif disconnected_task in done:
            assignment_task.cancel()
            return None
        else:
            assignment_task.cancel()
            raise TimeoutError()

    async def _consume_and_send_asgi_message_generator(
        self,
        obj_ref_generator: StreamingObjectRefGenerator,
        send: Send,
        timeout_s: Optional[float] = None,
    ) -> Optional[str]:
        """Consumes an obj ref generator that yields ASGI messages.

        The messages are sent over the `send` interface.

        If timeout_s is `None`, there's no timeout. If it's not `None`, a timeout error
        will be raised if the full generator isn't consumed within the timeout.

        Returns the status code for HTTP responses.
        """
        status_code = ""
        start = time.time()
        is_first_message = True
        while True:
            try:
                obj_ref = await obj_ref_generator._next_async(
                    timeout_s=calculate_remaining_timeout(
                        timeout_s=timeout_s,
                        start_time_s=start,
                        curr_time_s=time.time(),
                    )
                )
                if obj_ref.is_nil():
                    raise RayServeTimeout(is_first_message=is_first_message)

                asgi_messages: List[Message] = pickle.loads(await obj_ref)
                for asgi_message in asgi_messages:
                    if asgi_message["type"] == "http.response.start":
                        # HTTP responses begin with exactly one
                        # "http.response.start" message containing the "status"
                        # field Other response types (e.g., WebSockets) may not.
                        status_code = str(asgi_message["status"])
                    elif asgi_message["type"] == "websocket.disconnect":
                        status_code = str(asgi_message["code"])

                    await send(asgi_message)
                    is_first_message = False
            except StopAsyncIteration:
                break

        return status_code

    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: RayServeHandle,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> str:
        # Proxy the receive interface by placing the received messages on a queue.
        # The downstream replica must call back into `receive_asgi_messages` on this
        # actor to receive the messages.
        receive_queue = ASGIMessageQueue()
        self.asgi_receive_queues[request_id] = receive_queue
        proxy_asgi_receive_task = get_or_create_event_loop().create_task(
            self.proxy_asgi_receive(receive, receive_queue)
        )

        status_code = ""
        start = time.time()
        try:
            try:
                obj_ref_generator = await self._assign_request_with_timeout(
                    handle,
                    scope,
                    proxy_asgi_receive_task,
                    timeout_s=self.request_timeout_s,
                )
                if obj_ref_generator is None:
                    logger.info(
                        f"Client from {scope['client']} disconnected, cancelling the "
                        "request.",
                        extra={"log_to_stderr": False},
                    )
                    return DISCONNECT_ERROR_CODE
            except TimeoutError:
                logger.warning(
                    f"Request {request_id} timed out after "
                    f"{self.request_timeout_s}s while waiting for assignment."
                )
                await self._timeout_response(scope, receive, send, request_id)
                return TIMEOUT_ERROR_CODE

            try:
                status_code = await self._consume_and_send_asgi_message_generator(
                    obj_ref_generator,
                    send,
                    timeout_s=calculate_remaining_timeout(
                        timeout_s=self.request_timeout_s,
                        start_time_s=start,
                        curr_time_s=time.time(),
                    ),
                )
            except RayServeTimeout as serve_timeout_error:
                logger.warning(
                    f"Request {request_id} timed out after "
                    f"{self.request_timeout_s}s while executing."
                )
                # We should only send timeout response if we have not sent
                # any messages to the client yet. Header (including status code)
                # messages can only be sent once.
                if serve_timeout_error.is_first_message:
                    await self._timeout_response(scope, receive, send, request_id)
                return TIMEOUT_ERROR_CODE

        except Exception as e:
            logger.exception(e)
            status_code = "500"
        finally:
            if not proxy_asgi_receive_task.done():
                proxy_asgi_receive_task.cancel()
            else:
                # If the server disconnects, status_code is set above from the
                # disconnect message. Otherwise the disconnect code comes from
                # a client message via the receive interface.
                if (
                    status_code == ""
                    and scope["type"] == "websocket"
                    and proxy_asgi_receive_task.exception() is None
                ):
                    status_code = str(proxy_asgi_receive_task.result())

            del self.asgi_receive_queues[request_id]

        return status_code


class RequestIdMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):

        headers = MutableHeaders(scope=scope)
        if RAY_SERVE_REQUEST_ID_HEADER not in headers and "x-request-id" not in headers:
            # If X-Request-ID and RAY_SERVE_REQUEST_ID_HEADER are both not set, we
            # generate a new request ID.
            request_id = str(uuid.uuid4())
            headers.append("x-request-id", request_id)
            headers.append(RAY_SERVE_REQUEST_ID_HEADER, request_id)
        elif "x-request-id" in headers:
            request_id = headers["x-request-id"]
        else:
            # TODO(Sihan) Deprecate RAY_SERVE_REQUEST_ID_HEADER
            request_id = headers[RAY_SERVE_REQUEST_ID_HEADER]

        async def send_with_request_id(message: Dict):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                # TODO(Sihan) Deprecate RAY_SERVE_REQUEST_ID_HEADER
                headers.append(RAY_SERVE_REQUEST_ID_HEADER, request_id)
                headers.append("X-Request-ID", request_id)
            if message["type"] == "websocket.accept":
                # TODO(Sihan) Deprecate RAY_SERVE_REQUEST_ID_HEADER
                message[RAY_SERVE_REQUEST_ID_HEADER] = request_id
                message["X-Request-ID"] = request_id
            await send(message)

        await self.app(scope, receive, send_with_request_id)


@ray.remote(num_cpus=0)
class HTTPProxyActor:
    def __init__(
        self,
        host: str,
        port: int,
        root_path: str,
        controller_name: str,
        node_ip_address: str,
        node_id: NodeId,
        request_timeout_s: Optional[float] = None,
        http_middlewares: Optional[List["starlette.middleware.Middleware"]] = None,
    ):  # noqa: F821
        configure_component_logger(
            component_name="http_proxy", component_id=node_ip_address
        )
        logger.info(
            f"Proxy actor {ray.get_runtime_context().get_actor_id()} "
            f"starting on node {node_id}."
        )

        configure_component_memory_profiler(
            component_name="http_proxy", component_id=node_ip_address
        )

        if http_middlewares is None:
            http_middlewares = [Middleware(RequestIdMiddleware)]
        else:
            http_middlewares.append(Middleware(RequestIdMiddleware))

        if RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH:
            logger.info(
                "Calling user-provided callback from import path "
                f" {RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH}."
            )
            middlewares = validate_http_proxy_callback_return(
                call_function_from_import_path(
                    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH
                )
            )

            http_middlewares.extend(middlewares)

        self.host = host
        self.port = port
        self.root_path = root_path

        self.setup_complete = asyncio.Event()

        self.app = HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            request_timeout_s=(
                request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
            ),
        )

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
                    ray.get_runtime_context().get_worker_id(),
                    get_component_logger_file_path(),
                ]
            )

        return await done_set.pop()

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

    async def update_draining(self, draining: bool, _after: Optional[Any] = None):
        """Update the draining status of the http proxy.

        Unused `_after` argument is for scheduling: passing an ObjectRef
        allows delaying this call until after the `_after` call has returned.
        """

        self.app.update_draining(draining)

    async def is_drained(self, _after: Optional[Any] = None):
        """Check whether the proxy is drained or not.

        Unused `_after` argument is for scheduling: passing an ObjectRef
        allows delaying this call until after the `_after` call has returned.
        """

        return self.app.is_drained()

    async def check_health(self):
        """No-op method to check on the health of the HTTP Proxy.
        Make sure the async event loop is not blocked.
        """

        pass

    async def receive_asgi_messages(self, request_id: str) -> bytes:
        """Get ASGI messages for the provided `request_id`.

        After the proxy has stopped receiving messages for this `request_id`,
        this will always return immediately.
        """
        return pickle.dumps(await self.app.receive_asgi_messages(request_id))
