import asyncio
from asyncio.tasks import FIRST_COMPLETED
import json
import os
import logging
import pickle
import socket
import time
from typing import Callable, Dict, List, Generator, Optional, Set, Tuple
import grpc
from ray.serve.generated import serve_pb2, serve_pb2_grpc

import uvicorn
import starlette.responses
import starlette.routing
from starlette.types import Message, Receive, Send
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
    GRPCRequest,
    StreamingHTTPRequest,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
    SERVE_NAMESPACE,
    DEFAULT_LATENCY_BUCKET_MS,
    DEFAULT_GRPC_PORT,
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    RAY_SERVE_REQUEST_ID_HEADER,
    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_logger,
    get_component_logger_file_path,
)

from ray.serve._private.serve_request_response import (
    ASGIServeRequest,
    GRPCServeRequest,
    ServeRequest,
    ServeResponse,
)
from ray.serve._private.utils import (
    calculate_remaining_timeout,
    call_function_from_import_path,
    get_random_letters,
)


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

    def match_target(self, target: str) -> Optional[Tuple[str, RayServeHandle]]:
        """Return the route and handle from the target.

        Args:
            target: the target to match against endpoint.

        Returns:
            (route, handle) if found, else None.
        """
        for route, endpoint_and_app_name in self.route_info.items():
            endpoint, app_name = endpoint_and_app_name
            if target.endswith(endpoint):
                return route, self.handles[endpoint]

        return None


class GenericProxy:
    """This class is  served as the base class for different types of proxies.

    It contains all the common setup and methods required for running a proxy.
    At minimum, the particular proxy class need to implement
    `setup_request_context_and_handle()` and `send_request_to_replica_streaming()`.
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
                LongPollNamespace.ACTIVE_NODES: self._update_draining,
            },
            call_in_event_loop=get_or_create_event_loop(),
        )
        self.request_counter = metrics.Counter(
            f"serve_num_{self.proxy_name.lower()}_requests",
            description=f"The number of {self.proxy_name} requests processed.",
            tag_keys=("route", "method", "application", "status_code"),
        )

        self.request_error_counter = metrics.Counter(
            f"serve_num_{self.proxy_name.lower()}_error_requests",
            description=f"The number of non-200 {self.proxy_name} responses.",
            tag_keys=(
                "route",
                "error_code",
                "method",
            ),
        )

        self.deployment_request_error_counter = metrics.Counter(
            f"serve_num_deployment_{self.proxy_name.lower()}_error_requests",
            description=(
                f"The number of non-200 {self.proxy_name} responses returned by "
                "each deployment."
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
            f"serve_{self.proxy_name.lower()}_request_latency_ms",
            description=(
                f"The end-to-end latency of {self.proxy_name} requests "
                f"(measured from the Serve {self.proxy_name} proxy)."
            ),
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=(
                "route",
                "application",
                "status_code",
            ),
        )
        # `self._ongoing_requests` is used to count the number of ongoing requests
        # and determine whether to set/unset `self._prevent_node_downscale_ref`
        self._ongoing_requests = 0
        # `self._prevent_node_downscale_ref` is used to prevent the node from being
        # downscaled when there are ongoing requests
        self._prevent_node_downscale_ref = None
        # `self._draining` is used to indicate whether the node is the draining state.
        self._draining = False

    def _update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        self.route_info: Dict[str, Tuple[EndpointTag, List[str]]] = dict()
        for endpoint, info in endpoints.items():
            route = info.route
            self.route_info[route] = endpoint

        self.prefix_router.update_routes(endpoints)

    def _update_draining(self, active_nodes: Set[str]):
        """Update draining flag on http proxy.

        This is a callback for when controller detects there being a change in active
        nodes. Each http proxy will check if it's nodes is still active and set
        draining flag accordingly. Also, log a message when the draining flag is
        changed.
        """
        draining = self._node_id not in active_nodes
        if draining != self._draining:
            logger.info(f"Setting draining flag on node {self._node_id} to {draining}.")
            self._draining = draining

            # Since the draining flag is changed, we need to check if
            # `self._prevent_node_downscale_ref` is set to prevent the node from being
            # downscaled when there are ongoing requests.
            self._try_set_prevent_downscale_ref()

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

    async def _not_found(self, serve_request: ServeRequest):
        current_path = serve_request.path
        response = Response(
            f"Path '{current_path}' not found. "
            "Please ping http://.../-/routes for route table.",
            status_code=404,
        )
        await response.send(
            serve_request.scope, serve_request.receive, serve_request.send
        )

    async def _draining_response(self, serve_request: ServeRequest):
        response = Response(
            "This node is being drained.",
            status_code=503,
        )
        await response.send(
            serve_request.scope, serve_request.receive, serve_request.send
        )

    def _try_set_prevent_downscale_ref(self):
        """Try to set put a primary copy of object in the object store to prevent node
        from downscale.

        The only time we need to put the object store is when there are ongoing
        requests, the node is not draining, and the object reference is not set yet.
        This should be checked when either `self._ongoing_requests` or `self._draining`
        is changed. Also, log when the object reference is set.
        """
        if (
            self._ongoing_requests > 0
            and self._draining
            and self._prevent_node_downscale_ref is None
        ):
            logger.info("Putting keep alive object reference to prevent downscaling.")
            self._prevent_node_downscale_ref = ray.put("ongoing_requests")

    def _ongoing_requests_start(self):
        """Ongoing requests start.

        The current autoscale logic can downscale nodes with ongoing requests if the
        node doesn't have replicas and has no primary copies of objects in the object
        store. The counter and the dummy object reference will help to keep the node
        alive while draining requests, so they are not dropped unintentionally.
        """
        self._ongoing_requests += 1
        # Since the ongoing request is changed, we need to check if
        # `self._prevent_node_downscale_ref` is set to prevent the node from being
        # downscaled when the draining flag is true.
        self._try_set_prevent_downscale_ref()

    def _ongoing_requests_end(self):
        """Ongoing requests end.

        Decrement the ongoing request counter and drop the dummy object reference
        signaling that the node can be downscaled safely.
        """
        self._ongoing_requests -= 1
        if self._ongoing_requests == 0:
            logger.info(
                "Dropping keep alive object reference to allow downscaling.",
                extra={"log_to_stderr": False},
            )
            self._prevent_node_downscale_ref = None

    async def proxy_request(self, serve_request: ServeRequest) -> ServeResponse:
        """Wrapper for proxy request.

        This method wraps the request input into ServeRequest object and output
        response into ServeResponse object to be used commonly by both HTTP and
        GRPC proxies. It also handles the routing, including `/-/routes` and
        `/-/healthz`, ongoing request counter and keep alive object, and metrics
        counters.
        """
        assert serve_request.request_type in {"http", "websocket", "grpc"}

        method = serve_request.method

        # only use the non-root part of the path for routing
        route_path = serve_request.route_path

        if route_path == "/-/routes":
            if self._draining:
                return await self._draining_response(serve_request=serve_request)

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": "",
                    "status_code": "200",
                }
            )
            return await starlette.responses.JSONResponse(self.route_info)(
                serve_request.scope, serve_request.receive, serve_request.send
            )

        if route_path == "/-/healthz":
            if self._draining:
                return await self._draining_response(serve_request=serve_request)

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": "",
                    "status_code": "200",
                }
            )
            return await starlette.responses.PlainTextResponse("success")(
                serve_request.scope, serve_request.receive, serve_request.send
            )

        try:
            self._ongoing_requests_start()

            matched_route = self.prefix_router.match_route(route_path)
            if matched_route is None and isinstance(serve_request, ASGIServeRequest):
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
                return await self._not_found(
                    serve_request.scope, serve_request.receive, serve_request.send
                )

            route_prefix, handle, app_name, app_is_cross_language = matched_route

            # Modify the path and root path so that reverse lookups and redirection
            # work as expected. We do this here instead of in replicas so it can be
            # changed without restarting the replicas.
            if route_prefix != "/" and isinstance(serve_request, ASGIServeRequest):
                assert not route_prefix.endswith("/")
                serve_request.set_path(route_path.replace(route_prefix, "", 1))
                serve_request.set_root_path(serve_request.root_path + route_prefix)

            start_time = time.time()
            handle, request_id = self.setup_request_context_and_handle(
                app_name=app_name,
                handle=handle,
                route_path=route_path,
                serve_request=serve_request,
            )

            # Streaming codepath isn't supported for Java.
            if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING and not app_is_cross_language:
                serve_response = await self.send_request_to_replica_streaming(
                    request_id=request_id,
                    handle=handle,
                    serve_request=serve_request,
                )
            else:
                serve_response = await self.send_request_to_replica_unary(
                    handle=handle,
                    serve_request=serve_request,
                )

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": app_name,
                    "status_code": serve_response.status_code,
                }
            )

            latency_ms = (time.time() - start_time) * 1000.0
            self.processing_latency_tracker.observe(
                latency_ms,
                tags={
                    "route": route_path,
                    "application": app_name,
                    "status_code": serve_response.status_code,
                },
            )
            logger.info(
                access_log_msg(
                    method=method,
                    status=str(serve_response.status_code),
                    latency_ms=latency_ms,
                ),
                extra={"log_to_stderr": False},
            )
            if serve_response.status_code != "200":
                self.request_error_counter.inc(
                    tags={
                        "route": route_path,
                        "error_code": serve_response.status_code,
                        "method": method,
                    }
                )
                self.deployment_request_error_counter.inc(
                    tags={
                        "deployment": handle.deployment_name,
                        "error_code": serve_response.status_code,
                        "method": method,
                        "route": route_path,
                        "application": app_name,
                    }
                )
        finally:
            # If anything during the request failed, we still want to ensure the ongoing
            # request counter is decremented and possibly reset the keep alive object.
            self._ongoing_requests_end()

        return serve_response

    async def send_request_to_replica_unary(
        self,
        handle: RayServeHandle,
        serve_request: ServeRequest,
    ) -> ServeResponse:
        http_body_bytes = await receive_http_body(
            serve_request.scope, serve_request.receive, serve_request.send
        )

        # NOTE(edoakes): it's important that we defer building the starlette
        # request until it reaches the replica to avoid unnecessary
        # serialization cost, so we use a simple dataclass here.
        request = HTTPRequestWrapper(serve_request.scope, http_body_bytes)

        # Perform a pickle here to improve latency. Stdlib pickle for simple
        # dataclasses are 10-100x faster than cloudpickle.
        request = pickle.dumps(request)

        retries = 0
        backoff_time_s = 0.05
        backoff = False
        loop = get_or_create_event_loop()
        # We have received all the http request conent. The next `receive`
        # call might never arrive; if it does, it can only be `http.disconnect`.
        while retries < HTTP_REQUEST_MAX_RETRIES + 1:
            assignment_task: asyncio.Task = handle.remote(request)
            client_disconnection_task = loop.create_task(serve_request.receive())
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
                    f"Client from {serve_request.client} disconnected, cancelling the "
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
                    backoff = True
                else:
                    result = await object_ref
                    break
            except asyncio.CancelledError:
                # Here because the client disconnected, we will return a custom
                # error code for metric tracking.
                return ServeResponse(status_code=DISCONNECT_ERROR_CODE)
            except RayTaskError as e:
                error_message = f"Unexpected error, traceback: {e}."
                await Response(error_message, status_code=500).send(
                    serve_request.scope, serve_request.receive, serve_request.send
                )
                return ServeResponse(status_code="500")
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
            await Response(error_message, status_code=500).send(
                serve_request.scope, serve_request.receive, serve_request.send
            )
            return ServeResponse(status_code="500")

        if isinstance(result, (starlette.responses.Response, RawASGIResponse)):
            await result(serve_request.scope, serve_request.receive, serve_request.send)
            return ServeResponse(status_code=str(result.status_code))
        else:
            await Response(result).send(
                serve_request.scope, serve_request.receive, serve_request.send
            )
            return ServeResponse(status_code="200")

    async def _assign_request_with_timeout(
        self,
        handle: RayServeHandle,
        serve_request: ServeRequest,
        disconnected_task: Optional[asyncio.Task] = None,
        timeout_s: Optional[float] = None,
    ) -> Optional[StreamingObjectRefGenerator]:
        """Attempt to send a request on the handle within the timeout.

        If `timeout_s` is exceeded while trying to assign a replica, `TimeoutError`
        will be raised.

        `disconnected_task` is expected to be done if the client disconnects; in this
        case, we will abort assigning a replica and return `None`.
        """
        assignment_task = None
        if isinstance(serve_request, ASGIServeRequest):
            assignment_task = handle.remote(
                StreamingHTTPRequest(
                    pickled_asgi_scope=pickle.dumps(serve_request.scope),
                    http_proxy_handle=self.self_actor_handle,
                )
            )
        if isinstance(serve_request, GRPCServeRequest):
            assignment_task = handle.remote(
                GRPCRequest(
                    grpc_user_request=serve_request.user_request,
                    grpc_proxy_handle=self.self_actor_handle,
                )
            )

        tasks = []
        if assignment_task is not None:
            tasks.append(assignment_task)
        if disconnected_task is not None:
            tasks.append(disconnected_task)
        done, _ = await asyncio.wait(
            tasks,
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

    def generate_request_id(self) -> str:
        return get_random_letters(10)

    @property
    def proxy_name(self) -> str:
        """Proxy name used for metrics.

        Each proxy needs to implement its own logic for setting up the proxy name.
        """
        raise NotImplementedError

    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: RayServeHandle,
        route_path: str,
        serve_request: ServeRequest,
    ) -> Tuple[RayServeHandle, str]:
        """Setup the request context and handle for the request.

        Each proxy needs to implement its own logic for setting up the request context
        and handle.
        """
        raise NotImplementedError

    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: RayServeHandle,
        serve_request: ServeRequest,
    ) -> ServeResponse:
        """Send the request to the replica and handle streaming response.

        Each proxy needs to implement its own logic for sending the request and
        handling the streaming response.
        """
        raise NotImplementedError


class GRPCProxy(GenericProxy):
    """This class is meant to be instantiated and run by an gRPC server.

    >>> import grpc
    >>> controller_name = ... # doctest: +SKIP
    >>> grpc_server = grpc.aio.server() # doctest: +SKIP
    >>> serve_pb2_grpc.add_RayServeServiceServicer_to_server( # doctest: +SKIP
    >>>     self.grpc_proxy, grpc_server # doctest: +SKIP
    >>> ) # doctest: +SKIP
    """

    async def Predict(
        self, request: serve_pb2.RayServeRequest, context
    ) -> serve_pb2.RayServeResponse:
        """Entry point of the gRPC proxy.

        This method is called by the gRPC server when a request is received. It
        wraps the request in a ServeRequest object and calls proxy_request. The return
        value is protobuf RayServeResponse object.
        """
        app_name = request.application
        route_path, handle = self.prefix_router.match_target(app_name)
        serve_request = GRPCServeRequest(
            request=request,
            route_path=route_path,
            stream=False,
        )
        serve_response = await self.proxy_request(serve_request=serve_request)
        return serve_response.response

    async def PredictStreaming(
        self, request: serve_pb2.RayServeRequest, context
    ) -> Generator[serve_pb2.RayServeResponse, None, None]:
        """Entry point of the gRPC proxy.

        This method is called by the gRPC server when a request is received. It
        wraps the request in a ServeRequest object and calls proxy_request. The return
        value is protobuf RayServeResponse object.
        """
        app_name = request.application
        route_path, handle = self.prefix_router.match_target(app_name)
        serve_request = GRPCServeRequest(
            request=request,
            route_path=route_path,
            stream=True,
        )
        serve_response = await self.proxy_request(serve_request=serve_request)
        async for response in serve_response.streaming_response:
            yield response

    @property
    def proxy_name(self) -> str:
        return "GRPC"

    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: RayServeHandle,
        route_path: str,
        serve_request: ServeRequest,
    ) -> Tuple[RayServeHandle, str]:
        multiplexed_model_id = serve_request.multiplexed_model_id
        request_id = serve_request.request_id
        if not request_id:
            request_id = self.generate_request_id()
            serve_request.set_request_id(request_id)

        handle = handle.options(
            stream=serve_request.stream,
            serve_grpc_request=True,
        )
        if multiplexed_model_id:
            handle = handle.options(multiplexed_model_id=multiplexed_model_id)

        request_context_info = {
            "route": route_path,
            "request_id": request_id,
            "app_name": app_name,
            "multiplexed_model_id": serve_request.multiplexed_model_id,
        }
        ray.serve.context._serve_request_context.set(
            ray.serve.context.RequestContext(**request_context_info)
        )
        return handle, request_id

    async def _streaming_generator_helper(
        self,
        obj_ref_generator: StreamingObjectRefGenerator,
        request_id: str,
    ) -> Generator[serve_pb2.RayServeResponse, None, None]:
        while True:
            try:
                obj_ref = await obj_ref_generator._next_async()
                response = await obj_ref
                yield serve_pb2.RayServeResponse(
                    user_response=response,
                    request_id=request_id,
                )
            except StopAsyncIteration:
                break

    async def _consume_generator_stream(
        self,
        obj_ref: StreamingObjectRefGenerator,
        request_id: str,
    ) -> ServeResponse:
        streaming_response = self._streaming_generator_helper(obj_ref, request_id)
        return ServeResponse(status_code="200", streaming_response=streaming_response)

    async def _consume_generator_unary(
        self,
        obj_ref: ray.ObjectRef,
        request_id: str,
    ) -> ServeResponse:
        user_response = await obj_ref
        response = serve_pb2.RayServeResponse(
            user_response=user_response,
            request_id=request_id,
        )

        return ServeResponse(status_code="200", response=response)

    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: RayServeHandle,
        serve_request: ServeRequest,
    ) -> ServeResponse:
        try:
            try:
                obj_ref = await self._assign_request_with_timeout(
                    handle=handle,
                    serve_request=serve_request,
                    timeout_s=self.request_timeout_s,
                )
                if obj_ref is None:
                    logger.info(
                        f"Client from {serve_request.client} disconnected, "
                        "cancelling the request.",
                        extra={"log_to_stderr": False},
                    )
                    return DISCONNECT_ERROR_CODE, None
            except TimeoutError:
                logger.warning(
                    f"Request {request_id} timed out after "
                    f"{self.request_timeout_s}s while waiting for assignment."
                )
                return TIMEOUT_ERROR_CODE, None

            if serve_request.stream:
                return await self._consume_generator_stream(
                    obj_ref=obj_ref, request_id=request_id
                )
            else:
                return await self._consume_generator_unary(
                    obj_ref=obj_ref, request_id=request_id
                )

        except Exception as e:
            logger.exception(e)
            return ServeResponse(status_code="500")


class HTTPProxy(GenericProxy):
    """This class is meant to be instantiated and run by an ASGI HTTP server.

    >>> import uvicorn
    >>> controller_name = ... # doctest: +SKIP
    >>> uvicorn.run(HTTPProxy(controller_name)) # doctest: +SKIP
    """

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """
        serve_request = ASGIServeRequest(scope=scope, receive=receive, send=send)
        await self.proxy_request(serve_request=serve_request)

    @property
    def proxy_name(self) -> str:
        return "HTTP"

    def setup_request_context_and_handle(
        self,
        serve_request: ServeRequest,
        route_path: str,
        handle: RayServeHandle,
        app_name: str,
    ) -> Tuple[RayServeHandle, str]:
        request_id = self.generate_request_id()
        request_context_info = {
            "route": route_path,
            "request_id": request_id,
            "app_name": app_name,
        }
        for key, value in serve_request.headers:
            if key.decode() == SERVE_MULTIPLEXED_MODEL_ID:
                multiplexed_model_id = value.decode()
                handle = handle.options(multiplexed_model_id=multiplexed_model_id)
                request_context_info["multiplexed_model_id"] = multiplexed_model_id
            if key.decode().upper() == RAY_SERVE_REQUEST_ID_HEADER:
                request_context_info["request_id"] = value.decode()
        ray.serve.context._serve_request_context.set(
            ray.serve.context.RequestContext(**request_context_info)
        )
        return handle, request_id

    async def receive_asgi_messages(self, request_id: str) -> List[Message]:
        queue = self.asgi_receive_queues.get(request_id, None)
        if queue is None:
            raise KeyError(f"Request ID {request_id} not found.")

        await queue.wait_for_message()
        return queue.get_messages_nowait()

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
        while True:
            msg = await receive()
            await queue(msg)

            if msg["type"] == "http.disconnect":
                return None

            if msg["type"] == "websocket.disconnect":
                return msg["code"]

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
                    raise TimeoutError

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
            except StopAsyncIteration:
                break

        return status_code

    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: RayServeHandle,
        serve_request: ServeRequest,
    ) -> ServeResponse:
        # Proxy the receive interface by placing the received messages on a queue.
        # The downstream replica must call back into `receive_asgi_messages` on this
        # actor to receive the messages.
        receive_queue = ASGIMessageQueue()
        self.asgi_receive_queues[request_id] = receive_queue
        proxy_asgi_receive_task = get_or_create_event_loop().create_task(
            self.proxy_asgi_receive(serve_request.receive, receive_queue)
        )

        status_code = ""
        start = time.time()
        try:
            try:
                obj_ref_generator = await self._assign_request_with_timeout(
                    handle=handle,
                    serve_request=serve_request,
                    disconnected_task=proxy_asgi_receive_task,
                    timeout_s=self.request_timeout_s,
                )
                if obj_ref_generator is None:
                    logger.info(
                        f"Client from {serve_request.client} disconnected, "
                        "cancelling the request.",
                        extra={"log_to_stderr": False},
                    )
                    return ServeResponse(status_code=DISCONNECT_ERROR_CODE)
            except TimeoutError:
                logger.warning(
                    f"Request {request_id} timed out after "
                    f"{self.request_timeout_s}s while waiting for assignment."
                )
                return ServeResponse(status_code=TIMEOUT_ERROR_CODE)

            try:
                status_code = await self._consume_and_send_asgi_message_generator(
                    obj_ref_generator,
                    serve_request.send,
                    timeout_s=calculate_remaining_timeout(
                        timeout_s=self.request_timeout_s,
                        start_time_s=start,
                        curr_time_s=time.time(),
                    ),
                )
            except TimeoutError:
                logger.warning(
                    f"Request {request_id} timed out after "
                    f"{self.request_timeout_s}s while executing."
                )
                return ServeResponse(status_code=TIMEOUT_ERROR_CODE)

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
                    and serve_request.request_type == "websocket"
                    and proxy_asgi_receive_task.exception() is None
                ):
                    status_code = str(proxy_asgi_receive_task.result())

            del self.asgi_receive_queues[request_id]

        return ServeResponse(status_code=status_code)


class RequestIdMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        async def send_with_request_id(message: Dict):
            request_id = ray.serve.context._serve_request_context.get().request_id
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.append(RAY_SERVE_REQUEST_ID_HEADER, request_id)
            if message["type"] == "websocket.accept":
                message[RAY_SERVE_REQUEST_ID_HEADER] = request_id
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
        self.grpc_port = DEFAULT_GRPC_PORT
        self.root_path = root_path

        self.http_setup_complete = asyncio.Event()
        self.grpc_setup_complete = asyncio.Event()

        self.http_proxy = HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            request_timeout_s=(
                request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
            ),
        )
        self.grpc_proxy = GRPCProxy(
            controller_name=controller_name,
            node_id=node_id,
            request_timeout_s=(
                request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
            ),
        )

        self.wrapped_http_proxy = self.http_proxy

        for middleware in http_middlewares:
            self.wrapped_http_proxy = middleware.cls(
                self.wrapped_http_proxy, **middleware.options
            )

        # Start running the HTTP server on the event loop.
        # This task should be running forever. We track it in case of failure.
        self.running_task_http = get_or_create_event_loop().create_task(
            self.run_http_server()
        )

        # Right now just always start the GRPC server on the side. We can decide if we
        # want to make this configurable later.
        self.running_task_grpc = get_or_create_event_loop().create_task(
            self.run_grpc_server()
        )

    async def ready(self):
        """Returns when both HTTP and GRPC proxies are ready to serve traffic.
        Or throw exception when either proxy is not able to serve traffic.
        """
        setup_task_http = get_or_create_event_loop().create_task(
            self.http_setup_complete.wait()
        )
        setup_task_grpc = get_or_create_event_loop().create_task(
            self.grpc_setup_complete.wait()
        )

        waiting_tasks_http = [
            # Either the HTTP setup has completed.
            # The event is set inside self.run_http_server.
            setup_task_http,
            # Or self.run_http_server errored.
            self.running_task_http,
        ]
        done_set_http, _ = await asyncio.wait(
            waiting_tasks_http,
            return_when=asyncio.FIRST_COMPLETED,
        )
        waiting_tasks_grpc = [
            # Either the GRPC setup has completed.
            # The event is set inside self.run_grpc_server.
            setup_task_grpc,
            # Or self.run_grpc_server errored.
            self.running_task_grpc,
        ]
        done_set_grpc, _ = await asyncio.wait(
            waiting_tasks_grpc,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Return metadata, or re-throw the exception from self.running_task.
        if self.http_setup_complete.is_set() and self.grpc_setup_complete.is_set():
            # NOTE(zcin): We need to convert the metadata to a json string because
            # of cross-language scenarios. Java can't deserialize a Python tuple.
            return json.dumps(
                [
                    ray.get_runtime_context().get_worker_id(),
                    get_component_logger_file_path(),
                ]
            )
        elif not self.http_setup_complete.is_set():
            return await done_set_http.pop()
        else:
            return await done_set_grpc.pop()

    async def block_until_endpoint_exists(
        self, endpoint: EndpointTag, timeout_s: float
    ):
        await self.http_proxy.block_until_endpoint_exists(endpoint, timeout_s)
        await self.grpc_proxy.block_until_endpoint_exists(endpoint, timeout_s)

    async def run_http_server(self):
        sock = socket.socket()
        if SOCKET_REUSE_PORT_ENABLED:
            set_socket_reuse_port(sock)
        try:
            sock.bind((self.host, self.port))
        except OSError:
            # The OS failed to bind a socket to the given host and port.
            raise ValueError(
                f"Failed to bind Ray Serve HTTP proxy to '{self.host}:{self.port}'. "
                "Please make sure your http-host and http-port are specified correctly."
            )

        # Note(simon): we have to use lower level uvicorn Config and Server
        # class because we want to run the server as a coroutine. The only
        # alternative is to call uvicorn.run which is blocking.
        config = uvicorn.Config(
            self.wrapped_http_proxy,
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

        logger.info(
            "Starting HTTP server with on node: "
            f"{ray.get_runtime_context().get_node_id()} "
            f"listening on port {self.port}"
        )

        self.http_setup_complete.set()
        await server.serve(sockets=[sock])

    async def run_grpc_server(self):
        grpc_server = grpc.aio.server()
        grpc_server.add_insecure_port(f"[::]:{self.grpc_port}")
        serve_pb2_grpc.add_RayServeServiceServicer_to_server(
            self.grpc_proxy, grpc_server
        )
        await grpc_server.start()
        logger.info(
            "Starting gRPC server with on node: "
            f"{ray.get_runtime_context().get_node_id()} "
            f"listening on port {self.grpc_port}"
        )
        self.grpc_setup_complete.set()
        await grpc_server.wait_for_termination()

    async def check_health(self):
        """No-op method to check on the health of the HTTP Proxy.
        Make sure the async event loop is not blocked.
        """
        pass

    async def receive_asgi_messages(self, request_id: str) -> bytes:
        return pickle.dumps(await self.http_proxy.receive_asgi_messages(request_id))
