import asyncio
import json
import logging
import os
import pickle
import socket
import time
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Type

import grpc
import starlette
import starlette.routing
import uvicorn
from packaging import version
from starlette.datastructures import MutableHeaders
from starlette.middleware import Middleware
from starlette.types import Receive

import ray
from ray import serve
from ray._private.utils import get_or_create_event_loop
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayTaskError
from ray.serve._private.common import (
    DeploymentID,
    EndpointInfo,
    NodeId,
    RequestMetadata,
    RequestProtocol,
)
from ray.serve._private.constants import (
    DEFAULT_LATENCY_BUCKET_MS,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    PROXY_MIN_DRAINING_PERIOD_S,
    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH,
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
    SERVE_NAMESPACE,
)
from ray.serve._private.grpc_util import DummyServicer, create_serve_grpc_server
from ray.serve._private.http_util import (
    MessageQueue,
    convert_object_to_asgi_messages,
    receive_http_body,
    set_socket_reuse_port,
    validate_http_proxy_callback_return,
)
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_cpu_profiler,
    configure_component_logger,
    configure_component_memory_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.proxy_request_response import (
    ASGIProxyRequest,
    HandlerMetadata,
    ProxyRequest,
    ResponseGenerator,
    ResponseHandlerInfo,
    ResponseStatus,
    gRPCProxyRequest,
)
from ray.serve._private.proxy_response_generator import ProxyResponseGenerator
from ray.serve._private.proxy_router import (
    EndpointRouter,
    LongestPrefixRouter,
    ProxyRouter,
)
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import (
    call_function_from_import_path,
    generate_request_id,
    get_head_node_id,
)
from ray.serve.config import gRPCOptions
from ray.serve.exceptions import BackPressureError
from ray.serve.generated.serve_pb2 import HealthzResponse, ListApplicationsResponse
from ray.serve.generated.serve_pb2_grpc import add_RayServeAPIServiceServicer_to_server
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import LoggingConfig
from ray.util import metrics

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

RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S = int(
    os.environ.get("RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S", 0)
)
# TODO (shrekris-anyscale): Deprecate SERVE_REQUEST_PROCESSING_TIMEOUT_S env var
RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S = (
    float(os.environ.get("RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S", 0))
    or float(os.environ.get("SERVE_REQUEST_PROCESSING_TIMEOUT_S", 0))
    or None
)
# Controls whether Ray Serve is operating in debug-mode switching off some
# of the performance optimizations to make troubleshooting easier
RAY_SERVE_DEBUG_MODE = bool(os.environ.get("RAY_SERVE_DEBUG_MODE", 0))

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

HEALTHY_MESSAGE = "success"
DRAINING_MESSAGE = "This node is being drained."


class GenericProxy(ABC):
    """This class is served as the base class for different types of proxies.
    It contains all the common setup and methods required for running a proxy.

    The proxy subclass need to implement the following methods:
      - `protocol()`
      - `not_found_response()`
      - `routes_response()`
      - `health_response()`
      - `setup_request_context_and_handle()`
      - `send_request_to_replica()`
    """

    def __init__(
        self,
        node_id: NodeId,
        node_ip_address: str,
        is_head: bool,
        proxy_router_class: Type[ProxyRouter],
        request_timeout_s: Optional[float] = None,
        get_handle_override: Optional[Callable] = None,
    ):
        self.request_timeout_s = request_timeout_s
        if self.request_timeout_s is not None and self.request_timeout_s < 0:
            self.request_timeout_s = None

        self._node_id = node_id
        self._is_head = is_head

        # Used only for displaying the route table.
        self.route_info: Dict[str, DeploymentID] = dict()

        self.proxy_router = proxy_router_class(
            get_handle_override
            or partial(serve.get_deployment_handle, _record_telemetry=False),
            self.protocol,
        )
        self.request_counter = metrics.Counter(
            f"serve_num_{self.protocol.lower()}_requests",
            description=f"The number of {self.protocol} requests processed.",
            tag_keys=("route", "method", "application", "status_code"),
        )

        self.request_error_counter = metrics.Counter(
            f"serve_num_{self.protocol.lower()}_error_requests",
            description=f"The number of errored {self.protocol} responses.",
            tag_keys=(
                "route",
                "error_code",
                "method",
                "application",
            ),
        )

        self.deployment_request_error_counter = metrics.Counter(
            f"serve_num_deployment_{self.protocol.lower()}_error_requests",
            description=(
                f"The number of errored {self.protocol} "
                "responses returned by each deployment."
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
            f"serve_{self.protocol.lower()}_request_latency_ms",
            description=(
                f"The end-to-end latency of {self.protocol} requests "
                f"(measured from the Serve {self.protocol} proxy)."
            ),
            boundaries=DEFAULT_LATENCY_BUCKET_MS,
            tag_keys=(
                "method",
                "route",
                "application",
                "status_code",
            ),
        )

        self.num_ongoing_requests_gauge = metrics.Gauge(
            name=f"serve_num_ongoing_{self.protocol.lower()}_requests",
            description=f"The number of ongoing requests in this {self.protocol} "
            "proxy.",
            tag_keys=("node_id", "node_ip_address"),
        ).set_default_tags(
            {
                "node_id": node_id,
                "node_ip_address": node_ip_address,
            }
        )

        # `self._ongoing_requests` is used to count the number of ongoing requests
        self._ongoing_requests = 0
        # The time when the node starts to drain.
        # The node is not draining if it's None.
        self._draining_start_time: Optional[float] = None

        getattr(ServeUsageTag, f"{self.protocol.upper()}_PROXY_USED").record("1")

    @property
    @abstractmethod
    def protocol(self) -> RequestProtocol:
        """Protocol used in the proxy.

        Each proxy needs to implement its own logic for setting up the protocol.
        """
        raise NotImplementedError

    def _is_draining(self) -> bool:
        """Whether is proxy actor is in the draining status or not."""
        return self._draining_start_time is not None

    def update_routes(self, endpoints: Dict[DeploymentID, EndpointInfo]):
        self.route_info: Dict[str, DeploymentID] = dict()
        for deployment_id, info in endpoints.items():
            route = info.route
            self.route_info[route] = deployment_id

        self.proxy_router.update_routes(endpoints)

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
        """Update the draining status of the proxy.

        This is called by the proxy state manager
        to drain or un-drain the proxy actor.
        """

        if draining and (not self._is_draining()):
            logger.info(
                f"Start to drain the proxy actor on node {self._node_id}.",
                extra={"log_to_stderr": False},
            )
            self._draining_start_time = time.time()
        if (not draining) and self._is_draining():
            logger.info(
                f"Stop draining the proxy actor on node {self._node_id}.",
                extra={"log_to_stderr": False},
            )
            self._draining_start_time = None

    @abstractmethod
    async def not_found_response(
        self, proxy_request: ProxyRequest
    ) -> ResponseGenerator:
        raise NotImplementedError

    @abstractmethod
    async def routes_response(
        self, *, healthy: bool, message: str
    ) -> ResponseGenerator:
        raise NotImplementedError

    @abstractmethod
    async def health_response(
        self, *, healthy: bool, message: str
    ) -> ResponseGenerator:
        raise NotImplementedError

    def _ongoing_requests_start(self):
        """Ongoing requests start.

        The current autoscale logic can downscale nodes with ongoing requests if the
        node doesn't have replicas and has no primary copies of objects in the object
        store. The counter and the dummy object reference will help to keep the node
        alive while draining requests, so they are not dropped unintentionally.
        """
        self._ongoing_requests += 1
        self.num_ongoing_requests_gauge.set(self._ongoing_requests)

    def _ongoing_requests_end(self):
        """Ongoing requests end.

        Decrement the ongoing request counter and drop the dummy object reference
        signaling that the node can be downscaled safely.
        """
        self._ongoing_requests -= 1
        self.num_ongoing_requests_gauge.set(self._ongoing_requests)

    def _get_health_or_routes_reponse(
        self, proxy_request: ProxyRequest
    ) -> ResponseHandlerInfo:
        """Get the response handler for system health and route endpoints.

        If the proxy is draining or has not yet received a route table update from the
        controller, both will return a non-OK status.
        """
        router_ready_for_traffic, router_msg = self.proxy_router.ready_for_traffic(
            self._is_head
        )
        if self._is_draining():
            healthy = False
            message = DRAINING_MESSAGE
        elif not router_ready_for_traffic:
            healthy = False
            message = router_msg
        else:
            healthy = True
            message = HEALTHY_MESSAGE

        if proxy_request.is_health_request:
            response_generator = self.health_response(healthy=healthy, message=message)
        else:
            assert proxy_request.is_route_request
            response_generator = self.routes_response(healthy=healthy, message=message)

        return ResponseHandlerInfo(
            response_generator=response_generator,
            metadata=HandlerMetadata(
                route=proxy_request.route_path,
            ),
            should_record_access_log=False,
            should_increment_ongoing_requests=False,
        )

    def _get_response_handler_info(
        self, proxy_request: ProxyRequest
    ) -> ResponseHandlerInfo:
        if proxy_request.is_health_request or proxy_request.is_route_request:
            return self._get_health_or_routes_reponse(proxy_request)

        matched_route = None
        if self.protocol == RequestProtocol.HTTP:
            matched_route = self.proxy_router.match_route(proxy_request.route_path)
        elif self.protocol == RequestProtocol.GRPC:
            matched_route = self.proxy_router.get_handle_for_endpoint(
                proxy_request.route_path
            )

        if matched_route is None:
            return ResponseHandlerInfo(
                response_generator=self.not_found_response(proxy_request),
                metadata=HandlerMetadata(
                    route=proxy_request.route_path,
                ),
                should_record_access_log=True,
                should_increment_ongoing_requests=False,
            )
        else:
            route_prefix, handle, app_is_cross_language = matched_route
            # Modify the path and root path so that reverse lookups and redirection
            # work as expected. We do this here instead of in replicas so it can be
            # changed without restarting the replicas.
            route_path = proxy_request.route_path
            if route_prefix != "/" and self.protocol == RequestProtocol.HTTP:
                assert not route_prefix.endswith("/")
                proxy_request.set_root_path(proxy_request.root_path + route_prefix)
                # NOTE(edoakes): starlette<0.33.0 expected the ASGI 'root_prefix'
                # to be stripped from the 'path', which wasn't technically following
                # the standard. See https://github.com/encode/starlette/pull/2352.
                if version.parse(starlette.__version__) < version.parse("0.33.0"):
                    proxy_request.set_path(route_path.replace(route_prefix, "", 1))

            internal_request_id = generate_request_id()
            handle, request_id = self.setup_request_context_and_handle(
                app_name=handle.deployment_id.app_name,
                handle=handle,
                route_path=route_path,
                proxy_request=proxy_request,
                internal_request_id=internal_request_id,
            )

            response_generator = self.send_request_to_replica(
                request_id=request_id,
                internal_request_id=internal_request_id,
                handle=handle,
                proxy_request=proxy_request,
                app_is_cross_language=app_is_cross_language,
            )

            return ResponseHandlerInfo(
                response_generator=response_generator,
                metadata=HandlerMetadata(
                    application_name=handle.deployment_id.app_name,
                    deployment_name=handle.deployment_id.name,
                    route=route_path,
                ),
                should_record_access_log=True,
                should_increment_ongoing_requests=True,
            )

    async def proxy_request(self, proxy_request: ProxyRequest) -> ResponseGenerator:
        """Wrapper for proxy request.

        This method is served as common entry point by the proxy. It handles the
        routing, including routes and health checks, ongoing request counter,
        and metrics.
        """
        assert proxy_request.request_type in {"http", "websocket", "grpc"}

        response_handler_info = self._get_response_handler_info(proxy_request)

        start_time = time.time()
        if response_handler_info.should_increment_ongoing_requests:
            self._ongoing_requests_start()

        try:
            # The final message yielded must always be the `ResponseStatus`.
            status: Optional[ResponseStatus] = None
            async for message in response_handler_info.response_generator:
                if isinstance(message, ResponseStatus):
                    status = message

                yield message

            assert status is not None and isinstance(status, ResponseStatus)
        finally:
            # If anything during the request failed, we still want to ensure the ongoing
            # request counter is decremented.
            if response_handler_info.should_increment_ongoing_requests:
                self._ongoing_requests_end()

        latency_ms = (time.time() - start_time) * 1000.0
        if response_handler_info.should_record_access_log:
            logger.info(
                access_log_msg(
                    method=proxy_request.method,
                    status=str(status.code),
                    latency_ms=latency_ms,
                ),
                extra={"log_to_stderr": False, "serve_access_log": True},
            )

        self.request_counter.inc(
            tags={
                "route": response_handler_info.metadata.route,
                "method": proxy_request.method,
                "application": response_handler_info.metadata.application_name,
                "status_code": str(status.code),
            }
        )

        self.processing_latency_tracker.observe(
            latency_ms,
            tags={
                "method": proxy_request.method,
                "route": response_handler_info.metadata.route,
                "application": response_handler_info.metadata.application_name,
                "status_code": str(status.code),
            },
        )
        if status.is_error:
            self.request_error_counter.inc(
                tags={
                    "route": response_handler_info.metadata.route,
                    "error_code": str(status.code),
                    "method": proxy_request.method,
                    "application": response_handler_info.metadata.application_name,
                }
            )
            self.deployment_request_error_counter.inc(
                tags={
                    "deployment": response_handler_info.metadata.deployment_name,
                    "error_code": str(status.code),
                    "method": proxy_request.method,
                    "route": response_handler_info.metadata.route,
                    "application": response_handler_info.metadata.application_name,
                }
            )

    @abstractmethod
    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: DeploymentHandle,
        route_path: str,
        proxy_request: ProxyRequest,
        internal_request_id: str,
    ) -> Tuple[DeploymentHandle, str]:
        """Setup the request context and handle for the request.

        Each proxy needs to implement its own logic for setting up the request context
        and handle.
        """
        raise NotImplementedError

    @abstractmethod
    async def send_request_to_replica(
        self,
        request_id: str,
        internal_request_id: str,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
        app_is_cross_language: bool = False,
    ) -> ResponseGenerator:
        """Send the request to the replica and handle streaming response.

        Each proxy needs to implement its own logic for sending the request and
        handling the streaming response.
        """
        raise NotImplementedError


class gRPCProxy(GenericProxy):
    """This class is meant to be instantiated and run by an gRPC server.

    This is the servicer class for the gRPC server. It implements `unary_unary`
    as the entry point for unary gRPC request and `unary_stream` as the entry
    point for streaming gRPC request.
    """

    @property
    def protocol(self) -> RequestProtocol:
        return RequestProtocol.GRPC

    async def not_found_response(
        self, proxy_request: ProxyRequest
    ) -> ResponseGenerator:
        if not proxy_request.app_name:
            application_message = "Application metadata not set."
        else:
            application_message = f"Application '{proxy_request.app_name}' not found."
        not_found_message = (
            f"{application_message} Ping "
            "/ray.serve.RayServeAPIService/ListApplications for available applications."
        )

        yield ResponseStatus(
            code=grpc.StatusCode.NOT_FOUND,
            message=not_found_message,
            is_error=True,
        )

    async def routes_response(
        self, *, healthy: bool, message: str
    ) -> ResponseGenerator:
        yield ListApplicationsResponse(
            application_names=[
                endpoint.app_name for endpoint in self.route_info.values()
            ],
        ).SerializeToString()

        yield ResponseStatus(
            code=grpc.StatusCode.OK if healthy else grpc.StatusCode.UNAVAILABLE,
            message=message,
            is_error=not healthy,
        )

    async def health_response(self, *, healthy: bool, message) -> ResponseGenerator:
        yield HealthzResponse(message=message).SerializeToString()
        yield ResponseStatus(
            code=grpc.StatusCode.OK if healthy else grpc.StatusCode.UNAVAILABLE,
            message=message,
            is_error=not healthy,
        )

    def service_handler_factory(self, service_method: str, stream: bool) -> Callable:
        def set_grpc_code_and_details(
            context: grpc._cython.cygrpc._ServicerContext, status: ResponseStatus
        ):
            # Only the latest code and details will take effect. If the user already
            # set them to a truthy value in the context, skip setting them with Serve's
            # default values. By default, if nothing is set, the code is 0 and the
            # details is "", which both are falsy. So if the user did not set them or
            # if they're explicitly set to falsy values, such as None, Serve will
            # continue to set them with our default values.
            if not context.code():
                context.set_code(status.code)
            if not context.details():
                context.set_details(status.message)

        async def unary_unary(
            request_proto: Any, context: grpc._cython.cygrpc._ServicerContext
        ) -> bytes:
            """Entry point of the gRPC proxy unary request.

            This method is called by the gRPC server when a unary request is received.
            It wraps the request in a ProxyRequest object and calls proxy_request.
            The return value is serialized user defined protobuf bytes.
            """
            proxy_request = gRPCProxyRequest(
                request_proto=request_proto,
                context=context,
                service_method=service_method,
                stream=False,
            )

            status = None
            response = None
            async for message in self.proxy_request(proxy_request=proxy_request):
                if isinstance(message, ResponseStatus):
                    status = message
                else:
                    response = message

            set_grpc_code_and_details(context, status)

            return response

        async def unary_stream(
            request_proto: Any, context: grpc._cython.cygrpc._ServicerContext
        ) -> Generator[bytes, None, None]:
            """Entry point of the gRPC proxy streaming request.

            This method is called by the gRPC server when a streaming request is
            received. It wraps the request in a ProxyRequest object and calls
            proxy_request. The return value is a generator of serialized user defined
            protobuf bytes.
            """
            proxy_request = gRPCProxyRequest(
                request_proto=request_proto,
                context=context,
                service_method=service_method,
                stream=True,
            )

            status = None
            async for message in self.proxy_request(proxy_request=proxy_request):
                if isinstance(message, ResponseStatus):
                    status = message
                else:
                    yield message

            set_grpc_code_and_details(context, status)

        return unary_stream if stream else unary_unary

    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: DeploymentHandle,
        route_path: str,
        proxy_request: ProxyRequest,
        internal_request_id: str,
    ) -> Tuple[DeploymentHandle, str]:
        """Setup request context and handle for the request.

        Unpack gRPC request metadata and extract info to set up request context and
        handle.
        """
        multiplexed_model_id = proxy_request.multiplexed_model_id
        request_id = proxy_request.request_id
        if not request_id:
            request_id = generate_request_id()
            proxy_request.request_id = request_id

        handle = handle.options(
            stream=proxy_request.stream,
            multiplexed_model_id=multiplexed_model_id,
            method_name=proxy_request.method_name,
        )

        request_context_info = {
            "route": route_path,
            "request_id": request_id,
            "_internal_request_id": internal_request_id,
            "app_name": app_name,
            "multiplexed_model_id": multiplexed_model_id,
            "grpc_context": proxy_request.ray_serve_grpc_context,
        }
        ray.serve.context._serve_request_context.set(
            ray.serve.context._RequestContext(**request_context_info)
        )
        proxy_request.send_request_id(request_id=request_id)
        return handle, request_id

    async def send_request_to_replica(
        self,
        request_id: str,
        internal_request_id: str,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
        app_is_cross_language: bool = False,
    ) -> ResponseGenerator:
        handle_arg = proxy_request.request_object()
        response_generator = ProxyResponseGenerator(
            handle.remote(handle_arg),
            timeout_s=self.request_timeout_s,
        )

        try:
            async for context, result in response_generator:
                context._set_on_grpc_context(proxy_request.context)
                yield result

            status = ResponseStatus(code=grpc.StatusCode.OK)
        except TimeoutError:
            message = f"Request timed out after {self.request_timeout_s}s."
            logger.warning(message)
            status = ResponseStatus(
                code=grpc.StatusCode.DEADLINE_EXCEEDED,
                is_error=True,
                message=message,
            )
        except asyncio.CancelledError:
            message = f"Client for request {request_id} disconnected."
            logger.info(message)
            status = ResponseStatus(
                code=grpc.StatusCode.CANCELLED,
                is_error=True,
                message=message,
            )
        except BackPressureError as e:
            status = ResponseStatus(
                code=grpc.StatusCode.UNAVAILABLE,
                is_error=True,
                message=e.message,
            )
        except Exception as e:
            if isinstance(e, (RayActorError, RayTaskError)):
                logger.warning(f"Request failed: {e}", extra={"log_to_stderr": False})
            else:
                logger.exception("Request failed due to unexpected error.")
            status = ResponseStatus(
                code=grpc.StatusCode.INTERNAL,
                is_error=True,
                message=str(e),
            )

        # The status code should always be set.
        assert status is not None
        yield status


class HTTPProxy(GenericProxy):
    """This class is meant to be instantiated and run by an ASGI HTTP server."""

    def __init__(
        self,
        node_id: NodeId,
        node_ip_address: str,
        is_head: bool,
        proxy_router_class: Type[ProxyRouter],
        request_timeout_s: Optional[float] = None,
        proxy_actor: Optional[ActorHandle] = None,
        get_handle_override: Optional[Callable] = None,
    ):
        super().__init__(
            node_id,
            node_ip_address,
            is_head,
            proxy_router_class,
            request_timeout_s=request_timeout_s,
            get_handle_override=get_handle_override,
        )
        self.self_actor_handle = proxy_actor or ray.get_runtime_context().current_actor
        self.asgi_receive_queues: Dict[str, MessageQueue] = dict()

    @property
    def protocol(self) -> RequestProtocol:
        return RequestProtocol.HTTP

    async def not_found_response(
        self, proxy_request: ProxyRequest
    ) -> ResponseGenerator:
        status_code = 404
        for message in convert_object_to_asgi_messages(
            f"Path '{proxy_request.path}' not found. "
            "Ping http://.../-/routes for available routes.",
            status_code=status_code,
        ):
            yield message

        yield ResponseStatus(code=status_code, is_error=True)

    async def routes_response(
        self, *, healthy: bool, message: str
    ) -> ResponseGenerator:
        status_code = 200 if healthy else 503
        if healthy:
            response = dict()
            for route, endpoint in self.route_info.items():
                # For 2.x deployments, return {route -> app name}
                if endpoint.app_name:
                    response[route] = endpoint.app_name
                # Keep compatibility with 1.x deployments.
                else:
                    response[route] = endpoint.name
        else:
            response = message

        for asgi_message in convert_object_to_asgi_messages(
            response,
            status_code=status_code,
        ):
            yield asgi_message

        yield ResponseStatus(
            code=status_code,
            message=message,
            is_error=not healthy,
        )

    async def health_response(
        self, *, healthy: bool, message: str = ""
    ) -> ResponseGenerator:
        status_code = 200 if healthy else 503
        for asgi_message in convert_object_to_asgi_messages(
            message,
            status_code=status_code,
        ):
            yield asgi_message

        yield ResponseStatus(
            code=status_code,
            is_error=not healthy,
            message=message,
        )

    async def receive_asgi_messages(
        self, request_metadata: RequestMetadata
    ) -> ResponseGenerator:
        queue = self.asgi_receive_queues.get(request_metadata.internal_request_id, None)
        if queue is None:
            raise KeyError(f"Request ID {request_metadata.request_id} not found.")

        await queue.wait_for_message()
        return queue.get_messages_nowait()

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """
        proxy_request = ASGIProxyRequest(scope=scope, receive=receive, send=send)
        async for message in self.proxy_request(proxy_request):
            if not isinstance(message, ResponseStatus):
                await send(message)

    async def proxy_asgi_receive(
        self, receive: Receive, queue: MessageQueue
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
            # Close the queue so any subsequent calls to fetch messages return
            # immediately: https://github.com/ray-project/ray/issues/38368.
            queue.close()

    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: DeploymentHandle,
        route_path: str,
        proxy_request: ProxyRequest,
        internal_request_id: str,
    ) -> Tuple[DeploymentHandle, str]:
        """Setup request context and handle for the request.

        Unpack HTTP request headers and extract info to set up request context and
        handle.
        """
        request_context_info = {
            "route": route_path,
            "app_name": app_name,
            "_internal_request_id": internal_request_id,
            "is_http_request": True,
        }
        for key, value in proxy_request.headers:
            if key.decode() == SERVE_MULTIPLEXED_MODEL_ID:
                multiplexed_model_id = value.decode()
                handle = handle.options(multiplexed_model_id=multiplexed_model_id)
                request_context_info["multiplexed_model_id"] = multiplexed_model_id
            if key.decode() == "x-request-id":
                request_context_info["request_id"] = value.decode()
        ray.serve.context._serve_request_context.set(
            ray.serve.context._RequestContext(**request_context_info)
        )
        return handle, request_context_info["request_id"]

    async def _format_handle_arg_for_java(
        self,
        proxy_request: ProxyRequest,
    ) -> bytes:
        """Convert an HTTP request to the Java-accepted format (single byte string)."""
        query_string = proxy_request.scope.get("query_string")
        http_body_bytes = await receive_http_body(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )
        if query_string:
            arg = query_string.decode().split("=", 1)[1]
        else:
            arg = http_body_bytes.decode()

        return arg

    async def send_request_to_replica(
        self,
        request_id: str,
        internal_request_id: str,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
        app_is_cross_language: bool = False,
    ) -> ResponseGenerator:
        """Send the request to the replica and yield its response messages.

        The yielded values will be ASGI messages until the final one, which will be
        the status code.
        """
        if app_is_cross_language:
            handle_arg = await self._format_handle_arg_for_java(proxy_request)
            # Response is returned as raw bytes, convert it to ASGI messages.
            result_callback = convert_object_to_asgi_messages
        else:
            self_actor_handle = self.self_actor_handle
            handle_arg = proxy_request.request_object(
                receive_asgi_messages=self_actor_handle.receive_asgi_messages.remote
            )
            # Messages are returned as pickled dictionaries.
            result_callback = pickle.loads

        # Proxy the receive interface by placing the received messages on a queue.
        # The downstream replica must call back into `receive_asgi_messages` on this
        # actor to receive the messages.
        receive_queue = MessageQueue()
        self.asgi_receive_queues[internal_request_id] = receive_queue
        proxy_asgi_receive_task = get_or_create_event_loop().create_task(
            self.proxy_asgi_receive(proxy_request.receive, receive_queue)
        )

        response_generator = ProxyResponseGenerator(
            handle.remote(handle_arg),
            timeout_s=self.request_timeout_s,
            disconnected_task=proxy_asgi_receive_task,
            result_callback=result_callback,
        )

        status: Optional[ResponseStatus] = None
        response_started = False
        expecting_trailers = False
        try:
            async for asgi_message_batch in response_generator:
                # See the ASGI spec for message details:
                # https://asgi.readthedocs.io/en/latest/specs/www.html.
                for asgi_message in asgi_message_batch:
                    if asgi_message["type"] == "http.response.start":
                        # HTTP responses begin with exactly one
                        # "http.response.start" message containing the "status"
                        # field. Other response types (e.g., WebSockets) may not.
                        status_code = str(asgi_message["status"])
                        status = ResponseStatus(
                            code=status_code,
                            # TODO(edoakes): we need a more nuanced check than this.
                            is_error=status_code != "200",
                        )
                        expecting_trailers = asgi_message.get("trailers", False)
                    elif asgi_message["type"] == "websocket.accept":
                        # Websocket code explicitly handles client disconnects,
                        # so let the ASGI disconnect message propagate instead of
                        # cancelling the handler.
                        response_generator.stop_checking_for_disconnect()
                    elif (
                        asgi_message["type"] == "http.response.body"
                        and not asgi_message.get("more_body", False)
                        and not expecting_trailers
                    ):
                        # If the body is completed and we aren't expecting trailers, the
                        # response is done so we should stop listening for disconnects.
                        response_generator.stop_checking_for_disconnect()
                    elif asgi_message["type"] == "http.response.trailers":
                        # If we are expecting trailers, the response is only done when
                        # the trailers message has been sent.
                        if not asgi_message.get("more_trailers", False):
                            response_generator.stop_checking_for_disconnect()
                    elif asgi_message["type"] == "websocket.disconnect":
                        status = ResponseStatus(
                            code=str(asgi_message["code"]),
                            # TODO(edoakes): we need a more nuanced check than this.
                            is_error=False,
                        )
                        response_generator.stop_checking_for_disconnect()

                    yield asgi_message
                    response_started = True
        except TimeoutError:
            status = ResponseStatus(
                code=TIMEOUT_ERROR_CODE,
                is_error=True,
            )
            logger.warning(f"Request timed out after {self.request_timeout_s}s.")
            # We should only send timeout response if we have not sent
            # any messages to the client yet. Header (including status code)
            # messages can only be sent once.
            if not response_started:
                for message in convert_object_to_asgi_messages(
                    f"Request {request_id} timed out after {self.request_timeout_s}s.",
                    status_code=408,
                ):
                    yield message
        except asyncio.CancelledError:
            status = ResponseStatus(
                code=DISCONNECT_ERROR_CODE,
                is_error=True,
            )
            logger.info(
                f"Client for request {request_id} disconnected, cancelling request."
            )
        except BackPressureError as e:
            status_code = 503
            status = ResponseStatus(
                code=status_code,
                is_error=True,
                message=e.message,
            )
            for message in convert_object_to_asgi_messages(
                e.message,
                status_code=status_code,
            ):
                yield message
        except Exception as e:
            if isinstance(e, (RayActorError, RayTaskError)):
                logger.warning(f"Request failed: {e}", extra={"log_to_stderr": False})
            else:
                logger.exception("Request failed due to unexpected error.")
            status = ResponseStatus(
                code="500",
                is_error=True,
            )

        finally:
            # For websocket connection, queue receive task is done when receiving
            # disconnect message from client.
            receive_client_disconnect_msg = False
            if not proxy_asgi_receive_task.done():
                proxy_asgi_receive_task.cancel()
            else:
                receive_client_disconnect_msg = True

            # If the server disconnects, status_code can be set above from the
            # disconnect message.
            # If client disconnects, the disconnect code comes from
            # a client message via the receive interface.
            if status is None and proxy_request.request_type == "websocket":
                if receive_client_disconnect_msg:
                    # The disconnect message is sent from the client.
                    status = ResponseStatus(
                        code=str(proxy_asgi_receive_task.result()),
                        is_error=True,
                    )
                else:
                    # The server disconnect without sending a disconnect message
                    # (otherwise the `status` would be set).
                    status = ResponseStatus(
                        code="1000",  # [Sihan] is there a better code for this?
                        is_error=True,
                    )

            del self.asgi_receive_queues[internal_request_id]

        # The status code should always be set.
        assert status is not None
        yield status


class RequestIdMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        headers = MutableHeaders(scope=scope)
        if "x-request-id" not in headers:
            # If X-Request-ID is not set, we
            # generate a new request ID.
            request_id = generate_request_id()
            headers.append("x-request-id", request_id)
        elif "x-request-id" in headers:
            request_id = headers["x-request-id"]

        async def send_with_request_id(message: Dict):
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers.append("X-Request-ID", request_id)
            if message["type"] == "websocket.accept":
                message["X-Request-ID"] = request_id
            await send(message)

        await self.app(scope, receive, send_with_request_id)


@ray.remote(num_cpus=0)
class ProxyActor:
    def __init__(
        self,
        host: str,
        port: int,
        root_path: str,
        node_ip_address: str,
        node_id: NodeId,
        logging_config: LoggingConfig,
        request_timeout_s: Optional[float] = None,
        http_middlewares: Optional[List["starlette.middleware.Middleware"]] = None,
        keep_alive_timeout_s: int = DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
        grpc_options: Optional[gRPCOptions] = None,
        long_poll_client: Optional[LongPollClient] = None,
    ):  # noqa: F821
        self.grpc_options = grpc_options or gRPCOptions()
        self.host = host
        self.port = port
        self.grpc_port = self.grpc_options.port
        self.root_path = root_path
        self.keep_alive_timeout_s = (
            RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S or keep_alive_timeout_s
        )
        self._uvicorn_server = None
        self.node_ip_address = node_ip_address

        self.http_setup_complete = asyncio.Event()
        self.grpc_setup_complete = asyncio.Event()

        self.long_poll_client = long_poll_client or LongPollClient(
            ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE),
            {
                LongPollNamespace.GLOBAL_LOGGING_CONFIG: self._update_logging_config,
                LongPollNamespace.ROUTE_TABLE: self._update_routes_in_proxies,
            },
            call_in_event_loop=get_or_create_event_loop(),
        )

        configure_component_logger(
            component_name="proxy",
            component_id=node_ip_address,
            logging_config=logging_config,
        )

        startup_msg = f"Proxy starting on node {node_id} (HTTP port: {port}"
        if self.should_start_grpc_service():
            startup_msg += f", gRPC port: {self.grpc_options.port})."
        else:
            startup_msg += ")."
        logger.info(startup_msg)
        logger.debug(
            f"Configure Proxy actor {ray.get_runtime_context().get_actor_id()} "
            f"logger with logging config: {logging_config}"
        )

        configure_component_memory_profiler(
            component_name="proxy", component_id=node_ip_address
        )
        self.cpu_profiler, self.cpu_profiler_log = configure_component_cpu_profiler(
            component_name="proxy", component_id=node_ip_address
        )

        if http_middlewares is None:
            http_middlewares = [Middleware(RequestIdMiddleware)]
        else:
            http_middlewares.append(Middleware(RequestIdMiddleware))

        if RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH:
            logger.info(
                "Calling user-provided callback from import path "
                f"'{RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH}'."
            )
            middlewares = validate_http_proxy_callback_return(
                call_function_from_import_path(
                    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH
                )
            )

            http_middlewares.extend(middlewares)

        is_head = node_id == get_head_node_id()
        self.http_proxy = HTTPProxy(
            node_id=node_id,
            node_ip_address=node_ip_address,
            is_head=is_head,
            proxy_router_class=LongestPrefixRouter,
            request_timeout_s=(
                request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
            ),
        )
        self.grpc_proxy = (
            gRPCProxy(
                node_id=node_id,
                node_ip_address=node_ip_address,
                is_head=is_head,
                proxy_router_class=EndpointRouter,
                request_timeout_s=(
                    request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
                ),
            )
            if self.should_start_grpc_service()
            else None
        )

        self.wrapped_http_proxy = self.http_proxy

        for middleware in http_middlewares:
            if version.parse(starlette.__version__) < version.parse("0.35.0"):
                self.wrapped_http_proxy = middleware.cls(
                    self.wrapped_http_proxy, **middleware.options
                )
            else:
                # In starlette >= 0.35.0, middleware.options does not exist:
                # https://github.com/encode/starlette/pull/2381.
                self.wrapped_http_proxy = middleware.cls(
                    self.wrapped_http_proxy,
                    *middleware.args,
                    **middleware.kwargs,
                )

        # Start running the HTTP server on the event loop.
        # This task should be running forever. We track it in case of failure.
        self.running_task_http = get_or_create_event_loop().create_task(
            self.run_http_server()
        )

        # Start running the gRPC server on the event loop.
        # This task should be running forever. We track it in case of failure.
        self.running_task_grpc = get_or_create_event_loop().create_task(
            self.run_grpc_server()
        )

    def _update_routes_in_proxies(self, endpoints: Dict[DeploymentID, EndpointInfo]):
        self.http_proxy.update_routes(endpoints)
        if self.grpc_proxy is not None:
            self.grpc_proxy.update_routes(endpoints)

    def _update_logging_config(self, logging_config: LoggingConfig):
        configure_component_logger(
            component_name="proxy",
            component_id=self.node_ip_address,
            logging_config=logging_config,
        )

    def _get_logging_config(self) -> Tuple:
        """Get the logging configuration (for testing purposes)."""
        log_file_path = None
        for handler in logger.handlers:
            if isinstance(handler, logging.handlers.RotatingFileHandler):
                log_file_path = handler.baseFilename
        return log_file_path

    def should_start_grpc_service(self) -> bool:
        """Determine whether gRPC service should be started.

        gRPC service will only be started if a valid port is provided and if the
        servicer functions are passed.
        """
        return self.grpc_port > 0 and len(self.grpc_options.grpc_servicer_functions) > 0

    async def ready(self):
        """Returns when both HTTP and gRPC proxies are ready to serve traffic.
        Or throw exception when either proxy is not able to serve traffic.
        """
        http_setup_complete_wait_task = get_or_create_event_loop().create_task(
            self.http_setup_complete.wait()
        )
        grpc_setup_complete_wait_task = get_or_create_event_loop().create_task(
            self.grpc_setup_complete.wait()
        )

        waiting_tasks_http = [
            # Either the HTTP setup has completed.
            # The event is set inside self.run_http_server.
            http_setup_complete_wait_task,
            # Or self.run_http_server errored.
            self.running_task_http,
        ]
        done_set_http, _ = await asyncio.wait(
            waiting_tasks_http,
            return_when=asyncio.FIRST_COMPLETED,
        )
        waiting_tasks_grpc = [
            # Either the gRPC setup has completed.
            # The event is set inside self.run_grpc_server.
            grpc_setup_complete_wait_task,
            # Or self.run_grpc_server errored.
            self.running_task_grpc,
        ]
        done_set_grpc, _ = await asyncio.wait(
            waiting_tasks_grpc,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Return metadata, or re-throw the exception from self.running_task_http and
        # self.running_task_grpc.
        if self.http_setup_complete.is_set() and self.grpc_setup_complete.is_set():
            # NOTE(zcin): We need to convert the metadata to a json string because
            # of cross-language scenarios. Java can't deserialize a Python tuple.
            return json.dumps(
                [
                    ray.get_runtime_context().get_worker_id(),
                    get_component_logger_file_path(),
                ]
            )
        else:
            proxy_error = None
            if not self.http_setup_complete.is_set():
                try:
                    await done_set_http.pop()
                except Exception as e:
                    logger.exception(e)
                    proxy_error = e
            if not self.grpc_setup_complete.is_set():
                try:
                    await done_set_grpc.pop()
                except Exception as e:
                    logger.exception(e)
                    proxy_error = e
            raise proxy_error

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

        # NOTE: We have to use lower level uvicorn Config and Server
        # class because we want to run the server as a coroutine. The only
        # alternative is to call uvicorn.run which is blocking.
        config = uvicorn.Config(
            self.wrapped_http_proxy,
            host=self.host,
            port=self.port,
            loop=_determine_target_loop(),
            root_path=self.root_path,
            lifespan="off",
            log_level="warning",
            access_log=False,
            timeout_keep_alive=self.keep_alive_timeout_s,
        )
        self._uvicorn_server = uvicorn.Server(config=config)
        # TODO(edoakes): we need to override install_signal_handlers here
        # because the existing implementation fails if it isn't running in
        # the main thread and uvicorn doesn't expose a way to configure it.
        self._uvicorn_server.install_signal_handlers = lambda: None

        logger.debug(
            "Starting HTTP server on node: "
            f"{ray.get_runtime_context().get_node_id()} "
            f"listening on port {self.port}"
        )

        self.http_setup_complete.set()
        await self._uvicorn_server.serve(sockets=[sock])

    async def run_grpc_server(self):
        if not self.should_start_grpc_service():
            return self.grpc_setup_complete.set()

        grpc_server = create_serve_grpc_server(
            service_handler_factory=self.grpc_proxy.service_handler_factory,
        )

        grpc_server.add_insecure_port(f"[::]:{self.grpc_port}")

        # Dummy servicer is used to be callable for the gRPC server. Serve have a
        # custom gRPC server implementation to redirect calls into gRPCProxy.
        # See: ray/serve/_private/grpc_util.py
        dummy_servicer = DummyServicer()

        # Add Ray Serve gRPC service and methods (e.g. ListApplications and Healthz).
        add_RayServeAPIServiceServicer_to_server(dummy_servicer, grpc_server)

        # Iterate through each of user provided gRPC servicer functions and add user
        # defined services and methods.
        for grpc_servicer_function in self.grpc_options.grpc_servicer_func_callable:
            grpc_servicer_function(dummy_servicer, grpc_server)

        await grpc_server.start()
        logger.debug(
            "Starting gRPC server on node: "
            f"{ray.get_runtime_context().get_node_id()} "
            f"listening on port {self.grpc_port}"
        )
        self.grpc_setup_complete.set()
        await grpc_server.wait_for_termination()

    async def update_draining(self, draining: bool, _after: Optional[Any] = None):
        """Update the draining status of the HTTP and gRPC proxies.

        Unused `_after` argument is for scheduling: passing an ObjectRef
        allows delaying this call until after the `_after` call has returned.
        """

        self.http_proxy.update_draining(draining)
        if self.grpc_proxy:
            self.grpc_proxy.update_draining(draining)

    async def is_drained(self, _after: Optional[Any] = None):
        """Check whether both HTTP and gRPC proxies are drained or not.

        Unused `_after` argument is for scheduling: passing an ObjectRef
        allows delaying this call until after the `_after` call has returned.
        """

        return self.http_proxy.is_drained() and (
            self.grpc_proxy is None or self.grpc_proxy.is_drained()
        )

    async def check_health(self):
        """No-op method to check on the health of the HTTP Proxy.

        Make sure the async event loop is not blocked.
        """
        logger.debug("Received health check.", extra={"log_to_stderr": False})

    async def receive_asgi_messages(self, request_metadata: RequestMetadata) -> bytes:
        """Get ASGI messages for the provided `request_metadata`.

        After the proxy has stopped receiving messages for this `request_metadata`,
        this will always return immediately.

        Raises `KeyError` if this request ID is not found. This will happen when the
        request is no longer being handled (e.g., the user disconnects).
        """
        return pickle.dumps(
            await self.http_proxy.receive_asgi_messages(request_metadata)
        )

    def _save_cpu_profile_data(self) -> str:
        """Saves CPU profiling data, if CPU profiling is enabled.

        Logs a warning if CPU profiling is disabled.
        """

        if self.cpu_profiler is not None:
            import marshal

            self.cpu_profiler.snapshot_stats()
            with open(self.cpu_profiler_log, "wb") as f:
                marshal.dump(self.cpu_profiler.stats, f)
            logger.info(f'Saved CPU profile data to file "{self.cpu_profiler_log}"')
            return self.cpu_profiler_log
        else:
            logger.error(
                "Attempted to save CPU profile data, but failed because no "
                "CPU profiler was running! Enable CPU profiling by enabling "
                "the RAY_SERVE_ENABLE_CPU_PROFILING env var."
            )

    async def _uvicorn_keep_alive(self) -> Optional[int]:
        """Get the keep alive timeout used for the running uvicorn server.

        Return the timeout_keep_alive config used on the uvicorn server if it's running.
        If the server is not running, return None.
        """
        if self._uvicorn_server:
            return self._uvicorn_server.config.timeout_keep_alive


def _determine_target_loop():
    """We determine target loop based on whether RAY_SERVE_DEBUG_MODE is enabled:

    - RAY_SERVE_DEBUG_MODE=0 (default): we use "uvloop" (Cython) providing
                              high-performance, native implementation of the event-loop

    - RAY_SERVE_DEBUG_MODE=1: we fall back to "asyncio" (pure Python) event-loop
                              implementation that is considerably slower than "uvloop",
                              but provides for easy access to the source implementation
    """
    if RAY_SERVE_DEBUG_MODE:
        return "asyncio"
    else:
        return "uvloop"
