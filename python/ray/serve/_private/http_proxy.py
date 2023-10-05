from abc import ABC, abstractmethod
import asyncio
from asyncio.tasks import FIRST_COMPLETED
import json
import os
import logging
import pickle
import socket
import time
import grpc
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Type, Union
import uuid

import uvicorn
import starlette.responses
import starlette.routing
from starlette.types import Message, Receive
from starlette.datastructures import MutableHeaders
from starlette.middleware import Middleware

import ray
from ray.actor import ActorHandle
from ray.exceptions import RayActorError, RayTaskError
from ray.util import metrics
from ray.serve._private.usage import ServeUsageTag
from ray._private.utils import get_or_create_event_loop
from ray._raylet import StreamingObjectRefGenerator

from ray.serve.handle import (
    DeploymentHandle,
    _DeploymentResponseBase,
)
from ray.serve._private.grpc_util import (
    create_serve_grpc_server,
    DummyServicer,
)
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
    EndpointInfo,
    EndpointTag,
    NodeId,
    RequestProtocol,
)
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
    SERVE_MULTIPLEXED_MODEL_ID,
    SERVE_NAMESPACE,
    DEFAULT_LATENCY_BUCKET_MS,
    DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
    PROXY_MIN_DRAINING_PERIOD_S,
    RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING,
    RAY_SERVE_REQUEST_ID_HEADER,
    RAY_SERVE_HTTP_PROXY_CALLBACK_IMPORT_PATH,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace
from ray.serve._private.logging_utils import (
    access_log_msg,
    configure_component_logger,
    configure_component_cpu_profiler,
    configure_component_memory_profiler,
    get_component_logger_file_path,
)
from ray.serve._private.proxy_request_response import (
    ASGIProxyRequest,
    gRPCProxyRequest,
    ProxyRequest,
    ProxyResponse,
)
from ray.serve._private.proxy_router import (
    EndpointRouter,
    LongestPrefixRouter,
    ProxyRouter,
)
from ray.serve._private.utils import (
    calculate_remaining_timeout,
    call_function_from_import_path,
)
from ray.serve.config import gRPCOptions
from ray.serve.generated.serve_pb2 import HealthzResponse, ListApplicationsResponse
from ray.serve.generated.serve_pb2_grpc import add_RayServeAPIServiceServicer_to_server


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
DRAINED_MESSAGE = "This node is being drained."
HEALTH_CHECK_SUCCESS_MESSAGE = "success"


def generate_request_id() -> str:
    return str(uuid.uuid4())


class GenericProxy(ABC):
    """This class is served as the base class for different types of proxies.
    It contains all the common setup and methods required for running a proxy.

    The proxy subclass need to implement the following methods:
      - `protocol()`
      - `success_status_code()`
      - `not_found()`
      - `draining_response()`
      - `timeout_response()`
      - `routes_response()`
      - `health_response()`
      - `send_request_to_replica_unary()`
      - `setup_request_context_and_handle()`
      - `send_request_to_replica_streaming()`
    """

    def __init__(
        self,
        controller_name: str,
        node_id: NodeId,
        node_ip_address: str,
        proxy_router_class: Type[ProxyRouter],
        request_timeout_s: Optional[float] = None,
        controller_actor: Optional[ActorHandle] = None,
        proxy_actor: Optional[ActorHandle] = None,
    ):
        self.request_timeout_s = request_timeout_s
        if self.request_timeout_s is not None and self.request_timeout_s < 0:
            self.request_timeout_s = None

        self._node_id = node_id

        # Set the controller name so that serve connects to the
        # controller instance this proxy is running in.
        ray.serve.context._set_internal_replica_context(
            app_name=None,
            deployment=None,
            replica_tag=None,
            servable_object=None,
            controller_name=controller_name,
        )

        # Used only for displaying the route table.
        self.route_info: Dict[str, EndpointTag] = dict()

        self.self_actor_handle = proxy_actor or ray.get_runtime_context().current_actor
        self.asgi_receive_queues: Dict[str, ASGIMessageQueue] = dict()

        if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING:
            logger.info(
                "Experimental streaming feature flag enabled.",
                extra={"log_to_stderr": False},
            )

        def get_handle(deployment_name, app_name):
            # Delayed import due to circular dependency.
            # TODO(edoakes): use `get_deployment_handle` public API instead.
            from ray.serve.context import _get_global_client

            return _get_global_client().get_handle(
                deployment_name,
                app_name,
                sync=False,
                missing_ok=True,
            )

        self.proxy_router = proxy_router_class(get_handle, self.protocol)
        self.long_poll_client = LongPollClient(
            controller_actor
            or ray.get_actor(controller_name, namespace=SERVE_NAMESPACE),
            {
                LongPollNamespace.ROUTE_TABLE: self._update_routes,
            },
            call_in_event_loop=get_or_create_event_loop(),
        )
        self.request_counter = metrics.Counter(
            f"serve_num_{self.protocol.lower()}_requests",
            description=f"The number of {self.protocol} requests processed.",
            tag_keys=("route", "method", "application", "status_code"),
        )

        self.request_error_counter = metrics.Counter(
            f"serve_num_{self.protocol.lower()}_error_requests",
            description=(
                f"The number of non-{self.success_status_code} "
                f"{self.protocol} responses."
            ),
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
                f"The number of non-{self.success_status_code} {self.protocol} "
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

        # `self._prevent_node_downscale_ref` is used to prevent the node from being
        # downscaled when there are ongoing requests
        self._prevent_node_downscale_ref = ray.put("prevent_node_downscale_object")
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

    @property
    @abstractmethod
    def success_status_code(self) -> str:
        """Success status code for the proxy.

        Each proxy needs to define its success code.
        """
        raise NotImplementedError

    def _is_draining(self) -> bool:
        """Whether is proxy actor is in the draining status or not."""
        return self._draining_start_time is not None

    def _update_routes(self, endpoints: Dict[EndpointTag, EndpointInfo]) -> None:
        self.route_info: Dict[str, EndpointTag] = dict()
        for endpoint, info in endpoints.items():
            route = info.route
            self.route_info[route] = endpoint

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
    async def not_found(self, proxy_request: ProxyRequest) -> ProxyResponse:
        raise NotImplementedError

    @abstractmethod
    async def draining_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        raise NotImplementedError

    @abstractmethod
    async def timeout_response(
        self, proxy_request: ProxyRequest, request_id: str
    ) -> ProxyResponse:
        raise NotImplementedError

    @abstractmethod
    async def routes_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        raise NotImplementedError

    @abstractmethod
    async def health_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
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

    async def proxy_request(self, proxy_request: ProxyRequest) -> ProxyResponse:
        """Wrapper for proxy request.

        This method is served as common entry point by the proxy. It handles the
        routing, including routes and health checks, ongoing request counter,
        and metrics.
        """
        assert proxy_request.request_type in {"http", "websocket", "grpc"}

        method = proxy_request.method

        # only use the non-root part of the path for routing
        route_path = proxy_request.route_path

        if proxy_request.is_route_request:
            if self._is_draining():
                return await self.draining_response(proxy_request=proxy_request)

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": "",
                    "status_code": self.success_status_code,
                }
            )
            return await self.routes_response(proxy_request=proxy_request)

        if proxy_request.is_health_request:
            if self._is_draining():
                return await self.draining_response(proxy_request=proxy_request)

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": "",
                    "status_code": self.success_status_code,
                }
            )
            return await self.health_response(proxy_request=proxy_request)

        try:
            self._ongoing_requests_start()

            matched_route = None
            if self.protocol == RequestProtocol.HTTP:
                matched_route = self.proxy_router.match_route(route_path)
            elif self.protocol == RequestProtocol.GRPC:
                matched_route = self.proxy_router.get_handle_for_endpoint(route_path)

            if matched_route is None:
                proxy_response = await self.not_found(proxy_request=proxy_request)
                self.request_error_counter.inc(
                    tags={
                        "route": route_path,
                        "error_code": proxy_response.status_code,
                        "method": method,
                        "application": "",
                    }
                )
                self.request_counter.inc(
                    tags={
                        "route": route_path,
                        "method": method,
                        "application": "",
                        "status_code": proxy_response.status_code,
                    }
                )
                return proxy_response

            route_prefix, handle, app_is_cross_language = matched_route

            # Modify the path and root path so that reverse lookups and redirection
            # work as expected. We do this here instead of in replicas so it can be
            # changed without restarting the replicas.
            if route_prefix != "/" and self.protocol == RequestProtocol.HTTP:
                assert not route_prefix.endswith("/")
                proxy_request.set_path(route_path.replace(route_prefix, "", 1))
                proxy_request.set_root_path(proxy_request.root_path + route_prefix)

            start_time = time.time()
            handle, request_id = self.setup_request_context_and_handle(
                app_name=handle.deployment_id.app,
                handle=handle,
                route_path=route_path,
                proxy_request=proxy_request,
            )

            # Streaming codepath isn't supported for Java.
            if RAY_SERVE_ENABLE_EXPERIMENTAL_STREAMING and not app_is_cross_language:
                proxy_response = await self.send_request_to_replica_streaming(
                    request_id=request_id,
                    handle=handle,
                    proxy_request=proxy_request,
                )
            else:
                proxy_response = await self.send_request_to_replica_unary(
                    handle=handle,
                    proxy_request=proxy_request,
                )

            self.request_counter.inc(
                tags={
                    "route": route_path,
                    "method": method,
                    "application": handle.deployment_id.app,
                    "status_code": proxy_response.status_code,
                }
            )

            latency_ms = (time.time() - start_time) * 1000.0
            self.processing_latency_tracker.observe(
                latency_ms,
                tags={
                    "method": method,
                    "route": route_path,
                    "application": handle.deployment_id.app,
                    "status_code": proxy_response.status_code,
                },
            )
            logger.info(
                access_log_msg(
                    method=method,
                    status=str(proxy_response.status_code),
                    latency_ms=latency_ms,
                ),
                extra={"log_to_stderr": False},
            )
            if proxy_response.status_code != self.success_status_code:
                self.request_error_counter.inc(
                    tags={
                        "route": route_path,
                        "error_code": proxy_response.status_code,
                        "method": method,
                        "application": handle.deployment_id.app,
                    }
                )
                self.deployment_request_error_counter.inc(
                    tags={
                        "deployment": handle.deployment_id.name,
                        "error_code": proxy_response.status_code,
                        "method": method,
                        "route": route_path,
                        "application": handle.deployment_id.app,
                    }
                )
        finally:
            # If anything during the request failed, we still want to ensure the ongoing
            # request counter is decremented and possibly reset the keep alive object.
            self._ongoing_requests_end()

        return proxy_response

    async def _assign_request_with_timeout(
        self,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
        disconnected_task: Optional[asyncio.Task] = None,
        timeout_s: Optional[float] = None,
    ) -> Union[None, ray.ObjectRef, StreamingObjectRefGenerator]:
        """Attempt to send a request on the handle within the timeout.

        If `timeout_s` is exceeded while trying to assign a replica, `TimeoutError`
        will be raised.

        `disconnected_task` is expected to be done if the client disconnects; in this
        case, we will abort assigning a replica and return `None`.
        """
        result: _DeploymentResponseBase = handle.remote(
            proxy_request.request_object(proxy_handle=self.self_actor_handle)
        )
        to_object_ref_task = asyncio.ensure_future(
            result._to_object_ref_or_gen(_record_telemetry=False)
        )
        tasks = [to_object_ref_task]
        if disconnected_task is not None:
            tasks.append(disconnected_task)
        done, _ = await asyncio.wait(
            tasks,
            return_when=FIRST_COMPLETED,
            timeout=timeout_s,
        )
        if to_object_ref_task in done:
            return to_object_ref_task.result()
        elif disconnected_task is not None and disconnected_task in done:
            result.cancel()
            return None
        else:
            result.cancel()
            raise TimeoutError()

    @abstractmethod
    async def send_request_to_replica_unary(
        self,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
    ) -> ProxyResponse:
        """Send the request to the replica and handle unary response.

        Each proxy needs to implement its own logic for sending the request and
        handling the unary response.
        """
        raise NotImplementedError

    @abstractmethod
    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: DeploymentHandle,
        route_path: str,
        proxy_request: ProxyRequest,
    ) -> Tuple[DeploymentHandle, str]:
        """Setup the request context and handle for the request.

        Each proxy needs to implement its own logic for setting up the request context
        and handle.
        """
        raise NotImplementedError

    @abstractmethod
    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
    ) -> ProxyResponse:
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

    @property
    def success_status_code(self) -> str:
        return str(grpc.StatusCode.OK)

    async def not_found(self, proxy_request: ProxyRequest) -> ProxyResponse:
        if not proxy_request.app_name:
            application_message = "Application metadata not set."
        else:
            application_message = f"Application '{proxy_request.app_name}' not found."
        not_found_message = (
            f"{application_message} Please ping "
            "/ray.serve.RayServeAPIService/ListApplications for available applications."
        )
        status_code = grpc.StatusCode.NOT_FOUND
        proxy_request.send_status_code(status_code=status_code)
        proxy_request.send_details(message=not_found_message)
        return ProxyResponse(status_code=str(status_code))

    async def draining_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        status_code = grpc.StatusCode.UNAVAILABLE
        proxy_request.send_status_code(status_code=status_code)
        proxy_request.send_details(message=DRAINED_MESSAGE)
        if proxy_request.is_route_request:
            application_names = [endpoint.app for endpoint in self.route_info.values()]
            response_proto = ListApplicationsResponse(
                application_names=application_names
            )
        else:
            response_proto = HealthzResponse(message=DRAINED_MESSAGE)
        return ProxyResponse(
            status_code=str(status_code),
            response=response_proto.SerializeToString(),
        )

    async def timeout_response(
        self, proxy_request: ProxyRequest, request_id: str
    ) -> ProxyResponse:
        timeout_message = (
            f"Request {request_id} timed out after {self.request_timeout_s}s."
        )
        status_code = grpc.StatusCode.CANCELLED
        proxy_request.send_status_code(status_code=status_code)
        proxy_request.send_details(message=timeout_message)
        return ProxyResponse(status_code=str(status_code))

    async def routes_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        status_code = grpc.StatusCode.OK
        proxy_request.send_status_code(status_code=status_code)
        proxy_request.send_details(message=HEALTH_CHECK_SUCCESS_MESSAGE)
        application_names = [endpoint.app for endpoint in self.route_info.values()]
        response_proto = ListApplicationsResponse(
            application_names=application_names,
        )
        return ProxyResponse(
            status_code=str(status_code),
            response=response_proto.SerializeToString(),
        )

    async def health_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        status_code = grpc.StatusCode.OK
        proxy_request.send_status_code(status_code=status_code)
        proxy_request.send_details(message=HEALTH_CHECK_SUCCESS_MESSAGE)
        response_proto = HealthzResponse(message=HEALTH_CHECK_SUCCESS_MESSAGE)
        return ProxyResponse(
            status_code=str(status_code),
            response=response_proto.SerializeToString(),
        )

    def _set_internal_error_response(
        self, proxy_request: ProxyRequest, error: Exception
    ) -> ProxyResponse:
        status_code = grpc.StatusCode.INTERNAL
        proxy_request.send_status_code(status_code=status_code)
        proxy_request.send_details(message=str(error))
        return ProxyResponse(status_code=str(status_code))

    def service_handler_factory(self, service_method: str, stream: bool) -> Callable:
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
            proxy_response = await self.proxy_request(proxy_request=proxy_request)
            return proxy_response.response

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
            proxy_response = await self.proxy_request(proxy_request=proxy_request)
            async for response in proxy_response.streaming_response:
                yield response

        if not stream:
            return unary_unary
        return unary_stream

    async def send_request_to_replica_unary(
        self,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
    ) -> ProxyResponse:
        return await self.send_request_to_replica_streaming(
            request_id=proxy_request.request_id,
            handle=handle,
            proxy_request=proxy_request,
        )

    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: DeploymentHandle,
        route_path: str,
        proxy_request: ProxyRequest,
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
            "app_name": app_name,
            "multiplexed_model_id": multiplexed_model_id,
        }
        ray.serve.context._serve_request_context.set(
            ray.serve.context._RequestContext(**request_context_info)
        )
        proxy_request.send_request_id(request_id=request_id)
        return handle, request_id

    async def _streaming_generator_helper(
        self,
        obj_ref_generator: StreamingObjectRefGenerator,
        proxy_request: ProxyRequest,
        request_id: str,
        timeout_s: Optional[float] = None,
    ) -> Generator[bytes, None, None]:
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
                    await self.timeout_response(
                        proxy_request=proxy_request, request_id=request_id
                    )
                    break

                user_response_bytes = await obj_ref
                yield user_response_bytes

            except StopAsyncIteration:
                break
            except Exception as e:
                self._set_internal_error_response(proxy_request, e)
                break

    async def _consume_generator_stream(
        self,
        obj_ref: StreamingObjectRefGenerator,
        proxy_request: ProxyRequest,
        request_id: str,
        timeout_s: Optional[float] = None,
    ) -> ProxyResponse:
        streaming_response = self._streaming_generator_helper(
            obj_ref_generator=obj_ref,
            proxy_request=proxy_request,
            request_id=request_id,
            timeout_s=timeout_s,
        )

        return ProxyResponse(
            status_code=self.success_status_code, streaming_response=streaming_response
        )

    async def _consume_generator_unary(
        self,
        obj_ref: ray.ObjectRef,
        timeout_s: Optional[float] = None,
    ) -> ProxyResponse:
        try:
            user_response_bytes = await asyncio.wait_for(obj_ref, timeout=timeout_s)
            return ProxyResponse(
                status_code=self.success_status_code, response=user_response_bytes
            )
        except asyncio.exceptions.TimeoutError:
            raise TimeoutError() from None

    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
    ) -> ProxyResponse:
        start = time.time()
        try:
            obj_ref = None
            try:
                obj_ref = await self._assign_request_with_timeout(
                    handle=handle,
                    proxy_request=proxy_request,
                    timeout_s=self.request_timeout_s,
                )
                if obj_ref is None:
                    logger.info(
                        f"Client from {proxy_request.client} disconnected, "
                        "cancelling the request.",
                        extra={"log_to_stderr": False},
                    )
                    return ProxyResponse(status_code=DISCONNECT_ERROR_CODE)
                if proxy_request.stream:
                    return await self._consume_generator_stream(
                        obj_ref=obj_ref,
                        proxy_request=proxy_request,
                        request_id=request_id,
                        timeout_s=calculate_remaining_timeout(
                            timeout_s=self.request_timeout_s,
                            start_time_s=start,
                            curr_time_s=time.time(),
                        ),
                    )
                else:
                    return await self._consume_generator_unary(
                        obj_ref=obj_ref,
                        timeout_s=calculate_remaining_timeout(
                            timeout_s=self.request_timeout_s,
                            start_time_s=start,
                            curr_time_s=time.time(),
                        ),
                    )
            except TimeoutError:
                logger.warning(
                    f"Request {request_id} timed out after {self.request_timeout_s}s."
                )
                if obj_ref is not None:
                    ray.cancel(obj_ref)
                await self.timeout_response(
                    proxy_request=proxy_request, request_id=request_id
                )
                return ProxyResponse(status_code=TIMEOUT_ERROR_CODE)

        except Exception as e:
            logger.exception(e)
            return self._set_internal_error_response(proxy_request, e)


class HTTPProxy(GenericProxy):
    """This class is meant to be instantiated and run by an ASGI HTTP server.

    >>> import uvicorn
    >>> controller_name = ... # doctest: +SKIP
    >>> uvicorn.run(HTTPProxy(controller_name)) # doctest: +SKIP
    """

    @property
    def protocol(self) -> RequestProtocol:
        return RequestProtocol.HTTP

    @property
    def success_status_code(self) -> str:
        return "200"

    async def not_found(self, proxy_request: ProxyRequest) -> ProxyResponse:
        status_code = 404
        current_path = proxy_request.path
        response = Response(
            f"Path '{current_path}' not found. "
            "Please ping http://.../-/routes for route table.",
            status_code=status_code,
        )
        await response.send(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )
        return ProxyResponse(status_code=str(status_code))

    async def draining_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        status_code = 503
        response = Response(DRAINED_MESSAGE, status_code=status_code)
        await response.send(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )
        return ProxyResponse(status_code=str(status_code))

    async def timeout_response(
        self, proxy_request: ProxyRequest, request_id: str
    ) -> ProxyResponse:
        status_code = 408
        response = Response(
            f"Request {request_id} timed out after {self.request_timeout_s}s.",
            status_code=status_code,
        )
        await response.send(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )
        return ProxyResponse(status_code=str(status_code))

    async def routes_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        resp = dict()
        for route, endpoint in self.route_info.items():
            # For 2.x deployments, return {route -> app name}
            if endpoint.app:
                resp[route] = endpoint.app
            # Keep compatibility with 1.x deployments: return {route -> deployment name}
            else:
                resp[route] = endpoint.name

        await starlette.responses.JSONResponse(resp)(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )
        return ProxyResponse(status_code=self.success_status_code)

    async def health_response(self, proxy_request: ProxyRequest) -> ProxyResponse:
        await starlette.responses.PlainTextResponse(HEALTH_CHECK_SUCCESS_MESSAGE)(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )
        return ProxyResponse(status_code=self.success_status_code)

    async def receive_asgi_messages(self, request_id: str) -> List[Message]:
        queue = self.asgi_receive_queues.get(request_id, None)
        if queue is None:
            raise KeyError(f"Request ID {request_id} not found.")

        await queue.wait_for_message()
        return queue.get_messages_nowait()

    async def __call__(self, scope, receive, send):
        """Implements the ASGI protocol.

        See details at:
            https://asgi.readthedocs.io/en/latest/specs/index.html.
        """
        proxy_request = ASGIProxyRequest(scope=scope, receive=receive, send=send)
        await self.proxy_request(proxy_request=proxy_request)

    async def send_request_to_replica_unary(
        self,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
    ) -> ProxyResponse:
        http_body_bytes = await receive_http_body(
            proxy_request.scope, proxy_request.receive, proxy_request.send
        )

        # NOTE(edoakes): it's important that we defer building the starlette
        # request until it reaches the replica to avoid unnecessary
        # serialization cost, so we use a simple dataclass here.
        request = HTTPRequestWrapper(proxy_request.scope, http_body_bytes)

        # Perform a pickle here to improve latency. Stdlib pickle for simple
        # dataclasses are 10-100x faster than cloudpickle.
        request = pickle.dumps(request)

        retries = 0
        loop = get_or_create_event_loop()
        # We have received all the http request content. The next `receive`
        # call might never arrive; if it does, it can only be `http.disconnect`.
        while retries < HTTP_REQUEST_MAX_RETRIES + 1:
            should_backoff = False
            result_ref = handle.remote(request)
            client_disconnection_task = loop.create_task(proxy_request.receive())
            done, _ = await asyncio.wait(
                [
                    result_ref._to_object_ref(_record_telemetry=False),
                    client_disconnection_task,
                ],
                return_when=FIRST_COMPLETED,
            )
            if client_disconnection_task in done:
                message = await client_disconnection_task
                assert message["type"] == "http.disconnect", (
                    "Received additional request payload that's not disconnect. "
                    "This is an invalid HTTP state."
                )
                logger.warning(
                    f"Client from {proxy_request.client} disconnected, cancelling the "
                    "request.",
                    extra={"log_to_stderr": False},
                )
                result_ref.cancel()

            try:
                # NOTE (shrekris-anyscale): when the gcs, Serve controller, and
                # some replicas crash simultaneously (e.g. if the head node crashes),
                # requests to the dead replicas hang until the gcs recovers.
                # This asyncio.wait can kill those hanging requests and retry them
                # at another replica. Release tests should kill the head node and
                # check if latency drops significantly. See
                # https://github.com/ray-project/ray/pull/29534 for more info.
                done, pending = await asyncio.wait(
                    [result_ref, client_disconnection_task],
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=self.request_timeout_s,
                )
                if client_disconnection_task in done:
                    logger.info("Client disconnected, cancelling request.")
                    result_ref.cancel()
                    raise asyncio.CancelledError()
                elif len(done) == 0:
                    logger.info(
                        f"Request didn't finish within {self.request_timeout_s} seconds"
                        ". Retrying with another replica. You can modify this timeout "
                        'by setting "request_timeout_s" in your Serve config\'s '
                        "`http_options` field."
                    )
                    should_backoff = True
                    result_ref.cancel()
                else:
                    result = await result_ref
                    break
            except asyncio.CancelledError:
                # Here because the client disconnected, we will return a custom
                # error code for metric tracking.
                return ProxyResponse(status_code=DISCONNECT_ERROR_CODE)
            except RayTaskError as e:
                error_message = f"Unexpected error, traceback: {e}."
                await Response(error_message, status_code=500).send(
                    proxy_request.scope, proxy_request.receive, proxy_request.send
                )
                return ProxyResponse(status_code="500")
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
            await Response(error_message, status_code=500).send(
                proxy_request.scope, proxy_request.receive, proxy_request.send
            )
            return ProxyResponse(status_code="500")

        if isinstance(result, (starlette.responses.Response, RawASGIResponse)):
            await result(proxy_request.scope, proxy_request.receive, proxy_request.send)
            return ProxyResponse(status_code=str(result.status_code))
        else:
            await Response(result).send(
                proxy_request.scope, proxy_request.receive, proxy_request.send
            )
            return ProxyResponse(status_code=self.success_status_code)

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
            # Close the queue so any subsequent calls to fetch messages return
            # immediately: https://github.com/ray-project/ray/issues/38368.
            queue.close()

    async def _consume_and_send_asgi_message_generator(
        self,
        obj_ref_generator: StreamingObjectRefGenerator,
        disconnected_task: asyncio.Task,
        request_id: str,
        proxy_request: ProxyRequest,
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
        response_done = False
        response_started = False
        expecting_trailers = False
        is_websocket_connection = False
        while True:
            try:
                next_obj_ref_task = asyncio.ensure_future(
                    obj_ref_generator._next_async(),
                )
                tasks = [next_obj_ref_task]
                # Once the response is completed, the client will immediately
                # disconnect. This causes a race condition: if we get the disconnect
                # message before the replica handler finishes, we'll falsely detect a
                # client disconnect. To avoid this, stop listening for disconnects once
                # the full response has been sent.
                if not response_done:
                    tasks.append(disconnected_task)

                done, _ = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=calculate_remaining_timeout(
                        timeout_s=timeout_s,
                        start_time_s=start,
                        curr_time_s=time.time(),
                    ),
                )
                if len(done) == 0:
                    logger.warning(
                        f"Request {request_id} timed out after "
                        f"{self.request_timeout_s}s while executing."
                    )
                    next_obj_ref_task.cancel()
                    ray.cancel(obj_ref_generator)

                    # We should only send timeout response if we have not sent
                    # any messages to the client yet. Header (including status code)
                    # messages can only be sent once.
                    if not response_started:
                        await self.timeout_response(
                            proxy_request=proxy_request, request_id=request_id
                        )

                    return TIMEOUT_ERROR_CODE
                elif disconnected_task in done:
                    logger.info(
                        f"Client for request {request_id} disconnected "
                        "during execution, cancelling request."
                    )
                    next_obj_ref_task.cancel()
                    if not is_websocket_connection:
                        # Websocket code explicitly handles client disconnects,
                        # so let the ASGI disconnect message propagate instead of
                        # cancelling the handler.
                        ray.cancel(obj_ref_generator)
                    return DISCONNECT_ERROR_CODE

                obj_ref = next_obj_ref_task.result()
                asgi_messages: List[Message] = pickle.loads(await obj_ref)
                # See the ASGI spec for message details:
                # https://asgi.readthedocs.io/en/latest/specs/www.html.
                for asgi_message in asgi_messages:
                    if asgi_message["type"] == "http.response.start":
                        # HTTP responses begin with exactly one
                        # "http.response.start" message containing the "status"
                        # field. Other response types (e.g., WebSockets) may not.
                        status_code = str(asgi_message["status"])
                        expecting_trailers = asgi_message.get("trailers", False)
                    elif asgi_message["type"] == "websocket.accept":
                        is_websocket_connection = True
                    elif (
                        asgi_message["type"] == "http.response.body"
                        and not asgi_message.get("more_body", False)
                        and not expecting_trailers
                    ):
                        # If the body is completed and we aren't expecting trailers, the
                        # response is done.
                        response_done = True
                    elif asgi_message["type"] == "http.response.trailers":
                        # If we are expecting trailers, the response is only done when
                        # the trailers message has been sent.
                        response_done = not asgi_message.get("more_trailers", False)
                    elif asgi_message["type"] == "websocket.disconnect":
                        status_code = str(asgi_message["code"])
                        response_done = True

                    await proxy_request.send(asgi_message)
                    response_started = True
            except StopAsyncIteration:
                break

        return status_code

    def setup_request_context_and_handle(
        self,
        app_name: str,
        handle: DeploymentHandle,
        route_path: str,
        proxy_request: ProxyRequest,
    ) -> Tuple[DeploymentHandle, str]:
        """Setup request context and handle for the request.

        Unpack HTTP request headers and extract info to set up request context and
        handle.
        """
        request_context_info = {
            "route": route_path,
            "app_name": app_name,
        }
        for key, value in proxy_request.headers:
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
            ray.serve.context._RequestContext(**request_context_info)
        )
        return handle, request_context_info["request_id"]

    async def send_request_to_replica_streaming(
        self,
        request_id: str,
        handle: DeploymentHandle,
        proxy_request: ProxyRequest,
    ) -> ProxyResponse:
        # Proxy the receive interface by placing the received messages on a queue.
        # The downstream replica must call back into `receive_asgi_messages` on this
        # actor to receive the messages.
        receive_queue = ASGIMessageQueue()
        self.asgi_receive_queues[request_id] = receive_queue
        proxy_asgi_receive_task = get_or_create_event_loop().create_task(
            self.proxy_asgi_receive(proxy_request.receive, receive_queue)
        )

        status_code = ""
        start = time.time()
        try:
            obj_ref_generator = None
            try:
                obj_ref_generator = await self._assign_request_with_timeout(
                    handle=handle,
                    proxy_request=proxy_request,
                    disconnected_task=proxy_asgi_receive_task,
                    timeout_s=self.request_timeout_s,
                )
                if obj_ref_generator is None:
                    logger.info(
                        f"Client from {proxy_request.client} disconnected, "
                        "cancelling the request.",
                        extra={"log_to_stderr": False},
                    )
                    return ProxyResponse(status_code=DISCONNECT_ERROR_CODE)
            except TimeoutError:
                logger.warning(
                    f"Request {request_id} timed out after {self.request_timeout_s}s."
                )
                if obj_ref_generator is not None:
                    ray.cancel(obj_ref_generator)
                await self.timeout_response(
                    proxy_request=proxy_request, request_id=request_id
                )
                return ProxyResponse(status_code=TIMEOUT_ERROR_CODE)

            status_code = await self._consume_and_send_asgi_message_generator(
                obj_ref_generator,
                disconnected_task=proxy_asgi_receive_task,
                request_id=request_id,
                proxy_request=proxy_request,
                timeout_s=calculate_remaining_timeout(
                    timeout_s=self.request_timeout_s,
                    start_time_s=start,
                    curr_time_s=time.time(),
                ),
            )

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
                    and proxy_request.request_type == "websocket"
                    and proxy_asgi_receive_task.exception() is None
                ):
                    status_code = str(proxy_asgi_receive_task.result())

            del self.asgi_receive_queues[request_id]

        return ProxyResponse(status_code=status_code)


class RequestIdMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        headers = MutableHeaders(scope=scope)
        if RAY_SERVE_REQUEST_ID_HEADER not in headers and "x-request-id" not in headers:
            # If X-Request-ID and RAY_SERVE_REQUEST_ID_HEADER are both not set, we
            # generate a new request ID.
            request_id = generate_request_id()
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
        keep_alive_timeout_s: int = DEFAULT_UVICORN_KEEP_ALIVE_TIMEOUT_S,
        grpc_options: Optional[gRPCOptions] = None,
    ):  # noqa: F821
        self.grpc_options = grpc_options or gRPCOptions()
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
        self.cpu_profiler, self.cpu_profiler_log = configure_component_cpu_profiler(
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
        self.grpc_port = self.grpc_options.port
        self.root_path = root_path
        self.keep_alive_timeout_s = (
            RAY_SERVE_HTTP_KEEP_ALIVE_TIMEOUT_S or keep_alive_timeout_s
        )
        self._uvicorn_server = None

        self.http_setup_complete = asyncio.Event()
        self.grpc_setup_complete = asyncio.Event()

        self.http_proxy = HTTPProxy(
            controller_name=controller_name,
            node_id=node_id,
            node_ip_address=node_ip_address,
            proxy_router_class=LongestPrefixRouter,
            request_timeout_s=(
                request_timeout_s or RAY_SERVE_REQUEST_PROCESSING_TIMEOUT_S
            ),
        )
        self.grpc_proxy = (
            gRPCProxy(
                controller_name=controller_name,
                node_id=node_id,
                node_ip_address=node_ip_address,
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
            self.wrapped_http_proxy = middleware.cls(
                self.wrapped_http_proxy, **middleware.options
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
            timeout_keep_alive=self.keep_alive_timeout_s,
        )
        self._uvicorn_server = uvicorn.Server(config=config)
        # TODO(edoakes): we need to override install_signal_handlers here
        # because the existing implementation fails if it isn't running in
        # the main thread and uvicorn doesn't expose a way to configure it.
        self._uvicorn_server.install_signal_handlers = lambda: None

        logger.info(
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
        logger.info(
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

        pass

    async def receive_asgi_messages(self, request_id: str) -> bytes:
        """Get ASGI messages for the provided `request_id`.

        After the proxy has stopped receiving messages for this `request_id`,
        this will always return immediately.
        """
        return pickle.dumps(await self.http_proxy.receive_asgi_messages(request_id))

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
