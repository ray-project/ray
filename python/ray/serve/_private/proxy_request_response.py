import logging
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncIterator, List, Optional, Tuple, Union

import grpc
from starlette.types import Receive, Scope, Send

from ray.serve._private.common import StreamingHTTPRequest, gRPCRequest
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.tracing_utils import (
    extract_propagated_context,
    is_tracing_enabled,
    set_trace_context,
)
from ray.serve._private.utils import DEFAULT
from ray.serve.grpc_util import RayServegRPCContext

logger = logging.getLogger(SERVE_LOGGER_NAME)


class gRPCStreamingType(str, Enum):
    """Enum representing the gRPC streaming type."""

    UNARY_UNARY = "unary_unary"  # Single request, single response
    UNARY_STREAM = (
        "unary_stream"  # Single request, streaming response (server streaming)
    )
    STREAM_UNARY = (
        "stream_unary"  # Streaming request, single response (client streaming)
    )
    STREAM_STREAM = (
        "stream_stream"  # Streaming request, streaming response (bidirectional)
    )


class ProxyRequest(ABC):
    """Base ProxyRequest class to use in the common interface among proxies"""

    @property
    @abstractmethod
    def request_type(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def method(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def route_path(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_route_request(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def is_health_request(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def populate_tracing_context(self):
        """Implement this method to populate tracing context so the parent and
        child spans will be connected into a single trace."""
        raise NotImplementedError


class ASGIProxyRequest(ProxyRequest):
    """ProxyRequest implementation to wrap ASGI scope, receive, and send."""

    def __init__(self, scope: Scope, receive: Receive, send: Send):
        self.scope = scope
        self.receive = receive
        self.send = send

    @property
    def request_type(self) -> str:
        return self.scope.get("type", "")

    @property
    def method(self) -> str:
        # WebSocket messages don't have a 'method' field.
        return self.scope.get("method", "WS").upper()

    @property
    def route_path(self) -> str:
        return self.scope.get("path", "")[len(self.root_path) :]

    @property
    def is_route_request(self) -> bool:
        return self.route_path == "/-/routes"

    @property
    def is_health_request(self) -> bool:
        return self.route_path == "/-/healthz"

    @property
    def client(self) -> str:
        return self.scope.get("client", "")

    @property
    def root_path(self) -> str:
        return self.scope.get("root_path", "")

    @property
    def path(self) -> str:
        return self.scope.get("path", "")

    @property
    def headers(self) -> List[Tuple[bytes, bytes]]:
        return self.scope.get("headers", [])

    def set_path(self, path: str):
        self.scope["path"] = path

    def set_root_path(self, root_path: str):
        self.scope["root_path"] = root_path

    def serialized_replica_arg(self, proxy_actor_name: str) -> bytes:
        # NOTE(edoakes): it's important that the request is sent as raw bytes to
        # skip the Ray cloudpickle serialization codepath for performance.
        return pickle.dumps(
            StreamingHTTPRequest(
                asgi_scope=self.scope,
                proxy_actor_name=proxy_actor_name,
            )
        )

    def populate_tracing_context(self):
        """Populate tracing context for ASGI requests.

        This method extracts the "traceparent" header from the request headers and sets
        the tracing context from it.
        """
        if not is_tracing_enabled():
            return

        for key, value in self.headers:
            if key.decode() == "traceparent":
                trace_context = extract_propagated_context(
                    {key.decode(): value.decode()}
                )
                set_trace_context(trace_context)


class gRPCProxyRequest(ProxyRequest):
    """ProxyRequest implementation to wrap gRPC request protobuf and metadata."""

    def __init__(
        self,
        request_proto: Any,
        context: grpc._cython.cygrpc._ServicerContext,
        service_method: str,
        stream: bool,
        *,
        streaming_type: gRPCStreamingType = None,
        request_iterator: Optional[AsyncIterator[Any]] = None,
    ):
        self._request_proto = request_proto
        self._request_iterator = request_iterator
        self.context = context
        self.service_method = service_method
        self.stream = stream
        # Determine streaming type based on parameters
        if streaming_type is not None:
            self.streaming_type = streaming_type
        elif request_iterator is not None:
            # Has input stream
            self.streaming_type = (
                gRPCStreamingType.STREAM_STREAM
                if stream
                else gRPCStreamingType.STREAM_UNARY
            )
        else:
            # No input stream
            self.streaming_type = (
                gRPCStreamingType.UNARY_STREAM
                if stream
                else gRPCStreamingType.UNARY_UNARY
            )
        self.app_name = ""
        self.request_id = None
        self.method_name = "__call__"
        self.multiplexed_model_id = DEFAULT.VALUE
        # ray_serve_grpc_context is a class implemented by us to be able to serialize
        # the object and pass it into the deployment.
        self.ray_serve_grpc_context = RayServegRPCContext(context)
        self.setup_variables()

    def setup_variables(self):
        if not self.is_route_request and not self.is_health_request:
            service_method_split = self.service_method.split("/")
            self.method_name = service_method_split[-1]
            for key, value in self.context.invocation_metadata():
                if key == "application":
                    self.app_name = value
                elif key == "request_id":
                    self.request_id = value
                elif key == "multiplexed_model_id":
                    self.multiplexed_model_id = value

    @property
    def request_type(self) -> str:
        return "grpc"

    @property
    def method(self) -> str:
        return self.service_method

    @property
    def route_path(self) -> str:
        return self.app_name

    @property
    def is_route_request(self) -> bool:
        return self.service_method == "/ray.serve.RayServeAPIService/ListApplications"

    @property
    def is_health_request(self) -> bool:
        return self.service_method == "/ray.serve.RayServeAPIService/Healthz"

    @property
    def has_input_stream(self) -> bool:
        """Returns True if this request has a streaming input (client/bidi streaming)."""
        return self.streaming_type in (
            gRPCStreamingType.STREAM_UNARY,
            gRPCStreamingType.STREAM_STREAM,
        )

    @property
    def request_iterator(self) -> Optional[AsyncIterator[Any]]:
        """Returns the request iterator for client/bidi streaming, or None."""
        return self._request_iterator

    def send_request_id(self, request_id: str):
        # Setting the trailing metadata on the ray_serve_grpc_context object, so it's
        # not overriding the ones set from the user and will be sent back to the
        # client altogether.
        self.ray_serve_grpc_context.set_trailing_metadata([("request_id", request_id)])

    def serialized_replica_arg(self) -> bytes:
        # NOTE(edoakes): it's important that the request is sent as raw bytes to
        # skip the Ray cloudpickle serialization codepath for performance.
        return pickle.dumps(gRPCRequest(user_request_proto=self._request_proto))

    def populate_tracing_context(self):
        """Populate tracing context for gRPC requests.

        This method extracts the "traceparent" metadata from the request headers and
        sets the tracing context from it.
        """
        if not is_tracing_enabled():
            return

        for key, value in self.context.invocation_metadata():
            if key == "traceparent":
                trace_context = extract_propagated_context({key: value})
                set_trace_context(trace_context)


@dataclass(frozen=True)
class ResponseStatus:
    code: Union[str, grpc.StatusCode]  # Must be convertible to a string.
    is_error: bool = False
    message: str = ""


# Yields protocol-specific messages followed by a final `ResponseStatus`.
ResponseGenerator = AsyncIterator[Union[Any, ResponseStatus]]


@dataclass(frozen=True)
class HandlerMetadata:
    application_name: str = ""
    deployment_name: str = ""
    route: str = ""


@dataclass(frozen=True)
class ResponseHandlerInfo:
    response_generator: ResponseGenerator
    metadata: HandlerMetadata
    should_record_access_log: bool
    should_increment_ongoing_requests: bool
