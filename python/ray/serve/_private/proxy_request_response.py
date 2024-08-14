import logging
import pickle
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, List, Tuple, Union

import grpc
from starlette.types import Receive, Scope, Send

from ray.serve._private.common import StreamingHTTPRequest, gRPCRequest
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import DEFAULT
from ray.serve.grpc_util import RayServegRPCContext

logger = logging.getLogger(SERVE_LOGGER_NAME)


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
        return self.scope.get("method", "websocket").upper()

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

    def request_object(
        self, receive_asgi_messages: Callable[[str], Awaitable[bytes]]
    ) -> StreamingHTTPRequest:
        return StreamingHTTPRequest(
            pickled_asgi_scope=pickle.dumps(self.scope),
            receive_asgi_messages=receive_asgi_messages,
        )


class gRPCProxyRequest(ProxyRequest):
    """ProxyRequest implementation to wrap gRPC request protobuf and metadata."""

    def __init__(
        self,
        request_proto: Any,
        context: grpc._cython.cygrpc._ServicerContext,
        service_method: str,
        stream: bool,
    ):
        self.request = request_proto
        self.context = context
        self.service_method = service_method
        self.stream = stream
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
            self.request = pickle.dumps(self.request)
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
    def user_request(self) -> bytes:
        return self.request

    def send_request_id(self, request_id: str):
        # Setting the trailing metadata on the ray_serve_grpc_context object, so it's
        # not overriding the ones set from the user and will be sent back to the
        # client altogether.
        self.ray_serve_grpc_context.set_trailing_metadata([("request_id", request_id)])

    def request_object(self) -> gRPCRequest:
        return gRPCRequest(
            grpc_user_request=self.user_request,
        )


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
