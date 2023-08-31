from abc import ABC, abstractmethod
import grpc
import logging
import pickle

from starlette.types import Receive, Scope, Send
from typing import Any, List, Generator, Optional, Tuple

from ray.actor import ActorHandle
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import DEFAULT
from ray.serve._private.common import gRPCRequest, StreamingHTTPRequest

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

    def request_object(self, proxy_handle) -> StreamingHTTPRequest:
        return StreamingHTTPRequest(
            pickled_asgi_scope=pickle.dumps(self.scope),
            http_proxy_handle=proxy_handle,
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
        self.context.set_trailing_metadata([("request_id", request_id)])

    def send_status_code(self, status_code: grpc.StatusCode):
        self.context.set_code(status_code)

    def send_details(self, message: str):
        self.context.set_details(message)

    def request_object(self, proxy_handle: ActorHandle) -> gRPCRequest:
        return gRPCRequest(
            grpc_user_request=self.user_request,
            grpc_proxy_handle=proxy_handle,
        )


class ProxyResponse:
    """ProxyResponse class to use in the common interface among proxies"""

    def __init__(
        self,
        status_code: str,
        response: Optional[bytes] = None,
        streaming_response: Optional[Generator[bytes, None, None]] = None,
    ):
        self.status_code = status_code
        self.response = response
        self.streaming_response = streaming_response
