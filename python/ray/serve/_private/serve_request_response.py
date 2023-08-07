import grpc
import logging
import pickle

from starlette.types import Receive, Scope, Send
from typing import Any, Callable, List, Generator, Optional, Tuple

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import DEFAULT

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ServeRequest:
    """Base ServeRequest class to use in the common interface among proxies"""

    pass


class ASGIServeRequest(ServeRequest):
    """ServeRequest implementation to wrap ASGI scope, receive, and send."""

    def __init__(self, scope: Scope, receive: Receive, send: Send):
        self.scope = scope
        self.receive = receive
        self.send = send

    @property
    def request_type(self) -> str:
        return self.scope.get("type", "")

    @property
    def client(self) -> str:
        return self.scope.get("client", "")

    @property
    def method(self) -> str:
        return self.scope.get("method", "websocket").upper()

    @property
    def root_path(self) -> str:
        return self.scope.get("root_path", "")

    @property
    def route_path(self) -> str:
        return self.scope.get("path", "")[len(self.root_path) :]

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


class gRPCServeRequest(ServeRequest):
    """ServeRequest implementation to wrap gRPC request protobuf and metadata."""

    def __init__(
        self,
        request_proto: Any,
        context: "grpc._cython.cygrpc._ServicerContext",
        match_target: Callable[[str], Optional[str]],
        service_method: str,
        stream: bool,
    ):
        service_method_split = service_method.split("/")
        self.request = pickle.dumps(request_proto)
        self.context = context
        self.stream = stream
        self.app_name = ""
        self.route_path = None
        self.request_id = None
        self.method_name = service_method_split[-1].lower()
        self.multiplexed_model_id = DEFAULT.VALUE
        for key, value in context.invocation_metadata():
            if key == "application":
                self.app_name = value
            elif key == "request_id":
                self.request_id = value
            elif key == "multiplexed_model_id":
                self.multiplexed_model_id = value
        self.route_path = match_target(self.app_name)

    @property
    def user_request(self) -> bytes:
        return self.request

    @property
    def request_type(self) -> str:
        return "grpc"

    @property
    def method(self) -> str:
        return "GRPC"

    def send_request_id(self, request_id: str):
        self.context.set_trailing_metadata([("request_id", request_id)])


class ServeResponse:
    """ServerResponse class to use in the common interface among proxies"""

    def __init__(
        self,
        status_code: str,
        response: Optional[bytes] = None,
        streaming_response: Optional[Generator[bytes, None, None]] = None,
    ):
        self.status_code = status_code
        self.response = response
        self.streaming_response = streaming_response
