import grpc
import logging

from starlette.types import Receive, Scope, Send
from typing import Callable, List, Generator, Optional, Tuple

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.utils import DEFAULT
from google.protobuf.any_pb2 import Any as AnyProto

logger = logging.getLogger(SERVE_LOGGER_NAME)


class ServeRequest:
    pass


class ASGIServeRequest(ServeRequest):
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


class GRPCServeRequest(ServeRequest):
    def __init__(
        self,
        request: AnyProto,
        context: "grpc._cython.cygrpc._ServicerContext",
        match_target: Callable[[str], Optional[str]],
        stream: bool,
    ):
        # Any proto can be serialized by pickle so no need to call SerializeToString()
        self.request = request  # .SerializeToString()
        self.stream = stream
        self.app_name = None
        self.route_path = None
        self.request_id = None
        self.method_name = "__call__"
        self.multiplexed_model_id = DEFAULT.VALUE
        for key, value in context.invocation_metadata():
            if key == "application":
                self.app_name = value
            elif key == "request_id":
                self.request_id = value
            elif key == "multiplexed_model_id":
                self.multiplexed_model_id = value
            elif key == "route_path":
                self.route_path = value
            elif key == "method_name":
                self.method_name = value

        if not self.route_path:
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


class ServeResponse:
    def __init__(
        self,
        status_code: str,
        response: Optional[AnyProto] = None,
        streaming_response: Optional[Generator[AnyProto, None, None]] = None,
    ):
        self.status_code = status_code
        self.response = response
        self.streaming_response = streaming_response
