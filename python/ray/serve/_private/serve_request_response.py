import logging

from starlette.types import Receive, Scope, Send
from typing import List, Generator, Optional, Tuple

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.generated import serve_pb2


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
        self, request: serve_pb2.RayServeRequest, route_path: str, stream: bool
    ):
        self.request = request
        self.route_path = route_path
        self.stream = stream

    @property
    def user_request(self) -> bytes:
        return self.request.user_request

    @property
    def request_type(self) -> str:
        return "grpc"

    @property
    def method(self) -> str:
        return "GRPC"

    @property
    def multiplexed_model_id(self) -> str:
        return self.request.multiplexed_model_id

    @property
    def request_id(self) -> str:
        return self.request.request_id

    def set_request_id(self, request_id: str):
        self.request.request_id = request_id


class ServeResponse:
    def __init__(
        self,
        status_code: str,
        response: Optional[serve_pb2.RayServeResponse] = None,
        streaming_response: Optional[
            Generator[serve_pb2.RayServeResponse, None, None]
        ] = None,
    ):
        self.status_code = status_code
        self.response = response
        self.streaming_response = streaming_response
