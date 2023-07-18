import logging

from starlette.types import Receive, Scope, Send
from typing import List, Tuple

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.generated import serve_pb2

from google.protobuf.any_pb2 import Any as ProtoAny


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
    def __init__(self, request: serve_pb2.RayServeRequest):
        self.request = request

    @property
    def user_request(self) -> ProtoAny:
        return self.request.user_request
