import logging

from starlette.types import Receive, Scope, Send

from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.generated import serve_pb2


logger = logging.getLogger(SERVE_LOGGER_NAME)


class RequestWrapper:
    pass


class ASGIRequestWrapper(RequestWrapper):
    def __init__(self, scope: Scope, receive: Receive, send: Send):
        self.scope = scope
        self.receive = receive
        self.send = send

    @property
    def request_type(self) -> str:
        return self.scope["type"]

    @property
    def client(self) -> str:
        return self.scope["client"]


class GRPCRequestWrapper(RequestWrapper):
    def __init__(self, request: serve_pb2.RayServeRequest):
        self.request = request
