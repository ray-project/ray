import asyncio
import socket
from dataclasses import dataclass
import inspect
import json
import logging
import pickle
from typing import Any, List, Optional, Type

import starlette
from fastapi.encoders import jsonable_encoder
from starlette.types import ASGIApp, Message, Receive, Scope, Send
from uvicorn.config import Config
from uvicorn.lifespan.on import LifespanOn

from ray.actor import ActorHandle
from ray.serve.exceptions import RayServeException
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


class GRPCRequestWrapper(RequestWrapper):
    def __init__(self, request: serve_pb2.RayServeRequest):
        self.request = request

