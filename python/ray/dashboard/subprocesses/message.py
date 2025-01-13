from dataclasses import dataclass
from typing import Union

"""
Child bound messages.
"""


@dataclass
class RequestMessage:
    id: str
    # Name of the Module method to call, not the REST method name.
    method_name: str
    # aiohttp.web.Request is explicitly not serializable, so we use bytes instead.
    # TODO(ryw): add headers if needed
    body: bytes


@dataclass
class HealthCheckMessage:
    id: str


# Now it only contains RequestMessage. If later we need to add more messages, use Union.
ChildBoundMessage = RequestMessage

"""
Parent bound messages.
"""


@dataclass
class ResponseMessage:
    id: str
    # aiohttp.web.Response is explicitly not serializable, so we use bytes instead.
    status: int
    # TODO(ryw): add headers if needed
    # headers: Dict[str, str]
    body: bytes


@dataclass
class StreamResponseStartMessage:
    # TODO(ryw): if needed, add header: Dict[str, str]
    id: str
    body: bytes


@dataclass
class StreamResponseDataMessage:
    id: str
    body: bytes


@dataclass
class StreamResponseEndMessage:
    id: str


@dataclass
class ErrorMessage:
    id: str
    # Will be raised in the parent's aiohttp handler coroutine.
    # Must be serializable.
    error: Exception


ParentBoundMessage = Union[
    ResponseMessage,
    StreamResponseStartMessage,
    StreamResponseDataMessage,
    StreamResponseEndMessage,
    ErrorMessage,
]
