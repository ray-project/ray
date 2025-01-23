from dataclasses import dataclass
from typing import Union

"""
Child bound messages.
"""


@dataclass
class RequestMessage:
    # Request ID. Must be unique for each Module process.
    request_id: int
    # Name of the Module method to call, not the REST method name.
    method_name: str
    # aiohttp.web.Request is explicitly not serializable, so we use bytes instead.
    # TODO(ryw): add headers if needed
    body: bytes


# Now it only contains RequestMessage. If later we need to add more messages, use Union.
ChildBoundMessage = RequestMessage

"""
Parent bound messages.
"""


@dataclass
class UnaryResponseMessage:
    request_id: int
    # aiohttp.web.Response is explicitly not serializable, so we use bytes instead.
    status: int
    # TODO(ryw): add headers if needed
    # headers: Dict[str, str]
    body: bytes


@dataclass
class StreamResponseStartMessage:
    # TODO(ryw): if needed, add header: Dict[str, str]
    request_id: int
    body: bytes


@dataclass
class StreamResponseDataMessage:
    request_id: int
    body: bytes


@dataclass
class StreamResponseEndMessage:
    request_id: int


@dataclass
class ErrorMessage:
    request_id: int
    # Will be raised in the parent's aiohttp handler coroutine.
    # Must be serializable.
    error: Exception


ParentBoundMessage = Union[
    UnaryResponseMessage,
    StreamResponseStartMessage,
    StreamResponseDataMessage,
    StreamResponseEndMessage,
    ErrorMessage,
]
