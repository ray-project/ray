from dataclasses import dataclass
from typing import Dict, Union

"""
Child bound messages.
"""


@dataclass
class RequestMessage:
    # Request ID. Must be unique for each Module process.
    request_id: int
    # Name of the Module method to call, not the REST method name.
    method_name: str
    # aiohttp.web.Request is explicitly not serializable, so we pass these fields.
    http_method: str
    path_qs: str
    query: "multidict.MultiMapping[str, str]"  # noqa: F821
    headers: "multidict.MultiMapping[str, str]"  # noqa: F821
    body: bytes
    match_info: Dict[str, str]


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
