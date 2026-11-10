"""The KV-aware P/D router's prefill call to a P replica, over ZMQ.

- PrefillRequest: What the router sends P, and its wire format.
- PrefillClient: The router side. Runs prefill on the P replica it selected.
- PrefillService: The P side. Serves prefill calls on a ZMQ endpoint the
  replica advertises in its ``replica_metadata``.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import msgpack
from pydantic import ValidationError

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorResponse,
)
from ray.llm._internal.serve.core.ingress.utils import _sanitize_chat_completion_request
from ray.llm._internal.serve.core.protocol import RawRequestInfo
from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    bind_request_lifecycle_owner,
)
from ray.llm._internal.serve.routing_policies.kv_aware.network_utils import (
    ZmqRpcClient,
    ZmqRpcServer,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_channel import (
    decode_prompt_token_ids,
    encode_prompt_token_ids,
)
from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S,
)

PDRequest = Union[ChatCompletionRequest, CompletionRequest]
# KV transfer params for D, or P's error for the client.
PrefillResult = Union[Dict[str, Any], ErrorResponse]

# P/D covers only chat and completions: PDDecodeServer and the KV connector
# backend handle only these request types. Other routes go to D, which serves
# them without P/D.
_PD_REQUEST_TYPES = {
    "/v1/chat/completions": ChatCompletionRequest,
    "/v1/completions": CompletionRequest,
}


def parse_pd_request(path: str, body: Optional[Dict[str, Any]]) -> Optional[PDRequest]:
    """The typed request P runs, or None when D serves the request alone."""
    request_cls = _PD_REQUEST_TYPES.get(path.split("?", 1)[0])
    if request_cls is None or body is None:
        return None
    try:
        request = request_cls.model_validate(body)
    except ValidationError:
        return None
    if isinstance(request, ChatCompletionRequest):
        request = _sanitize_chat_completion_request(request)
    return request


@dataclass
class PrefillRequest:
    """One prefill the router runs on the P replica it selected."""

    # The client's API path and request body, which P validates.
    path: str
    body: bytes
    # Prompt token IDs, so P skips tokenizing.
    token_ids: List[int]
    # The id P's tracker booked the selection under. P runs the request, and
    # reports its lifecycle events, under the same id.
    request_id: str
    # P's tracker's lifecycle endpoint, or None to report nothing.
    lifecycle_owner: Optional[str] = None
    # The LoRA adapter, or "" for the base model.
    multiplexed_model_id: str = ""
    headers: Dict[str, str] = field(default_factory=dict)

    def encode(self) -> bytes:
        return msgpack.packb(
            {
                "path": self.path,
                "body": self.body,
                "token_ids": encode_prompt_token_ids(self.token_ids),
                "request_id": self.request_id,
                "lifecycle_owner": self.lifecycle_owner,
                "multiplexed_model_id": self.multiplexed_model_id,
                "headers": self.headers,
            }
        )

    @classmethod
    def decode(cls, payload: bytes) -> "PrefillRequest":
        fields = msgpack.unpackb(payload)
        fields["token_ids"] = decode_prompt_token_ids(fields["token_ids"])
        return cls(**fields)


def _encode_result(result: PrefillResult) -> bytes:
    if isinstance(result, ErrorResponse):
        return msgpack.packb({"error": result.model_dump(mode="json")})
    return msgpack.packb({"kv_transfer_params": result})


def _decode_result(payload: bytes) -> PrefillResult:
    result = msgpack.unpackb(payload)
    if "error" in result:
        return ErrorResponse.model_validate(result["error"])
    return result["kv_transfer_params"]


class PrefillClient:
    """Runs prefill on P replicas for the P/D router.

    Raises ``ConnectionError`` when P dies mid-call, ``ZmqRpcError`` when P
    fails the call, and ``asyncio.TimeoutError`` past HAProxy's routing
    timeout, after which nobody awaits the decision. Cancelling a call cancels
    the prefill on P.
    """

    def __init__(
        self, timeout_s: float = RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_TIMEOUT_S
    ):
        self._rpc = ZmqRpcClient()
        self._timeout_s = timeout_s

    async def prefill(self, endpoint: str, request: PrefillRequest) -> PrefillResult:
        reply = await self._rpc.call(
            endpoint, request.encode(), timeout_s=self._timeout_s
        )
        return _decode_result(reply)

    async def close(self) -> None:
        await self._rpc.close()


class PrefillService:
    """Serves the P/D router's prefill calls on a P replica.

    Runs each request under a Serve request context with its request id and
    LoRA adapter, and has the engine report lifecycle events to its owner.
    """

    def __init__(
        self,
        run_prefill: Callable[
            [PDRequest, RawRequestInfo, List[int]], Awaitable[PrefillResult]
        ],
    ):
        self._run_prefill = run_prefill
        self._server = ZmqRpcServer(self._handle)

    async def start(self) -> str:
        """Start serving and return the endpoint to advertise to routers."""
        port = await self._server.start()
        return f"tcp://{ray.util.get_node_ip_address()}:{port}"

    async def _handle(self, payload: bytes) -> bytes:
        prefill_request = PrefillRequest.decode(payload)
        request = parse_pd_request(
            prefill_request.path, json.loads(prefill_request.body)
        )
        if request is None:
            raise ValueError(f"Not a P/D request: {prefill_request.path}")
        replica_context = serve.context._get_internal_replica_context()
        # Each call runs in its own task, so this context ends with the call.
        serve.context._serve_request_context.set(
            serve.context._RequestContext(
                route=prefill_request.path,
                request_id=prefill_request.request_id,
                multiplexed_model_id=prefill_request.multiplexed_model_id,
                app_name=replica_context.app_name if replica_context else "",
            )
        )
        with bind_request_lifecycle_owner(prefill_request.lifecycle_owner):
            result = await self._run_prefill(
                request,
                RawRequestInfo(headers=prefill_request.headers),
                prefill_request.token_ids,
            )
        return _encode_result(result)

    async def close(self) -> None:
        await self._server.close()
