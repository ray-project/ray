"""Sequential P/D coordination for the direct-streaming ingress router."""

import asyncio
import base64
import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List

from fastapi import HTTPException, Request

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
)
from ray.llm._internal.serve.core.ingress.utils import _sanitize_chat_completion_request
from ray.llm._internal.serve.core.protocol import (
    IngressRoutingResponse,
    PromptTokenizer,
    RawRequestInfo,
    TokenizeError,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TRANSFER_PARAMS_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    build_kv_token_tracker,
)
from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
)
from ray.serve.context import _get_serve_request_context
from ray.serve.handle import DeploymentHandle

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig

DecodeRouter = Callable[
    [SimpleNamespace, List[int], str], Awaitable[IngressRoutingResponse]
]


class PDRequestCoordinator:
    """Run prefill before selecting decode for a direct-streaming request.

    Each ingress replica owns one coordinator with separate P and D trackers.
    The prefill handle selects and invokes P. After P finishes, the decode
    callback selects D and stages tokens. The returned routing response carries
    KV transfer metadata to HAProxy; D streams through HAProxy to the client.
    """

    def __init__(
        self,
        prefill: DeploymentHandle,
        prefill_config: "LLMConfig",
        decode_deployment_id: DeploymentID,
        decode_config: "LLMConfig",
        tokenizer: PromptTokenizer,
        route_decode: DecodeRouter,
    ) -> None:
        self.prefill = prefill
        self.prefill_kv_token_tracker = build_kv_token_tracker(
            prefill_config, prefill.deployment_id
        )
        self.decode_kv_token_tracker = build_kv_token_tracker(
            decode_config, decode_deployment_id
        )
        self.trackers = {
            prefill.deployment_id: self.prefill_kv_token_tracker,
            decode_deployment_id: self.decode_kv_token_tracker,
        }
        prefill._init(_run_router_in_separate_loop=False)
        self.tokenizer = tokenizer
        self.route_decode = route_decode

    async def route(self, request: Request, body: bytes) -> "IngressRoutingResponse":
        if "x-body-truncated" in request.headers:
            raise HTTPException(413, "P/D routing requires the complete request body")
        try:
            payload = json.loads(body)
            cls = ChatCompletionRequest if "messages" in payload else CompletionRequest
            parsed = cls.model_validate(payload)
            if isinstance(parsed, ChatCompletionRequest):
                parsed = _sanitize_chat_completion_request(parsed)
            if parsed.kv_transfer_params:
                raise ValueError("kv_transfer_params is managed by the P/D router")
            if isinstance(parsed, ChatCompletionRequest):
                for message in parsed.messages:
                    content = message.get("content")
                    if isinstance(content, list) and any(
                        part.get("type") != "text" for part in content
                    ):
                        raise ValueError(
                            "KV-aware P/D currently supports text-only prompts"
                        )
            token_ids = await self.tokenizer.tokenize(parsed.model_dump())
            if not token_ids:
                raise ValueError("KV-aware P/D requires a single tokenizable prompt")
        except TokenizeError as exc:
            raise HTTPException(exc.status_code, exc.message) from exc
        except (ValueError, TypeError) as exc:
            raise HTTPException(400, str(exc)) from exc

        request_id = _get_serve_request_context()._internal_request_id
        raw_info = RawRequestInfo.from_starlette_request(request)
        try:
            params: Dict[str, Any] = await self.prefill.prefill.remote(
                parsed,
                raw_info,
                request_token_ids=token_ids,
                routing_request_id=request_id,
            )
            await self.prefill_kv_token_tracker.release_request(request_id)
            metadata = base64.b64encode(
                json.dumps(params, separators=(",", ":")).encode()
            ).decode("ascii")
            response = await self.route_decode(
                SimpleNamespace(**parsed.model_dump()), token_ids, request_id
            )
            response[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD][
                KV_TRANSFER_PARAMS_HEADER
            ] = metadata
            return response
        except BaseException:
            # Selection can finish just as cancellation interrupts its await.
            await asyncio.shield(
                asyncio.gather(
                    self.prefill_kv_token_tracker.release_request(request_id),
                    self.decode_kv_token_tracker.release_request(request_id),
                )
            )
            raise
