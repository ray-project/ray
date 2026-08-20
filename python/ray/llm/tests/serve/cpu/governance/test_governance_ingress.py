import json
import sys
from typing import List
from unittest.mock import MagicMock

import pytest
from starlette.responses import JSONResponse, StreamingResponse

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionStreamResponse,
    ModelCard,
)
from ray.llm._internal.serve.core.ingress.ingress import CallMethod
from ray.serve.llm.governance import (
    BlockedResponse,
    GovernanceIngress,
    LLMMiddleware,
)

MODEL_ID = "test-model"


def _make_model_card() -> ModelCard:
    return ModelCard(
        id=MODEL_ID,
        object="model",
        owned_by="test",
        permission=[],
        metadata={},
    )


def _make_mock_handle(remote_fn):
    mock_handle = MagicMock()
    mock_handle.chat = MagicMock()
    mock_handle.chat.remote = remote_fn
    mock_handle.options.return_value = mock_handle
    return mock_handle


def _make_ingress(middlewares: List[LLMMiddleware], mock_handle) -> GovernanceIngress:
    return GovernanceIngress(
        llm_deployments={MODEL_ID: mock_handle},
        model_cards={MODEL_ID: _make_model_card()},
        middlewares=middlewares,
    )


def _chat_request(**kwargs) -> ChatCompletionRequest:
    defaults = {
        "model": MODEL_ID,
        "messages": [{"role": "user", "content": "Hello"}],
        "stream": False,
    }
    defaults.update(kwargs)
    return ChatCompletionRequest(**defaults)


def _non_streaming_response(content: str = "Hello!") -> ChatCompletionResponse:
    return ChatCompletionResponse(
        id="test-id",
        choices=[
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        model=MODEL_ID,
        object="chat.completion",
        usage={
            "prompt_tokens": 3,
            "completion_tokens": 5,
            "total_tokens": 8,
        },
    )


class BlockerMiddleware(LLMMiddleware):
    def __init__(
        self,
        rule_triggered: str = "ACCESS_DENIED",
        reason: str = "blocked",
    ):
        self.rule_triggered = rule_triggered
        self.reason = reason

    async def before_inference(self, request, context):
        return BlockedResponse(rule_triggered=self.rule_triggered, reason=self.reason)


class PassThroughMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        return request


class TrackingMiddleware(LLMMiddleware):
    def __init__(self):
        self.complete_calls: list[dict] = []

    async def before_inference(self, request, context):
        return request

    async def after_inference(self, request, response, context):
        return response

    async def on_inference_complete(self, usage, context):
        self.complete_calls.append(usage)


@pytest.mark.asyncio
async def test_governance_blocks_before_llm_call():
    llm_calls = 0

    async def mock_chat(request, raw_request_info):
        nonlocal llm_calls
        llm_calls += 1
        yield _non_streaming_response()

    ingress = _make_ingress([BlockerMiddleware()], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 403
    assert llm_calls == 0
    payload = json.loads(response.body)
    assert payload["error"]["code"] == "ACCESS_DENIED"


@pytest.mark.asyncio
async def test_governance_throttled_returns_429():
    async def mock_chat(request, raw_request_info):
        yield _non_streaming_response()

    blocked = BlockedResponse(
        decision="THROTTLED",
        rule_triggered="BUDGET_EXCEEDED",
        reason="slow down",
        retry_after=60,
    )

    class ThrottleMiddleware(LLMMiddleware):
        async def before_inference(self, request, context):
            return blocked

    ingress = _make_ingress([ThrottleMiddleware()], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(),
        call_method=CallMethod.CHAT.value,
    )

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "60"


@pytest.mark.asyncio
async def test_governance_non_streaming_success():
    async def mock_chat(request, raw_request_info):
        yield _non_streaming_response("from-llm")

    ingress = _make_ingress([PassThroughMiddleware()], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    payload = json.loads(response.body)
    assert payload["choices"][0]["message"]["content"] == "from-llm"
    assert payload["usage"]["total_tokens"] == 8


@pytest.mark.asyncio
async def test_governance_streaming_fires_on_inference_complete():
    tracker = TrackingMiddleware()

    async def mock_chat(request, raw_request_info):
        yield ChatCompletionStreamResponse(
            id="stream-id",
            choices=[{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}],
            model=MODEL_ID,
            object="chat.completion.chunk",
        )
        yield ChatCompletionStreamResponse(
            id="stream-id",
            choices=[{"index": 0, "delta": {}, "finish_reason": "stop"}],
            model=MODEL_ID,
            object="chat.completion.chunk",
            usage={
                "prompt_tokens": 2,
                "completion_tokens": 4,
                "total_tokens": 6,
            },
        )

    ingress = _make_ingress([tracker], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(stream=True),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, StreamingResponse)
    chunks = [chunk async for chunk in response.body_iterator]
    assert len(chunks) >= 1
    assert tracker.complete_calls == [
        {"prompt_tokens": 2, "completion_tokens": 4, "total_tokens": 6}
    ]


class ResponseBlockerMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        return request

    async def after_inference(self, request, response, context):
        return BlockedResponse(rule_triggered="PII_DETECTED", reason="leak")


@pytest.mark.asyncio
async def test_governance_after_inference_block_closes_upstream():
    closed = {"value": False}

    async def mock_chat(request, raw_request_info):
        try:
            yield _non_streaming_response("secret")
            yield _non_streaming_response("should-not-run")
        finally:
            closed["value"] = True

    ingress = _make_ingress([ResponseBlockerMiddleware()], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    assert closed["value"] is True


@pytest.mark.asyncio
async def test_governance_streaming_complete_on_client_disconnect():
    tracker = TrackingMiddleware()

    async def mock_chat(request, raw_request_info):
        yield ChatCompletionStreamResponse(
            id="stream-id",
            choices=[{"index": 0, "delta": {"content": "hi"}, "finish_reason": None}],
            model=MODEL_ID,
            object="chat.completion.chunk",
        )
        yield ChatCompletionStreamResponse(
            id="stream-id",
            choices=[{"index": 0, "delta": {}, "finish_reason": "stop"}],
            model=MODEL_ID,
            object="chat.completion.chunk",
            usage={
                "prompt_tokens": 2,
                "completion_tokens": 4,
                "total_tokens": 6,
            },
        )

    ingress = _make_ingress([tracker], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(stream=True),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, StreamingResponse)
    iterator = response.body_iterator
    await iterator.__anext__()
    await iterator.aclose()

    assert tracker.complete_calls == [{}]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
