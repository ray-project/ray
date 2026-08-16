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
from ray.llm._internal.serve.core.governance.ingress import GovernanceIngress
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware
from ray.llm._internal.serve.core.governance.reference_middleware import (
    BudgetMiddleware,
    PIIMiddleware,
)
from ray.llm._internal.serve.core.ingress.ingress import CallMethod

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


@pytest.mark.asyncio
async def test_governance_e2e_blocks_pii_before_llm():
    llm_calls = 0

    async def mock_chat(request, raw_request_info):
        nonlocal llm_calls
        llm_calls += 1
        yield _non_streaming_response("should-not-run")

    ingress = _make_ingress(
        [PIIMiddleware()],
        _make_mock_handle(mock_chat),
    )
    response = await ingress._process_llm_request(
        _chat_request(
            messages=[{"role": "user", "content": "Email me at secret@example.com"}],
        ),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    assert llm_calls == 0
    payload = json.loads(response.body)
    assert payload["error"]["code"] == "PII_DETECTED"


@pytest.mark.asyncio
async def test_governance_e2e_blocks_pii_streaming_request():
    llm_calls = 0

    async def mock_chat(request, raw_request_info):
        nonlocal llm_calls
        llm_calls += 1
        yield _non_streaming_response("should-not-run")

    ingress = _make_ingress(
        [PIIMiddleware()],
        _make_mock_handle(mock_chat),
    )
    response = await ingress._process_llm_request(
        _chat_request(
            stream=True,
            messages=[{"role": "user", "content": "Email me at secret@example.com"}],
        ),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    assert llm_calls == 0


@pytest.mark.asyncio
async def test_governance_e2e_budget_allows_then_blocks():
    budget = BudgetMiddleware(token_budget=8)

    async def mock_chat(request, raw_request_info):
        yield _non_streaming_response("ok")

    ingress = _make_ingress([budget], _make_mock_handle(mock_chat))

    first = await ingress._process_llm_request(
        _chat_request(user="budget-user"),
        call_method=CallMethod.CHAT.value,
    )
    assert isinstance(first, JSONResponse)
    assert first.status_code == 200

    second = await ingress._process_llm_request(
        _chat_request(user="budget-user"),
        call_method=CallMethod.CHAT.value,
    )
    assert isinstance(second, JSONResponse)
    assert second.status_code == 402
    payload = json.loads(second.body)
    assert payload["error"]["code"] == "BUDGET_EXCEEDED"


@pytest.mark.asyncio
async def test_governance_e2e_blocks_pii_in_model_response():
    async def mock_chat(request, raw_request_info):
        yield _non_streaming_response("Reach me at leak@example.com")

    ingress = _make_ingress(
        [PIIMiddleware(scan_requests=False, scan_responses=True)],
        _make_mock_handle(mock_chat),
    )
    response = await ingress._process_llm_request(
        _chat_request(messages=[{"role": "user", "content": "Say hello"}]),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 400
    payload = json.loads(response.body)
    assert payload["error"]["code"] == "PII_DETECTED"


@pytest.mark.asyncio
async def test_governance_e2e_pii_and_budget_chain():
    middlewares = [BudgetMiddleware(token_budget=100), PIIMiddleware()]

    async def mock_chat(request, raw_request_info):
        yield _non_streaming_response("clean response")

    ingress = _make_ingress(middlewares, _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(messages=[{"role": "user", "content": "Summarize this doc"}]),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, JSONResponse)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_governance_e2e_streaming_success_with_budget_tracking():
    budget = BudgetMiddleware(token_budget=100)

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

    ingress = _make_ingress([budget], _make_mock_handle(mock_chat))
    response = await ingress._process_llm_request(
        _chat_request(stream=True, user="stream-user"),
        call_method=CallMethod.CHAT.value,
    )

    assert isinstance(response, StreamingResponse)
    chunks = [chunk async for chunk in response.body_iterator]
    assert len(chunks) >= 1
    assert budget._usage_by_user["stream-user"] == 6


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
