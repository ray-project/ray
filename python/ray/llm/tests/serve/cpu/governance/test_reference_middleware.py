import sys

import pytest

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
)
from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)
from ray.llm._internal.serve.core.governance.reference_middleware import (
    BudgetMiddleware,
    PIIMiddleware,
)
from ray.llm._internal.serve.core.governance.utils import (
    extract_request_text,
    extract_response_text,
)


@pytest.fixture
def context() -> RequestContext:
    return RequestContext(model_id="test-model", user_id="user-a")


def test_budget_middleware_is_instantiable_without_after_inference():
    middleware = BudgetMiddleware(token_budget=10)
    assert isinstance(middleware, BudgetMiddleware)


@pytest.mark.asyncio
async def test_pii_middleware_blocks_email_in_request(context):
    middleware = PIIMiddleware()
    request = ChatCompletionRequest(
        model="test-model",
        messages=[{"role": "user", "content": "Contact me at user@example.com"}],
    )

    result = await middleware.before_inference(request, context)

    assert isinstance(result, BlockedResponse)
    assert result.rule_triggered == "PII_DETECTED"
    assert result.reason == "Request contains email address"


@pytest.mark.asyncio
async def test_pii_middleware_blocks_ssn_in_request(context):
    middleware = PIIMiddleware()
    request = ChatCompletionRequest(
        model="test-model",
        messages=[{"role": "user", "content": "My SSN is 123-45-6789"}],
    )

    result = await middleware.before_inference(request, context)

    assert isinstance(result, BlockedResponse)
    assert result.rule_triggered == "PII_DETECTED"


@pytest.mark.asyncio
async def test_pii_middleware_allows_clean_request(context):
    middleware = PIIMiddleware()
    request = ChatCompletionRequest(
        model="test-model",
        messages=[{"role": "user", "content": "Hello there"}],
    )

    result = await middleware.before_inference(request, context)

    assert result is request


@pytest.mark.asyncio
async def test_budget_middleware_blocks_after_budget_exhausted(context):
    middleware = BudgetMiddleware(token_budget=10)
    middleware._usage_by_user["user-a"] = 10
    request = ChatCompletionRequest(
        model="test-model",
        messages=[{"role": "user", "content": "Hello"}],
    )

    result = await middleware.before_inference(request, context)

    assert isinstance(result, BlockedResponse)
    assert result.rule_triggered == "BUDGET_EXCEEDED"


@pytest.mark.asyncio
async def test_budget_middleware_tracks_usage_on_complete(context):
    middleware = BudgetMiddleware(token_budget=100)
    request = ChatCompletionRequest(
        model="test-model",
        messages=[{"role": "user", "content": "Hello"}],
    )

    assert await middleware.before_inference(request, context) is request
    await middleware.on_inference_complete({"total_tokens": 7}, context)

    assert middleware._usage_by_user["user-a"] == 7


def test_extract_request_text_from_chat_messages():
    request = ChatCompletionRequest(
        model="test-model",
        messages=[
            {"role": "user", "content": "Hello"},
            {"role": "user", "content": "World"},
        ],
    )

    assert extract_request_text(request) == "Hello\nWorld"


def test_extract_response_text_from_dict_choice():
    response = {
        "choices": [{"message": {"role": "assistant", "content": "Hi there"}}],
    }

    assert extract_response_text(response) == "Hi there"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
