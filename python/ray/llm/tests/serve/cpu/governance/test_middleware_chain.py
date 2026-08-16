import pytest

from ray.llm._internal.serve.core.governance.chain import MiddlewareChain
from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware


@pytest.fixture
def context() -> RequestContext:
    return RequestContext(model_id="test-model", request_id="req-1")


class BeforeOnlyMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        return request


class BlockerMiddleware(LLMMiddleware):
    def __init__(self, rule_triggered: str = "ACCESS_DENIED", reason: str = "blocked"):
        self.rule_triggered = rule_triggered
        self.reason = reason

    async def before_inference(self, request, context):
        return BlockedResponse(rule_triggered=self.rule_triggered, reason=self.reason)


class TrackingMiddleware(LLMMiddleware):
    def __init__(self, name: str):
        self.name = name
        self.before_calls: list[str] = []
        self.after_calls: list[str] = []
        self.complete_calls: list[str] = []

    async def before_inference(self, request, context):
        self.before_calls.append(self.name)
        return request

    async def after_inference(self, request, response, context):
        self.after_calls.append(self.name)
        return response

    async def on_inference_complete(self, usage, context):
        self.complete_calls.append(self.name)


class ResponseTaggerMiddleware(LLMMiddleware):
    def __init__(self, tag: str):
        self.tag = tag

    async def before_inference(self, request, context):
        return request

    async def after_inference(self, request, response, context):
        return f"{response}-{self.tag}"


@pytest.mark.asyncio
async def test_empty_chain_passes_request(context):
    chain = MiddlewareChain([])
    request = {"prompt": "hello"}

    result = await chain.before_inference(request, context)

    assert result == request
    assert not isinstance(result, BlockedResponse)


@pytest.mark.asyncio
async def test_before_inference_returns_blocked_response(context):
    chain = MiddlewareChain([BlockerMiddleware()])

    result = await chain.before_inference({"prompt": "hello"}, context)

    assert isinstance(result, BlockedResponse)
    assert result.rule_triggered == "ACCESS_DENIED"
    assert result.reason == "blocked"


@pytest.mark.asyncio
async def test_before_inference_first_blocker_wins(context):
    first = BlockerMiddleware(rule_triggered="ACCESS_DENIED", reason="first")
    second = BlockerMiddleware(rule_triggered="BUDGET_EXCEEDED", reason="second")
    chain = MiddlewareChain([first, second])

    result = await chain.before_inference({"prompt": "hello"}, context)

    assert isinstance(result, BlockedResponse)
    assert result.rule_triggered == "ACCESS_DENIED"
    assert result.reason == "first"


@pytest.mark.asyncio
async def test_before_inference_stops_after_block(context):
    tracker = TrackingMiddleware("tracker")
    blocker = BlockerMiddleware()
    chain = MiddlewareChain([blocker, tracker])

    await chain.before_inference({"prompt": "hello"}, context)

    assert tracker.before_calls == []


@pytest.mark.asyncio
async def test_before_inference_runs_in_order(context):
    first = TrackingMiddleware("first")
    second = TrackingMiddleware("second")
    chain = MiddlewareChain([first, second])

    await chain.before_inference({"prompt": "hi"}, context)

    assert first.before_calls == ["first"]
    assert second.before_calls == ["second"]


@pytest.mark.asyncio
async def test_after_inference_runs_all_middleware(context):
    first = TrackingMiddleware("first")
    second = TrackingMiddleware("second")
    chain = MiddlewareChain([first, second])

    await chain.after_inference({"prompt": "hi"}, {"text": "out"}, context)

    assert first.after_calls == ["first"]
    assert second.after_calls == ["second"]


@pytest.mark.asyncio
async def test_after_inference_chains_response(context):
    chain = MiddlewareChain(
        [
            ResponseTaggerMiddleware("a"),
            ResponseTaggerMiddleware("b"),
        ]
    )

    result = await chain.after_inference({"prompt": "hi"}, "response", context)

    assert result == "response-a-b"


@pytest.mark.asyncio
async def test_after_inference_returns_blocked_response(context):
    class ResponseBlockerMiddleware(LLMMiddleware):
        async def before_inference(self, request, context):
            return request

        async def after_inference(self, request, response, context):
            return BlockedResponse(
                rule_triggered="PII_DETECTED",
                reason="blocked in after_inference",
            )

    chain = MiddlewareChain([ResponseBlockerMiddleware()])
    result = await chain.after_inference({"prompt": "hi"}, {"text": "out"}, context)

    assert isinstance(result, BlockedResponse)
    assert result.rule_triggered == "PII_DETECTED"


@pytest.mark.asyncio
async def test_on_inference_complete_calls_all(context):
    first = TrackingMiddleware("first")
    second = TrackingMiddleware("second")
    chain = MiddlewareChain([first, second])
    usage = {"prompt_tokens": 10, "completion_tokens": 5}

    await chain.on_inference_complete(usage, context)

    assert first.complete_calls == ["first"]
    assert second.complete_calls == ["second"]


class FailingCompleteMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        return request

    async def after_inference(self, request, response, context):
        return response

    async def on_inference_complete(self, usage, context):
        raise RuntimeError("boom")


@pytest.mark.asyncio
async def test_on_inference_complete_continues_after_middleware_error(context):
    failing = FailingCompleteMiddleware()
    tracker = TrackingMiddleware("tracker")
    chain = MiddlewareChain([failing, tracker])
    usage = {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}

    await chain.on_inference_complete(usage, context)

    assert tracker.complete_calls == ["tracker"]


class FailingBeforeMiddleware(LLMMiddleware):
    async def before_inference(self, request, context):
        raise RuntimeError("before boom")


@pytest.mark.asyncio
async def test_before_only_middleware_is_instantiable(context):
    middleware = BeforeOnlyMiddleware()
    chain = MiddlewareChain([middleware])
    request = {"prompt": "hello"}

    assert await chain.before_inference(request, context) is request
    assert await chain.after_inference(request, {"text": "out"}, context) == {
        "text": "out"
    }


@pytest.mark.asyncio
async def test_before_inference_reraises_middleware_error(context):
    tracker = TrackingMiddleware("tracker")
    chain = MiddlewareChain([FailingBeforeMiddleware(), tracker])

    with pytest.raises(RuntimeError, match="before boom"):
        await chain.before_inference({"prompt": "hello"}, context)

    assert tracker.before_calls == []
