"""Count transport through vLLM's actual serializers; timing belongs to C tests."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from ray.llm._internal.serve.observability.metrics import llm_metrics


@pytest.fixture(autouse=True)
def native_response_factories(monkeypatch):
    from vllm.entrypoints.openai.chat_completion import serving as chat
    from vllm.entrypoints.openai.completion import serving as completion

    # Install once in production; isolate the module-global factory in tests.
    llm_metrics._install_token_count_response.cache_clear()
    for module, name in (
        (chat, "ChatCompletionStreamResponse"),
        (completion, "CompletionStreamResponse"),
    ):
        monkeypatch.setattr(module, name, getattr(module, name))
    yield
    llm_metrics._install_token_count_response.cache_clear()


def result(*outputs):
    from vllm.outputs import CompletionOutput, RequestOutput

    return RequestOutput(
        request_id="test",
        prompt="prompt",
        prompt_token_ids=[1, 2],
        prompt_logprobs=None,
        outputs=[
            CompletionOutput(
                index=index,
                text=text,
                token_ids=list(range(count)),
                cumulative_logprob=None,
                logprobs=None,
                finish_reason=finish,
            )
            for index, count, text, finish in outputs
        ],
        finished=False,
        num_cached_tokens=0,
        num_cache_creation_tokens=0,
    )


async def source(items):
    for item in items:
        await asyncio.sleep(0)
        yield item


async def events(stream):
    received = []
    async for wire in stream:
        if wire != "data: [DONE]\n\n":
            event = json.loads(wire.removeprefix("data: "))
            assert "error" not in event, event
            received.append(event)
    return received


def serving_options(serving):
    serving.enable_force_include_usage = False
    serving.enable_per_request_metrics = False
    serving.enable_prompt_tokens_details = False
    serving.system_fingerprint = None


def without_counts(received):
    for event in received:
        for choice in event["choices"]:
            choice.pop("ray_token_count", None)
    return received


@pytest.mark.asyncio
@pytest.mark.parametrize("echo", [False, True])
@pytest.mark.parametrize("return_token_ids", [False, True])
async def test_completion_counts(echo, return_token_ids):
    """Batched deltas, prompt/choice indexing, echo, and unchanged usage options."""
    from vllm.entrypoints.openai.completion.protocol import CompletionRequest
    from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion

    serving = object.__new__(OpenAIServingCompletion)
    serving_options(serving)
    request = CompletionRequest(
        model="model",
        prompt=["prompt", "prompt"],
        n=2,
        stream=True,
        echo=echo,
        return_token_ids=return_token_ids,
        stream_options={"include_usage": True, "continuous_usage_stats": False},
    )
    original_request = request.model_dump()
    items = [
        (0, result((0, 3, "a", None), (1, 2, "b", None))),
        (1, result((0, 4, "c", None), (1, 1, "d", None))),
        (0, result((0, 4, "e", "stop"), (1, 0, "", "stop"))),
        (1, result((0, 2, "f", "stop"), (1, 3, "g", "stop"))),
    ]

    def stream():
        return serving.completion_stream_generator(
            request,
            [{}, {}],
            source(items),
            "test",
            1,
            "model",
            2,
            None,
            SimpleNamespace(final_usage_info=None),
        )

    baseline = await events(stream())
    llm_metrics.enable_stream_metrics_token_counts(serving, chat=False)
    measured = await events(stream())
    counts = [
        (choice["index"], choice["ray_token_count"])
        for event in measured
        for choice in event["choices"]
    ]
    assert counts == [(0, 3), (1, 2), (2, 4), (3, 1), (0, 4), (1, 0), (2, 2), (3, 3)]
    assert all(event.get("usage") is None for event in measured if event["choices"])
    assert measured[-1]["usage"]["completion_tokens"] == 19
    assert without_counts(measured) == baseline
    assert request.model_dump() == original_request
    # The native objects were not mutated by the adapter.
    assert all(isinstance(value.outputs, list) for _, value in items)


@pytest.mark.asyncio
@pytest.mark.parametrize("hide_reasoning", [False, True])
async def test_chat_suppressed_counts(hide_reasoning):
    """Role/echo are zero; suppressed outputs accumulate into the next choice."""
    from vllm.entrypoints.openai.chat_completion.protocol import (
        ChatCompletionRequest,
        DeltaMessage,
    )
    from vllm.entrypoints.openai.chat_completion.serving import OpenAIServingChat

    class Parser:
        def __init__(self, *args, **kwargs):
            pass

        def parse_delta(self, delta_text, **kwargs):
            return None if delta_text == "hidden" else DeltaMessage(content=delta_text)

        def count_reasoning_tokens(self, tokens):
            return 0

    serving = object.__new__(OpenAIServingChat)
    serving_options(serving)
    serving.parser_cls = Parser
    serving.model_config = SimpleNamespace()
    serving.response_role = "assistant"
    serving._include_reasoning_tokens_details = False
    serving.enable_log_outputs = False
    request = ChatCompletionRequest(
        model="model",
        messages=[{"role": "assistant", "content": "echo"}],
        n=2,
        stream=True,
        echo=True,
        include_reasoning=not hide_reasoning,
        return_token_ids=True,
        stream_options={"include_usage": True, "continuous_usage_stats": False},
    )
    original_request = request.model_dump()
    items = [
        result((0, 2, "hidden", None), (1, 3, "hidden", None)),
        result((0, 1, "a", None), (1, 4, "b", None)),
        result((0, 4, "c", "stop"), (1, 0, "", "stop")),
    ]

    def stream():
        return serving.chat_completion_stream_generator(
            request,
            source(items),
            "test",
            "model",
            [{"role": "assistant", "content": "echo"}],
            object(),
            SimpleNamespace(final_usage_info=None),
        )

    baseline = await events(stream())
    llm_metrics.enable_stream_metrics_token_counts(serving, chat=True)
    measured = await events(stream())
    counts = [
        (choice["index"], choice["ray_token_count"])
        for event in measured
        for choice in event["choices"]
    ]
    initial = [(0, 0), (1, 0), (0, 0), (1, 0)]
    assert counts == initial + (
        [(0, 3), (1, 7), (0, 4), (1, 0)]
        if hide_reasoning
        else [(0, 2), (1, 3), (0, 1), (1, 4), (0, 4), (1, 0)]
    )
    if hide_reasoning:
        assert all(
            choice.get("token_ids") is None
            for event in measured
            for choice in event["choices"]
        )
    else:
        assert (
            sum(
                len(choice.get("token_ids") or [])
                for event in measured
                for choice in event["choices"]
            )
            == 14
        )
    assert measured[-1]["usage"]["completion_tokens"] == 14
    assert without_counts(measured) == baseline
    assert request.model_dump() == original_request


@pytest.mark.asyncio
@pytest.mark.parametrize("echo", [False, True])
async def test_request_count_isolation(echo):
    """Interleaved generators and cancellation cannot leak request-local counts."""
    from vllm.entrypoints.openai.completion import serving as completion
    from vllm.entrypoints.openai.completion.protocol import CompletionRequest

    closed = []

    async def generator(request, result_generator, request_id):
        try:
            async for item in result_generator:
                for output in item[1].outputs:
                    await asyncio.sleep(0)
                    choice = completion.CompletionResponseStreamChoice(
                        index=0,
                        text=output.text,
                        token_ids=output.token_ids
                        if request.return_token_ids
                        else None,
                    )
                    response = completion.CompletionStreamResponse(
                        id=request_id, created=1, model="model", choices=[choice]
                    )
                    yield "data: " + response.model_dump_json() + "\n\n"
        finally:
            closed.append(request.n)

    serving = SimpleNamespace(completion_stream_generator=generator)
    llm_metrics.enable_stream_metrics_token_counts(serving, chat=False)

    def stream(count):
        return serving.completion_stream_generator(
            CompletionRequest(model="model", prompt="prompt", n=count, echo=echo),
            source([(0, result((0, count, "x", None)))]),
            "same-id",
        )

    a, b = await asyncio.gather(events(stream(3)), events(stream(7)))
    assert a[0]["choices"][0]["ray_token_count"] == 3
    assert b[0]["choices"][0]["ray_token_count"] == 7
    cancelled = stream(5)
    await anext(cancelled)
    await cancelled.aclose()
    assert sorted(closed) == [3, 5, 7]
    ordinary = completion.CompletionStreamResponse(
        id="same-id",
        created=1,
        model="model",
        choices=[completion.CompletionResponseStreamChoice(index=0, text="ordinary")],
    )
    assert "ray_token_count" not in ordinary.choices[0].model_dump()
