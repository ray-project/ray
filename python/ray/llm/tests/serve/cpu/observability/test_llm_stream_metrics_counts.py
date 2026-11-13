"""Per-choice token counts from vLLM's own stream generators, as added by
python/requirements/llm/patches/vllm-stream-token-counts.patch; timing belongs
to C tests."""

import asyncio
import json
from types import SimpleNamespace

import pytest


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


async def events(stream, wires):
    received = []
    async for wire in stream:
        wires.append(wire)
        if wire != "data: [DONE]\n\n":
            event = json.loads(wire.removeprefix("data: "))
            assert "error" not in event, event
            received.append(event)
    return received


async def counted(stream):
    """Runs ``stream()`` and checks that every streamed choice has a count."""
    received = await events(stream(), [])
    assert all(
        "num_tokens" in choice for event in received for choice in event["choices"]
    )
    return received


def counts(received):
    return [
        (choice["index"], choice["num_tokens"])
        for event in received
        for choice in event["choices"]
    ]


def serving_options(serving):
    serving.enable_force_include_usage = False
    serving.enable_per_request_metrics = False
    serving.enable_prompt_tokens_details = False
    serving.system_fingerprint = None
    serving.enable_log_outputs = False
    serving.enable_log_deltas = False
    serving.request_logger = None


class RequestLogger:
    def __init__(self):
        self.logged = []

    def log_outputs(self, **kwargs):
        self.logged.append(kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("weight_versions", [False, True])
@pytest.mark.parametrize("log_outputs", [False, True])
@pytest.mark.parametrize("echo", [False, True])
@pytest.mark.parametrize("return_token_ids", [False, True])
async def test_completion_counts(echo, return_token_ids, log_outputs, weight_versions):
    """Batched deltas, prompt/choice indexing, echo, output logging, and
    unchanged usage options."""
    from vllm.entrypoints.openai.completion.protocol import CompletionRequest
    from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion

    serving = object.__new__(OpenAIServingCompletion)
    serving_options(serving)
    serving.enable_log_outputs = serving.enable_log_deltas = log_outputs
    serving.request_logger = RequestLogger() if log_outputs else None
    request = CompletionRequest(
        model="model",
        prompt=["prompt", "prompt"],
        n=2,
        stream=True,
        echo=echo,
        return_token_ids=return_token_ids,
        return_weight_versions=weight_versions,
        stream_options={"include_usage": True, "continuous_usage_stats": False},
    )
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

    received = await counted(stream)
    assert counts(received) == [
        (0, 3),
        (1, 2),
        (2, 4),
        (3, 1),
        (0, 4),
        (1, 0),
        (2, 2),
        (3, 3),
    ]
    if log_outputs:
        # Output logging still sees vLLM's native token ID lists.
        assert all(
            isinstance(entry["output_token_ids"], list)
            for entry in serving.request_logger.logged
            if entry.get("delta")
        )
    assert all(event.get("usage") is None for event in received if event["choices"])
    assert received[-1]["usage"]["completion_tokens"] == 19


@pytest.mark.asyncio
@pytest.mark.parametrize("hide_reasoning", [False, True])
async def test_chat_suppressed_counts(hide_reasoning):
    """Role/echo are zero; outputs a parser withholds count toward the next
    emitted choice."""
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
    serving.enable_auto_tools = False
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

    received = await counted(stream)
    initial = [(0, 0), (1, 0), (0, 0), (1, 0)]
    assert counts(received) == initial + (
        [(0, 3), (1, 7), (0, 4), (1, 0)]
        if hide_reasoning
        else [(0, 2), (1, 3), (0, 1), (1, 4), (0, 4), (1, 0)]
    )
    assert received[-1]["usage"]["completion_tokens"] == 14


@pytest.mark.asyncio
async def test_interleaved_streams_keep_their_counts():
    from vllm.entrypoints.openai.completion.protocol import CompletionRequest
    from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion

    serving = object.__new__(OpenAIServingCompletion)
    serving_options(serving)

    def stream(count):
        request = CompletionRequest(
            model="model", prompt="prompt", stream=True, max_tokens=8
        )
        items = [
            (0, result((0, count, "x", None))),
            (0, result((0, count + 1, "y", "stop"))),
        ]
        return serving.completion_stream_generator(
            request,
            [{}],
            source(items),
            "same-id",
            1,
            "model",
            1,
            None,
            SimpleNamespace(final_usage_info=None),
        )

    a, b = await asyncio.gather(events(stream(3), []), events(stream(7), []))
    assert counts(a) == [(0, 3), (0, 4)]
    assert counts(b) == [(0, 7), (0, 8)]
