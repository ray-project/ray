import contextlib
import sys

import pytest
from vllm.outputs import CompletionOutput, RequestOutput
from vllm.sampling_params import SamplingParams

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.llm._internal.serve.routing_policies.kv_aware.lifecycle import (
    LifecycleEvent,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.token_tracking import (
    enable_token_tracking,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockAsyncLLM
from ray.serve.llm.request_router import KVAwareRouter

# A pre-tokenized prompt, as vLLM's serving layer always passes to generate.
PROMPT = {"prompt_token_ids": [1, 2, 3]}
PREFILL_COMPLETE = LifecycleEvent.PREFILL_COMPLETE
FINISHED = LifecycleEvent.FINISHED


def request_output(token_counts, prompt_len=5, finished=False):
    """A real vLLM RequestOutput: one CompletionOutput per entry in token_counts."""
    return RequestOutput(
        request_id="r",
        prompt=None,
        prompt_token_ids=list(range(prompt_len)),
        prompt_logprobs=None,
        outputs=[
            CompletionOutput(
                index=i,
                text="",
                token_ids=list(range(n)),
                cumulative_logprob=None,
                logprobs=None,
            )
            for i, n in enumerate(token_counts)
        ],
        finished=finished,
    )


def delta_steps(num_tokens, prompt_len=5):
    """A DELTA-kind stream: one new token per step, last step finished."""
    return [
        request_output([1], prompt_len=prompt_len, finished=i == num_tokens - 1)
        for i in range(num_tokens)
    ]


class RecordingPublisher:
    """Stands in for ``LifecyclePublisher``."""

    def __init__(self):
        self.events = []

    def publish(self, request_id, event):
        self.events.append((request_id, event))


def tracking_engine(script, **engine_kwargs):
    publisher = RecordingPublisher()
    engine = enable_token_tracking(MockAsyncLLM, publisher)(script, **engine_kwargs)
    return engine, publisher


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "script, error_after, close_after, expected",
    [
        (delta_steps(3), None, None, [PREFILL_COMPLETE, FINISHED]),
        (delta_steps(3), None, 1, [PREFILL_COMPLETE, FINISHED]),
        (delta_steps(3), 1, None, [PREFILL_COMPLETE, FINISHED]),
        (
            [request_output([0]), request_output([0], finished=True)],
            None,
            None,
            [FINISHED],
        ),
    ],
    ids=["finished", "client-closed", "engine-error", "no-tokens"],
)
async def test_publishes_lifecycle_events(script, error_after, close_after, expected):
    """Prefill completes at the first output token, and every stream publishes
    its end exactly once."""
    engine, publisher = tracking_engine(script, error_after=error_after)
    stream = engine.generate(PROMPT, SamplingParams(), "r")
    raises = pytest.raises(RuntimeError) if error_after else contextlib.nullcontext()
    received = 0
    with raises:
        async for _ in stream:
            received += 1
            if received == close_after:
                await stream.aclose()

    assert publisher.events == [("r", event) for event in expected]


@pytest.mark.asyncio
async def test_lifecycle_uses_serve_request_id():
    """Events name the Serve request id routers book the request under, even if
    vLLM's engine-level id is different."""
    engine, publisher = tracking_engine(delta_steps(1))

    serve.context._serve_request_context.set(
        serve.context._RequestContext(request_id="serve-route-id")
    )
    try:
        async for _ in engine.generate(PROMPT, SamplingParams(), "chatcmpl-route-id"):
            pass
    finally:
        serve.context._serve_request_context.set(serve.context._RequestContext())

    assert publisher.events == [
        ("serve-route-id", PREFILL_COMPLETE),
        ("serve-route-id", FINISHED),
    ]


def test_decorator_returns_subclass():
    """The decorator returns an isinstance-compatible subclass."""
    assert issubclass(
        enable_token_tracking(MockAsyncLLM, RecordingPublisher()), MockAsyncLLM
    )


@pytest.mark.parametrize(
    "request_router_config, expected",
    [
        ({"request_router_class": KVAwareRouter}, True),
        ({}, False),  # default (non-KV) router
        (None, False),  # no router configured
    ],
)
def test_is_kv_aware(request_router_config, expected):
    """The engine publishes lifecycle events only on KVAwareRouter deployments."""
    deployment_config = {}
    if request_router_config is not None:
        deployment_config["request_router_config"] = request_router_config
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
        },
        accelerator_type=None,
        deployment_config=deployment_config,
    )
    assert is_kv_aware(llm_config) is expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
