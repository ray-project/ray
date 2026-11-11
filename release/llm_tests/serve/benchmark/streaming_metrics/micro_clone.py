"""Per-event cost: native vs clone(generator) + pydantic-only count subclass."""
import asyncio
import sys
import time
import gc
import types
from operator import length_hint
from typing import Annotated, Any
from types import SimpleNamespace
from pydantic import BeforeValidator, Field, PlainSerializer
from vllm.outputs import CompletionOutput, RequestOutput
from vllm.entrypoints.openai.completion.protocol import (
    CompletionRequest,
    CompletionResponseStreamChoice,
)
from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion

N = 20000
variant = sys.argv[1]


def make_serving():
    s = object.__new__(OpenAIServingCompletion)
    s.enable_force_include_usage = False
    s.enable_per_request_metrics = False
    s.enable_prompt_tokens_details = False
    s.system_fingerprint = None
    s.enable_log_outputs = False
    s.request_logger = None
    s.enable_log_deltas = False
    return s


prompt_ids = list(range(1024))


def outputs(n=N):
    for k in range(n):
        yield (
            0,
            RequestOutput(
                request_id="r",
                prompt="x",
                prompt_token_ids=prompt_ids if k == 0 else None,
                prompt_logprobs=None,
                outputs=[
                    CompletionOutput(
                        index=0,
                        text=" tok",
                        token_ids=[1000 + k % 7] * (1 + (k % 3 == 0)),
                        cumulative_logprob=None,
                        logprobs=None,
                        finish_reason=None,
                    )
                ],
                finished=False,
                num_cached_tokens=0,
                num_cache_creation_tokens=0,
            ),
        )


async def agen(items):
    for it in items:
        yield it


import os  # noqa: E402

NULL = (
    PlainSerializer(object.__init__, return_type=None)
    if os.environ.get("NULLMODE", "c") == "c"
    else PlainSerializer(lambda _value: None, return_type=None)
)


class CountChoice(CompletionResponseStreamChoice):
    prompt_token_ids: Annotated[Any, NULL] = None
    token_ids: Annotated[Any, NULL] = None
    ray_token_count: Annotated[int, BeforeValidator(length_hint)] = Field(
        default=0, validation_alias="token_ids"
    )


from vllm.entrypoints.openai.completion.protocol import (  # noqa: E402
    CompletionStreamResponse,
)


class CountResponse(CompletionStreamResponse):
    choices: list[CountChoice]


def clone(method, **overrides):
    f = method.__func__
    g = dict(f.__globals__)
    g.update(overrides)
    c = types.FunctionType(f.__code__, g, f.__name__, f.__defaults__, f.__closure__)
    c.__kwdefaults__ = f.__kwdefaults__
    return types.MethodType(c, method.__self__)


def main():
    serving = make_serving()
    req = CompletionRequest(model="model", prompt="x", max_tokens=N + 10, stream=True)
    gen_fn = serving.completion_stream_generator
    if variant == "clone":
        gen_fn = clone(
            serving.completion_stream_generator,
            CompletionResponseStreamChoice=CountChoice,
            CompletionStreamResponse=CountResponse,
        )
        req = req.model_copy(update={"return_token_ids": True})
    elif variant == "hook":
        from ray.llm._internal.serve.observability.metrics import llm_metrics

        llm_metrics.enable_stream_metrics_token_counts(serving, chat=False)
        gen_fn = serving.completion_stream_generator

    async def run(items):
        n = 0
        b = 0
        first = []
        async for w in gen_fn(
            req,
            [{"prompt": "x"}],
            agen(items),
            "cmpl-abc",
            1,
            "model",
            1,
            None,
            SimpleNamespace(final_usage_info=None),
        ):
            n += 1
            b += len(w)
            if len(first) < 3:
                first.append(w)
        return n, b, first

    asyncio.run(run(list(outputs(2000))))
    best = 1e9
    for _ in range(5):
        items = list(outputs())
        gc.collect()
        gc.disable()
        t0 = time.process_time()
        n, b, first = asyncio.run(run(items))
        dt = time.process_time() - t0
        gc.enable()
        best = min(best, dt)
    print(f"{variant:8s} us/event={best/n*1e6:.3f} bytes/event={b/n:.1f}")
    for w in first:
        print("   ", w.strip()[:400])


main()
