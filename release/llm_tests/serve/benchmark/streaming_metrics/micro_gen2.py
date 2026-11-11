"""Per-event CPU of vLLM's completion stream generator: stock, COPIES (allow /
ignore choice) and the instrumented ON path (closed / fallback), interleaved.

Usage: python micro_gen2.py [blocks] [events_per_block]
"""
import asyncio
import gc
import statistics
import sys
import time
import types
from types import SimpleNamespace
from typing import List

from vllm.entrypoints.openai.completion import serving as completion
from vllm.entrypoints.openai.completion.protocol import CompletionRequest
from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion
from vllm.outputs import CompletionOutput, RequestOutput

from ray.llm._internal.serve.observability.metrics import llm_metrics as lm

BLOCKS = int(sys.argv[1]) if len(sys.argv) > 1 else 60
N = int(sys.argv[2]) if len(sys.argv) > 2 else 4000
prompt_ids = list(range(1024))


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


def outputs(n):
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
                        token_ids=[1000 + k % 7],
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


async def run(serving, items):
    req = CompletionRequest(model="model", prompt="x", max_tokens=N + 10, stream=True)
    n = 0
    async for _ in serving.completion_stream_generator(
        req,
        [{"prompt": "x"}],
        agen(items),
        "cmpl-0123456789abcdef",
        1,
        "model",
        1,
        None,
        SimpleNamespace(final_usage_info=None),
    ):
        n += 1
    return n


def copies(extra):
    s = make_serving()
    c = lm._stream_model(
        completion.CompletionResponseStreamChoice, "C0" + extra, extra=extra
    )
    r = lm._stream_model(
        completion.CompletionStreamResponse, "R0" + extra, choices=(List[c], ...)
    )
    s.completion_stream_generator = types.MethodType(
        lm._clone(
            OpenAIServingCompletion.completion_stream_generator,
            CompletionResponseStreamChoice=c,
            CompletionStreamResponse=r,
        ),
        s,
    )
    return s


def hook(closed):
    s = make_serving()
    original = lm._closed_choices
    if not closed:
        lm._closed_choices = lambda generator: False
    try:
        lm.enable_stream_metrics_token_counts(s, chat=False)
    finally:
        lm._closed_choices = original
    return s


variants = {
    "stock": make_serving(),
    "copies_allow": copies("allow"),
    "copies_ignore": copies("ignore"),
    "on_closed": hook(True),
    "on_fallback": hook(False),
}
times = {k: [] for k in variants}
for s in variants.values():
    asyncio.run(run(s, list(outputs(2000))))
for b in range(BLOCKS):
    names = list(variants) if b % 2 == 0 else list(reversed(variants))
    for name in names:
        items = list(outputs(N))
        gc.collect()
        gc.disable()
        t0 = time.process_time_ns()
        n = asyncio.run(run(variants[name], items))
        times[name].append((time.process_time_ns() - t0) / n / 1000)
        gc.enable()
for base in ("copies_allow", "copies_ignore"):
    print(f"vs {base}:")
    for name in variants:
        d = sorted(a - c for a, c in zip(times[name], times[base]))
        print(
            f"  {name:13s} median {statistics.median(times[name]):.3f} us/event  minus {base}: "
            f"median {statistics.median(d):+.3f} IQR [{d[len(d)//4]:+.3f}, {d[3*len(d)//4]:+.3f}]"
        )
