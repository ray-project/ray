"""Count-carrier variants vs the closed COPIES choice (construct + serialize).

Usage: python micro_count.py [blocks]
"""
import gc
from copy import copy
import statistics
import sys
import time
from typing import Annotated, Any, List, Optional
from pydantic import ConfigDict, Field, Strict, create_model
from vllm.entrypoints.openai.completion.protocol import (
    CompletionResponseStreamChoice as C,
    CompletionStreamResponse as R,
)
from ray.llm._internal.serve.observability.metrics import llm_metrics as lm

BLOCKS = int(sys.argv[1]) if len(sys.argv) > 1 else 300
N = 2000


def fields_of(model, **o):
    f = {k: (i.annotation, copy(i)) for k, i in model.model_fields.items()}
    f.update(o)
    return f


def pair(choice, name):
    return choice, lm._stream_model(R, name, choices=(List[choice], ...))


ign = ConfigDict(extra="ignore")
nn = (Optional[List[int]], Field(default=None, validation_alias="weight_versions"))


def on(name, count, key="ray_token_count"):
    return pair(
        create_model(
            name,
            __config__=ign,
            **fields_of(C, prompt_token_ids=nn, token_ids=nn, **{key: count}),
        ),
        "R" + name,
    )


V = {
    "copies": (pair(lm._stream_model(C, "C0", extra="ignore"), "R0"), None),
    "on": (lm._completion_models(True)[(False, True)], 1),
    "int_strict": (
        on(
            "S",
            (Annotated[int, Strict()], Field(default=0, validation_alias="token_ids")),
        ),
        1,
    ),
    "any": (on("A", (Any, Field(default=0, validation_alias="token_ids"))), 1),
    "short_key": (
        on("K", (int, Field(default=0, validation_alias="token_ids")), key="ray_n"),
        1,
    ),
    "int_noalias_dflt": (
        on("D", (int, Field(default=0, validation_alias="token_ids"))),
        1,
    ),
}


def block(choice, response, tid, n):
    t0 = time.perf_counter_ns()
    for k in range(n):
        chunk = response(
            id="cmpl-0123456789abcdef0123456789abcdef",
            object="text_completion",
            created=1762800000,
            model="Qwen/Qwen3.5-0.8B",
            choices=[
                choice(
                    index=0,
                    text=" tok",
                    logprobs=None,
                    finish_reason=None,
                    stop_reason=None,
                    prompt_token_ids=None,
                    token_ids=tid,
                    weight_versions=None,
                )
            ],
            kv_transfer_params=None,
        )
        data = chunk.model_dump_json(exclude_unset=True)
        f"data: {data}\n\n"
    return (time.perf_counter_ns() - t0) / n / 1000


for name, ((c, r), tid) in V.items():
    print(
        f"{name:16s}",
        r(
            id="x",
            created=1,
            model="m",
            choices=[
                c(
                    index=0,
                    text="t",
                    logprobs=None,
                    finish_reason=None,
                    stop_reason=None,
                    prompt_token_ids=None,
                    token_ids=tid,
                    weight_versions=None,
                )
            ],
            kv_transfer_params=None,
        ).model_dump_json(exclude_unset=True),
    )
T = {k: [] for k in V}
gc.disable()
for b in range(BLOCKS):
    for name in list(V) if b % 2 == 0 else list(reversed(V)):
        (c, r), tid = V[name]
        T[name].append(block(c, r, tid, N))
gc.enable()
base = T["copies"]
print(f"copies median {statistics.median(base):.3f} us/event")
for name in V:
    if name == "copies":
        continue
    d = sorted(t - c for t, c in zip(T[name], base))

    def q(p):
        return d[int(p * (len(d) - 1))]

    print(
        f"{name:16s} minus copies: median {statistics.median(d):+.3f}  IQR [{q(.25):+.3f}, {q(.75):+.3f}] us/event"
    )
