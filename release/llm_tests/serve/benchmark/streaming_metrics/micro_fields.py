"""Per-event cost of stream response model variants (construct + serialize).

Interleaves variants in many short blocks and reports the median per-block
difference to COPIES, so slow drift and noise cancel.

Usage: python micro_fields.py [blocks] [events_per_block]
"""
import gc
import statistics
import sys
import time
from typing import Any, List, Optional

from pydantic import ConfigDict, Field, create_model
from vllm.entrypoints.openai.completion.protocol import (
    CompletionResponseStreamChoice as C,
    CompletionStreamResponse as R,
)

from ray.llm._internal.serve.observability.metrics import llm_metrics as lm

BLOCKS = int(sys.argv[1]) if len(sys.argv) > 1 else 400
N = int(sys.argv[2]) if len(sys.argv) > 2 else 2000


def pair(choice, name):
    return choice, lm._stream_model(R, name, choices=(List[choice], ...))


native_null = (
    Optional[List[int]],
    Field(default=None, validation_alias="weight_versions"),
)
variants = {
    "copies": pair(lm._stream_model(C, "C0"), "R0"),
    "on": lm._completion_models(False)[(False, True)],
    "on_closed": lm._completion_models(True)[(False, True)],
    "placebo": pair(
        lm._stream_model(C, "P0", ray_unused_0=(Any, None), ray_unused_1=(Any, None)),
        "PR0",
    ),
}


def fields_of(model, **overrides):
    from copy import copy

    f = {k: (i.annotation, copy(i)) for k, i in model.model_fields.items()}
    f.update(overrides)
    return f


count = (int, Field(default=0, validation_alias="token_ids"))
absorb = (Any, Field(default=None, validation_alias="prompt_token_ids", exclude=True))
ign = ConfigDict(extra="ignore")


def pair_ign(choice, name, extra="ignore"):
    return choice, create_model(
        name,
        __config__=ConfigDict(extra=extra),
        **fields_of(R, choices=(List[choice], ...)),
    )


variants["copies_ignore"] = pair_ign(
    create_model("CI", __config__=ign, **fields_of(C)), "RI"
)
variants["on9_both_ignore"] = pair_ign(
    create_model(
        "O9B",
        __config__=ign,
        **fields_of(
            C,
            prompt_token_ids=native_null,
            token_ids=native_null,
            ray_token_count=count,
        ),
    ),
    "RO9B",
)
variants["on9i_absorb"] = pair(
    create_model(
        "O9AB",
        __config__=ign,
        **fields_of(
            C,
            prompt_token_ids=native_null,
            token_ids=native_null,
            ray_token_count=count,
            ray_prompt_token_ids=absorb,
        ),
    ),
    "RO9AB",
)
variants["on9i_any"] = pair(
    create_model(
        "O9AN",
        __config__=ign,
        **fields_of(
            C,
            prompt_token_ids=native_null,
            token_ids=native_null,
            ray_token_count=(Any, Field(default=0, validation_alias="token_ids")),
        ),
    ),
    "RO9AN",
)
variants["copies_choice_ignore"] = pair(
    create_model("CCI", __config__=ign, **fields_of(C)), "RCCI"
)
variants["on9_ignore"] = pair(
    create_model(
        "O9",
        __config__=ign,
        **fields_of(
            C,
            prompt_token_ids=native_null,
            token_ids=native_null,
            ray_token_count=count,
        ),
    ),
    "RO9",
)
variants["on9_allow"] = pair(
    create_model(
        "O9A",
        __config__=ConfigDict(extra="allow"),
        **fields_of(
            C,
            prompt_token_ids=native_null,
            token_ids=native_null,
            ray_token_count=count,
        ),
    ),
    "RO9A",
)
variants["alias_only"] = pair(
    create_model(
        "AO",
        __config__=ConfigDict(extra="allow"),
        **fields_of(
            C,
            prompt_token_ids=native_null,
            token_ids=native_null,
            ray_prompt_token_ids=absorb,
            ray_tok=(
                Any,
                Field(default=None, validation_alias="token_ids", exclude=True),
            ),
        ),
    ),
    "RAO",
)
variants["copies_int"] = pair(
    create_model(
        "CN",
        __config__=ConfigDict(extra="allow"),
        **fields_of(C, ray_token_count=(int, 0)),
    ),
    "RCN",
)

CALLS = {
    "copies": dict(token_ids=None),
    "on": dict(token_ids=1),
    "on_closed": dict(token_ids=1),
    "placebo": dict(token_ids=None),
    "copies_ignore": dict(token_ids=None),
    "on9_both_ignore": dict(token_ids=1),
    "copies_choice_ignore": dict(token_ids=None),
    "on9i_absorb": dict(token_ids=1),
    "on9i_any": dict(token_ids=1),
    "on9_ignore": dict(token_ids=1),
    "on9_allow": dict(token_ids=1),
    "alias_only": dict(token_ids=1),
    "copies_int": dict(token_ids=None),
}


def block(choice, response, token_ids, n):
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
                    token_ids=token_ids,
                    weight_versions=None,
                )
            ],
            kv_transfer_params=None,
        )
        data = chunk.model_dump_json(exclude_unset=True)
        f"data: {data}\n\n"
    return (time.perf_counter_ns() - t0) / n / 1000


for name, (choice, response) in variants.items():
    chunk = response(
        id="x",
        created=1,
        model="m",
        choices=[
            choice(
                index=0,
                text="t",
                logprobs=None,
                finish_reason=None,
                stop_reason=None,
                prompt_token_ids=None,
                weight_versions=None,
                **CALLS[name],
            )
        ],
        kv_transfer_params=None,
    )
    print(f"{name:8s} {chunk.model_dump_json(exclude_unset=True)}")

times = {name: [] for name in variants}
gc.disable()
for b in range(BLOCKS):
    order = list(variants) if b % 2 == 0 else list(reversed(variants))
    for name in order:
        choice, response = variants[name]
        times[name].append(block(choice, response, CALLS[name]["token_ids"], N))
gc.enable()
base = times["copies"]
print(f"copies median {statistics.median(base):.3f} us/event")
for name in variants:
    if name == "copies":
        continue
    d = sorted(t - c for t, c in zip(times[name], base))

    def q(p):
        return d[int(p * (len(d) - 1))]

    print(
        f"{name:8s} median {statistics.median(times[name]):.3f}  minus copies: median {statistics.median(d):+.3f}  IQR [{q(.25):+.3f}, {q(.75):+.3f}] us/event"
    )
