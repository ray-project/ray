"""Per-event cost of count-carrier variants in vLLM 0.33's completion generator."""
import asyncio
import gc
import sys
import time
import types
from operator import length_hint
from typing import Annotated, Any
from types import SimpleNamespace
from pydantic import BeforeValidator, Field, PlainSerializer
from vllm.outputs import CompletionOutput, RequestOutput
from vllm.entrypoints.openai.completion.protocol import (
    CompletionRequest,
    CompletionResponseStreamChoice,
    CompletionStreamResponse,
)
from vllm.entrypoints.openai.completion.serving import OpenAIServingCompletion

N = 30000
variant = sys.argv[1]


def _null(_v):
    return None


Count = Annotated[int, BeforeValidator(length_hint)]
Hidden = Annotated[Any, PlainSerializer(_null, return_type=None)]


class V1Choice(CompletionResponseStreamChoice):  # current
    ray_token_count: Count = Field(default=0, validation_alias="token_ids")
    prompt_token_ids: Hidden = None
    token_ids: Hidden = None


class V2Choice(CompletionResponseStreamChoice):  # as_list->len, plain int
    ray_token_count: int = Field(default=0, validation_alias="token_ids")
    prompt_token_ids: Hidden = None
    token_ids: Hidden = None


class V3Choice(CompletionResponseStreamChoice):  # as V2, IDs excluded
    ray_token_count: int = Field(default=0, validation_alias="token_ids")
    prompt_token_ids: Any = Field(default=None, exclude=True)
    token_ids: Any = Field(default=None, exclude=True)


class V4Choice(
    CompletionResponseStreamChoice
):  # as V2, nulls natively via weight_versions
    ray_token_count: int = Field(default=0, validation_alias="token_ids")
    prompt_token_ids: list[int] | None = Field(
        default=None, validation_alias="weight_versions"
    )
    token_ids: list[int] | None = Field(
        default=None, validation_alias="weight_versions"
    )
    ray_prompt_token_ids: Any = Field(
        default=None, validation_alias="prompt_token_ids", exclude=True
    )


from pydantic import ConfigDict, create_model  # noqa: E402


def standalone(model, name, **overrides):
    """vLLM's fields and config, without OpenAIBaseModel's extra-field logging."""
    fields = {n: (f.annotation, f) for n, f in model.model_fields.items()}
    fields.update(overrides)
    return create_model(name, __config__=ConfigDict(extra="allow"), **fields)


V5Choice = standalone(
    CompletionResponseStreamChoice,
    "V5Choice",
    ray_token_count=(int, Field(default=0, validation_alias="token_ids")),
    prompt_token_ids=(
        list[int] | None,
        Field(default=None, validation_alias="weight_versions"),
    ),
    token_ids=(
        list[int] | None,
        Field(default=None, validation_alias="weight_versions"),
    ),
    ray_prompt_token_ids=(
        Any,
        Field(default=None, validation_alias="prompt_token_ids", exclude=True),
    ),
)
V5Response = standalone(
    CompletionStreamResponse, "V5Response", choices=(list[V5Choice], ...)
)


V0Choice = standalone(CompletionResponseStreamChoice, "V0Choice")
V0Response = standalone(
    CompletionStreamResponse, "V0Response", choices=(list[V0Choice], ...)
)


def response(choice):
    if choice is V5Choice:
        return V5Response
    if choice is V0Choice:
        return V0Response
    return type(
        "R", (CompletionStreamResponse,), {"__annotations__": {"choices": list[choice]}}
    )


def clone(fn, **over):
    g = dict(fn.__globals__)
    g.update(over)
    c = types.FunctionType(fn.__code__, g, fn.__name__, fn.__defaults__, fn.__closure__)
    c.__kwdefaults__ = fn.__kwdefaults__
    return c


def make_serving():
    s = object.__new__(OpenAIServingCompletion)
    for k, v in dict(
        enable_force_include_usage=False,
        enable_per_request_metrics=False,
        enable_prompt_tokens_details=False,
        system_fingerprint=None,
        enable_log_outputs=False,
        request_logger=None,
        enable_log_deltas=False,
    ).items():
        setattr(s, k, v)
    return s


prompt = list(range(1024))


def outputs(n):
    for k in range(n):
        yield (
            0,
            RequestOutput(
                request_id="r",
                prompt="x",
                prompt_token_ids=prompt if k == 0 else None,
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


def main():
    s = make_serving()
    fn = OpenAIServingCompletion.completion_stream_generator
    req = CompletionRequest(model="m", prompt="x", max_tokens=N + 10, stream=True)
    if variant == "v0":  # copies only: no count, no return_token_ids
        fn = clone(
            fn,
            CompletionResponseStreamChoice=V0Choice,
            CompletionStreamResponse=V0Response,
        )
    elif variant != "native":
        req = req.model_copy(update={"return_token_ids": True})
        choice = {
            "v1": V1Choice,
            "v2": V2Choice,
            "v3": V3Choice,
            "v4": V4Choice,
            "v5": V5Choice,
        }[variant]
        over = dict(
            CompletionResponseStreamChoice=choice,
            CompletionStreamResponse=response(choice),
        )
        if variant in ("v2", "v3", "v4", "v5"):
            over["as_list"] = len
        fn = clone(fn, **over)

    async def run(items):
        n = b = 0
        first = None
        async for w in fn(
            s,
            req,
            [{"prompt": "x"}],
            agen(items),
            "cmpl-a",
            1,
            "m",
            1,
            None,
            SimpleNamespace(final_usage_info=None),
        ):
            n += 1
            b += len(w)
            first = first or w
        return n, b, first

    asyncio.run(run(list(outputs(2000))))
    best = 1e9
    for _ in range(7):
        items = list(outputs(N))
        gc.collect()
        gc.disable()
        t0 = time.process_time()
        n, b, first = asyncio.run(run(items))
        dt = time.process_time() - t0
        gc.enable()
        best = min(best, dt)
    print(f"{variant:6s} us/event={best / n * 1e6:.3f} bytes/event={b / n:.1f}")
    return first


w = main()
if "-v" in sys.argv:
    print(w)
