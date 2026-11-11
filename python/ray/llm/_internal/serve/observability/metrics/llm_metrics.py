"""Streaming identity and token-count metadata for HAProxy timing metrics."""

import json
import types
from functools import lru_cache, wraps
from inspect import signature
from operator import length_hint
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ray
from ray import serve

if TYPE_CHECKING:
    from fastapi import FastAPI

# Match vLLM's TTFT, ITL and request-duration buckets, in seconds.
TTFT_BUCKETS = [
    0.001,
    0.005,
    0.01,
    0.02,
    0.04,
    0.06,
    0.08,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    20.0,
    40.0,
    80.0,
    160.0,
    640.0,
    2560.0,
]
ITL_BUCKETS = [
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.15,
    0.2,
    0.3,
    0.4,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    20.0,
    40.0,
    80.0,
]
REQUEST_DURATION_BUCKETS = [
    0.3,
    0.5,
    0.8,
    1.0,
    1.5,
    2.0,
    2.5,
    5.0,
    10.0,
    15.0,
    20.0,
    30.0,
    40.0,
    50.0,
    60.0,
    120.0,
    240.0,
    480.0,
    960.0,
    1920.0,
    7680.0,
]

LLM_METRIC_TAG_KEYS = ("model_name", "ReplicaId", "engine_worker_id", "engine")
LLM_METRICS_HEADER = "x-ray-llm-metric-tags"


def get_llm_metric_tags(model_name: str, engine: str = "0") -> Dict[str, str]:
    """Match vLLM's identity on the native engine application's replica."""
    context = serve.context._get_internal_replica_context()
    return {
        "model_name": model_name,
        "ReplicaId": context.replica_id.unique_id,
        "engine_worker_id": ray.get_runtime_context().get_worker_id(),
        "engine": engine,
    }


def add_llm_stream_metrics_headers(
    app: "FastAPI", *, model_name: str, engine: str = "0"
) -> None:
    """Add native stream identity once, without wrapping the ASGI body send.

    Args:
        app: Native vLLM FastAPI application, after routes are registered.
        model_name: Model identifier attached to streaming observations.
        engine: Single engine index matching vLLM's histogram labels.
    """
    from fastapi.routing import APIRoute, request_response

    # JSON escaping produces an ASCII header value, including for Unicode model
    # names or control characters. Identity is serialized once per application.
    header_value = json.dumps(
        get_llm_metric_tags(model_name, engine),
        ensure_ascii=True,
        separators=(",", ":"),
    )

    def with_identity(handler):
        async def route_handler(request):
            response = await handler(request)
            if response.status_code == 200 and response.headers.get(
                "content-type", ""
            ).startswith("text/event-stream"):
                response.headers[LLM_METRICS_HEADER] = header_value
            return response

        return route_handler

    for route in app.routes:
        if isinstance(route, APIRoute) and route.path in (
            "/v1/chat/completions",
            "/v1/completions",
        ):
            route.app = request_response(with_identity(route.get_route_handler()))


# Per-choice count of tokens generated since that choice's previous chunk. HAProxy
# pairs it with the chunk's native ``"choices":[{"index":I`` prefix.
TOKEN_COUNT_FIELD = "ray_token_count"


def _clone(function, **globals_overrides):
    """Return ``function`` with some module globals replaced.

    The clone shares the original bytecode, defaults and closure, so it behaves
    exactly like vLLM's function except for the names in ``globals_overrides``.
    Lookups of those names happen at call time, never per instance.
    """
    missing = set(globals_overrides) - set(function.__code__.co_names)
    if missing:
        raise RuntimeError(
            f"{function.__qualname__} no longer references {sorted(missing)}; "
            "update Ray Serve LLM streaming metrics for this vLLM version."
        )
    scope = dict(function.__globals__)
    scope.update(globals_overrides)
    clone = types.FunctionType(
        function.__code__,
        scope,
        function.__name__,
        function.__defaults__,
        function.__closure__,
    )
    clone.__kwdefaults__ = function.__kwdefaults__
    clone.__qualname__ = function.__qualname__
    return clone


def _null(_value) -> None:
    return None


def _copyable(model: type) -> bool:
    """Whether a field-for-field copy of ``model`` behaves exactly like it.

    True when vLLM's extra-field logging validator is the model's only validator
    or serializer and its config is vLLM's default.
    """
    decorators = model.__pydantic_decorators__
    return (
        dict(model.model_config) == {"extra": "allow"}
        and list(decorators.model_validators) == ["__log_extra_fields__"]
        and not any(
            getattr(decorators, kind)
            for kind in (
                "validators",
                "field_validators",
                "root_validators",
                "field_serializers",
                "model_serializers",
                "computed_fields",
            )
        )
    )


def _stream_model(
    model: type, name: str, *, extra: str = "allow", **overrides: Any
) -> type:
    """Return a copy of one of vLLM's stream response models, plus ``overrides``.

    vLLM's ``OpenAIBaseModel`` runs a Python wrap validator on every
    construction to debug-log unknown input keys (~1 us per model, twice per
    streamed event). vLLM's generators construct these response models with
    known keys only, so it never logs. The copy keeps vLLM's fields (in order),
    types, defaults and config and omits only that validator. If the model has
    any other validator or serializer, or different config, subclass instead so
    behavior stays exact.

    Args:
        model: vLLM response model to copy.
        name: Name of the new model.
        extra: ``"ignore"`` skips the per-instance extras dict that vLLM's
            ``"allow"`` builds (~0.2 us per construction). Only for callers that
            pass declared fields only and never set attributes on instances, and
            only for copyable models.
        **overrides: Field definitions ``(annotation, default or FieldInfo)``.

    Returns:
        The new pydantic model class.
    """
    from copy import copy

    from pydantic import ConfigDict, create_model

    if not _copyable(model):
        if extra != "allow":
            raise ValueError(f"{model.__name__} cannot be copied with extra={extra}")
        return create_model(name, __base__=model, **overrides)
    fields = {
        field: (info.annotation, copy(info))
        for field, info in model.model_fields.items()
    }
    fields.update(overrides)
    return create_model(name, __config__=ConfigDict(extra=extra), **fields)


def _closed_choices(generator) -> bool:
    """Whether ``generator``'s stream choices may ignore unknown input.

    Python stores a call's keyword names as a code constant; the choice
    construction is the call passing ``index`` and ``text``. If every such call
    passes declared fields only, vLLM's ``extra="allow"`` never stores anything
    in the extras dict, so ``extra="ignore"`` serializes the same output. The
    generator must also never read ``choices`` back, since a closed model
    rejects undeclared attribute assignment where vLLM's would store it.
    Anything this inspection cannot see (``**`` calls, nested functions) keeps
    vLLM's ``"allow"``.
    """
    import dis

    from vllm.entrypoints.openai.completion.protocol import (
        CompletionResponseStreamChoice,
    )

    if not _copyable(CompletionResponseStreamChoice):
        return False
    code = generator.__code__
    if any(isinstance(value, types.CodeType) for value in code.co_consts) or any(
        instruction.opname == "CALL_FUNCTION_EX"
        for instruction in dis.get_instructions(code)
    ):
        return False
    calls = [
        names
        for names in code.co_consts
        if isinstance(names, tuple) and {"index", "text"} <= set(names)
    ]
    declared = CompletionResponseStreamChoice.model_fields.keys()
    return (
        bool(calls)
        and "choices" not in code.co_names
        and all(set(names) <= declared for names in calls)
    )


@lru_cache(maxsize=2)
def _completion_models(closed: bool):
    """Response models that add the token count during native validation.

    vLLM passes each chunk's delta ``token_ids`` to its choice model when
    ``return_token_ids`` is set. ``ray_token_count`` reads that same input, so
    no Python frame runs per event. The ID fields serialize exactly as vLLM does
    when the client did not request them (``null``); the visible variant keeps
    them for clients that did.

    Returns ``{(visible_ids, counted_by_len): (choice model, response model)}``.
    With ``counted_by_len``, the cloned generator passes ``len(token_ids)``
    instead of the list (see ``_completion_generators``), so the count is a
    plain integer field; otherwise ``length_hint`` counts the list.

    The ``counted_by_len`` model also serializes the hidden ID fields without
    Python: they read vLLM's ``weight_versions`` input, which is ``None`` unless
    the client requested weight versions (that case uses the listed model), so
    pydantic-core writes vLLM's own ``null`` values. The prompt IDs passed for
    the internal ``return_token_ids`` must not be serialized: with ``closed``
    (see ``_closed_choices``) the choice ignores that undeclared input, and
    otherwise an excluded field consumes it.
    """
    from typing import Annotated

    from pydantic import BeforeValidator, Field, PlainSerializer
    from vllm.entrypoints.openai.completion.protocol import (
        CompletionResponseStreamChoice,
        CompletionStreamResponse,
    )

    listed_count = (
        Annotated[int, BeforeValidator(length_hint)],
        Field(default=0, validation_alias="token_ids"),
    )
    hidden = (Annotated[Any, PlainSerializer(_null, return_type=None)], None)
    native_null = (
        Optional[List[int]],
        Field(default=None, validation_alias="weight_versions"),
    )
    counted = dict(
        prompt_token_ids=native_null,
        token_ids=native_null,
        ray_token_count=(int, Field(default=0, validation_alias="token_ids")),
    )
    if not closed:
        counted["ray_prompt_token_ids"] = (
            Any,
            Field(default=None, validation_alias="prompt_token_ids", exclude=True),
        )
    choices = {
        (True, False): _stream_model(
            CompletionResponseStreamChoice,
            "VisibleIdsChoice",
            ray_token_count=listed_count,
        ),
        (False, False): _stream_model(
            CompletionResponseStreamChoice,
            "HiddenListedChoice",
            prompt_token_ids=hidden,
            token_ids=hidden,
            ray_token_count=listed_count,
        ),
        (False, True): _stream_model(
            CompletionResponseStreamChoice,
            "HiddenCountedChoice",
            extra="ignore" if closed else "allow",
            **counted,
        ),
    }
    return {
        key: (
            choice,
            _stream_model(
                CompletionStreamResponse,
                choice.__name__.replace("Choice", "Response"),
                choices=(List[choice], ...),
            ),
        )
        for key, choice in choices.items()
    }


def _completion_generators(original):
    """Clone vLLM's completion stream generator around the counting models.

    vLLM's generator calls ``as_list`` for the choice's ``token_ids`` and, only
    when output logging is enabled, for logged IDs. Without logging, the
    ``counted_by_len`` clone binds ``as_list`` to ``len``: the choice receives
    the count directly, from a C builtin.
    """
    generators = {}
    models = _completion_models(_closed_choices(original))
    for (visible, counted), (choice, response) in models.items():
        overrides = dict(
            CompletionResponseStreamChoice=choice, CompletionStreamResponse=response
        )
        if counted:
            overrides["as_list"] = len
        generators[(visible, counted)] = _clone(original, **overrides)
    return generators


def _echo_generator(original, pending: Dict[int, int]):
    """Per-request clone for echo, whose rendering changes with token IDs.

    Counts arrive from the engine outputs in ``pending`` and are attached to the
    next emitted choice; this rare path keeps vLLM's echo output unchanged.
    """
    from vllm.entrypoints.openai.completion.protocol import (
        CompletionResponseStreamChoice,
    )

    def choice(**fields):
        value = CompletionResponseStreamChoice(**fields)
        value.__pydantic_extra__[TOKEN_COUNT_FIELD] = pending.pop(value.index, 0)
        return value

    return _clone(original, CompletionResponseStreamChoice=choice)


async def _counted_outputs(results, pending: Dict[int, int], num_choices: int):
    async for prompt_index, result in results:
        for output in result.outputs:
            index = prompt_index * num_choices + output.index
            pending[index] = pending.get(index, 0) + len(output.token_ids)
        yield prompt_index, result


def _install_completion_counts(serving) -> None:
    original = type(serving).completion_stream_generator
    parameters = signature(original)
    generators = _completion_generators(original)
    # The counted clone relies on as_list feeding only token IDs and logs, and on
    # weight_versions being passed only when requested.
    if not hasattr(serving, "enable_log_outputs") or (
        "return_weight_versions" not in original.__code__.co_names
    ):
        raise RuntimeError(
            "vLLM's completion stream generator changed; update Ray Serve LLM "
            "streaming metrics for this vLLM version."
        )

    @wraps(original)
    def with_counts(*args, **kwargs):
        bound = parameters.bind(serving, *args, **kwargs)
        request = bound.arguments["request"]
        if request.echo:
            pending: Dict[int, int] = {}
            bound.arguments["result_generator"] = _counted_outputs(
                bound.arguments["result_generator"], pending, request.n or 1
            )
            generator = _echo_generator(original, pending)
        else:
            visible = bool(request.return_token_ids)
            if not visible:
                # A renderer-only copy: engine sampling parameters were built
                # from the client's request, which is left untouched.
                bound.arguments["request"] = request.model_copy(
                    update={"return_token_ids": True}
                )
            counted = (
                not visible
                and not serving.enable_log_outputs
                and not request.return_weight_versions
            )
            generator = generators[(visible, counted)]
        return generator(*bound.args, **bound.kwargs)

    serving.completion_stream_generator = with_counts


def _install_chat_counts(serving) -> None:
    """Attach exact per-choice deltas of vLLM's own chat output token counts.

    ``ChatStreamProcessor.num_output_tokens`` counts every generated token,
    including tokens a reasoning/tool parser withholds; the delta since the
    previous emitted chunk is attached during vLLM's own serialization loop.
    """
    from vllm.entrypoints.openai.chat_completion import serving as module

    cls = type(serving)
    registries: Dict[int, list] = {}

    class CountingProcessor(module.ChatStreamProcessor):
        def __init__(self, request, *args, **kwargs):
            super().__init__(request, *args, **kwargs)
            registries[id(request)].append(self)

    choice = _stream_model(module.ChatCompletionResponseStreamChoice, "ChatChoice")
    response = _stream_model(
        module.ChatCompletionStreamResponse, "ChatResponse", choices=(List[choice], ...)
    )
    raw = _clone(
        cls.chat_completion_stream_generator_raw,
        ChatStreamProcessor=CountingProcessor,
        ChatCompletionResponseStreamChoice=choice,
        ChatCompletionStreamResponse=response,
    )
    # The serialization loop below mirrors vLLM's chat_completion_stream_generator.
    if "chat_completion_stream_generator_raw" not in (
        cls.chat_completion_stream_generator.__code__.co_names
    ):
        raise RuntimeError(
            "vLLM's chat stream serializer changed; update Ray Serve LLM "
            "streaming metrics for this vLLM version."
        )
    ErrorResponse = module.ErrorResponse
    logger = module.logger

    @wraps(cls.chat_completion_stream_generator)
    async def with_counts(request, *args, **kwargs):
        processors: list = []
        emitted: Dict[int, int] = {}
        registries[id(request)] = processors
        try:
            async for chunk in raw(serving, request, *args, **kwargs):
                if isinstance(chunk, ErrorResponse):
                    data = chunk.model_dump_json()
                else:
                    for choice in chunk.choices:
                        total = processors[choice.index].num_output_tokens
                        choice.__pydantic_extra__[TOKEN_COUNT_FIELD] = total - (
                            emitted.get(choice.index, 0)
                        )
                        emitted[choice.index] = total
                    data = chunk.model_dump_json(
                        exclude_unset=True, exclude_none=not chunk.choices
                    )
                yield f"data: {data}\n\n"
        except Exception as e:
            logger.exception("Error serializing chat completion stream.")
            data = serving.create_streaming_error_response(e)
            yield f"data: {data}\n\n"
        finally:
            registries.pop(id(request), None)
        yield "data: [DONE]\n\n"

    serving.chat_completion_stream_generator = with_counts


def enable_stream_metrics_token_counts(serving, *, chat: bool) -> None:
    """Carry per-chunk token counts to HAProxy in vLLM's single serialization.

    No clocks or metrics are recorded here; HAProxy times the chunks. Client
    request options and native response fields are unchanged apart from the
    added ``ray_token_count`` on every streamed choice.
    """
    if chat:
        _install_chat_counts(serving)
    else:
        _install_completion_counts(serving)
