"""Streaming identity and token-count metadata for HAProxy timing metrics."""

import json
from functools import lru_cache, wraps
from inspect import signature
from typing import TYPE_CHECKING, Dict

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


class _StreamRequestId(str):
    """Carry request-local counts through native response construction.

    vLLM passes this ID unchanged to each response constructor. Pydantic emits
    the original string value; no global request map or async context is needed.
    """

    __slots__ = ("pending", "preserve_token_ids")

    def __new__(cls, value, pending, preserve_token_ids):
        instance = super().__new__(cls, value)
        instance.pending = pending
        instance.preserve_token_ids = preserve_token_ids
        return instance


@lru_cache(maxsize=2)
def _install_token_count_response(chat: bool) -> None:
    """Add N at native response construction, using its existing serializer."""
    from importlib import import_module

    module = import_module(
        "vllm.entrypoints.openai."
        + ("chat_completion" if chat else "completion")
        + ".serving"
    )
    name = "ChatCompletionStreamResponse" if chat else "CompletionStreamResponse"
    original = getattr(module, name)
    validate = original.__pydantic_validator__.validate_python

    @wraps(original)
    def with_counts(**kwargs):
        identity = kwargs["id"]
        response = validate(kwargs)
        if isinstance(identity, _StreamRequestId):
            for choice in response.choices:
                if identity.pending is None:
                    fields = choice.__dict__
                    count = len(fields["token_ids"] or ())
                    if not identity.preserve_token_ids:
                        # Native validation has completed. Clear these known
                        # optional fields without Pydantic's per-field setters
                        # on every event; its existing serializer omits None.
                        fields["token_ids"] = fields["prompt_token_ids"] = None
                else:
                    count = identity.pending.pop(choice.index, 0)
                choice.model_extra["ray_token_count"] = count
        return response

    setattr(module, name, with_counts)


class _TokenCountResult:
    """Request-local view of native delta outputs, without mutating engine data.

    Observe counts when vLLM accesses outputs, after its initial role/echo
    responses. Counts from outputs suppressed by its parser remain pending
    until the next emitted choice. All other engine metadata is forwarded.
    """

    __slots__ = ("result", "pending", "offset", "observed")

    def __init__(self, pending):
        self.pending = pending

    def __getattr__(self, name):
        return getattr(self.result, name)

    @property
    def outputs(self):
        if not self.observed:
            self.observed = True
            for output in self.result.outputs:
                index = self.offset + output.index
                self.pending[index] = self.pending.get(index, 0) + len(output.token_ids)
        return self.result.outputs


def enable_stream_metrics_token_counts(serving, *, chat: bool) -> None:
    """Carry delta counts to HAProxy through vLLM's single SSE serialization.

    No clocks or metrics are recorded here. The original native async generator
    is returned directly; its response ID carries isolated request-local counts.
    """
    _install_token_count_response(chat)
    method = (
        "chat_completion_stream_generator" if chat else "completion_stream_generator"
    )
    original = getattr(serving, method)
    parameters = signature(original)

    @wraps(original)
    def with_counts(*args, **kwargs):
        bound = parameters.bind(*args, **kwargs)
        request = bound.arguments["request"]
        if not chat and not request.echo:
            # Native completions already pass delta IDs to their choice model.
            # Use a renderer-only request copy: engine sampling parameters have
            # already been constructed, and the client's request stays intact.
            # IDs are removed by the response factory unless the client asked for
            # them. This avoids wrapping the engine iterator on the common path.
            pending = None
            preserve_token_ids = bool(request.return_token_ids)
            if not preserve_token_ids:
                bound.arguments["request"] = request.model_copy(
                    update={"return_token_ids": True}
                )
        else:
            # Echo can change with return_token_ids; chat parsers can suppress
            # outputs or hide IDs. Count native deltas lazily instead so those
            # paths keep their exact native rendering and accumulate hidden N.
            pending = {}
            preserve_token_ids = bool(request.return_token_ids)
            results = bound.arguments["result_generator"]

            async def counted_results():
                view = _TokenCountResult(pending)
                async for item in results:
                    if chat:
                        view.result, view.offset = item, 0
                    else:
                        prompt_index, view.result = item
                        view.offset = prompt_index * (request.n or 1)
                    view.observed = False
                    yield view if chat else (prompt_index, view)

            bound.arguments["result_generator"] = counted_results()
        bound.arguments["request_id"] = _StreamRequestId(
            bound.arguments["request_id"], pending, preserve_token_ids
        )
        return original(*bound.args, **bound.kwargs)

    setattr(serving, method, with_counts)
