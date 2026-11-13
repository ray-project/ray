"""Streaming identity for HAProxy timing metrics.

HAProxy times each streamed chunk and reads each choice's ``num_tokens``, which
vLLM emits with ``python/requirements/llm/patches/vllm-stream-token-counts.patch``.
"""

import json
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
