# flake8: noqa

# __propagation_start__
import requests
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)
from starlette.requests import Request

from ray import serve
from ray.serve.schema import TracingConfig


@serve.deployment
class Upstream:
    def __call__(self, request: Request) -> str:
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            "call_downstream", context=serve.get_trace_context()
        ):
            headers = {}
            # Add a `traceparent` header for the current span.
            TraceContextTextMapPropagator().inject(headers)
            return requests.get(
                "http://localhost:8000/downstream", headers=headers
            ).text


@serve.deployment
class Downstream:
    def __call__(self, request: Request) -> str:
        return "Hello from downstream!"


serve.start(tracing_config=TracingConfig(enabled=True, sampling_ratio=1.0))
serve.run(Downstream.bind(), name="downstream", route_prefix="/downstream")
serve.run(Upstream.bind(), name="upstream", route_prefix="/")

print(requests.get("http://localhost:8000/").text)
# __propagation_end__

import json
import os

from ray._common.test_utils import wait_for_condition
from ray.serve._private.logging_utils import get_serve_logs_dir

spans_dir = os.path.join(get_serve_logs_dir(), "spans")


def read_spans():
    """Return all spans in this session's span files."""
    if not os.path.isdir(spans_dir):
        return []
    spans = []
    decoder = json.JSONDecoder()
    for file_name in os.listdir(spans_dir):
        with open(os.path.join(spans_dir, file_name)) as f:
            text, pos = f.read(), 0
        while True:
            while pos < len(text) and text[pos].isspace():
                pos += 1
            if pos == len(text):
                break
            span, pos = decoder.raw_decode(text, pos)
            spans.append(span)
    return spans


def downstream_joined_upstream_trace() -> bool:
    spans = read_spans()
    upstream = [s for s in spans if s["name"] == "call_downstream"]
    downstream = [
        s for s in spans if s["name"].startswith("proxy_http_request Downstream")
    ]
    return any(
        d["parent_id"] == u["context"]["span_id"]
        and d["context"]["trace_id"] == u["context"]["trace_id"]
        for u in upstream
        for d in downstream
    )


wait_for_condition(downstream_joined_upstream_trace)
serve.shutdown()
