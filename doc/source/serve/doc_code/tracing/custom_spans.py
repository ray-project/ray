# flake8: noqa

# __custom_spans_start__
import requests
from opentelemetry import trace
from starlette.requests import Request

from ray import serve
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import TracingConfig


@serve.deployment
class Reverser:
    def reverse(self, text: str) -> str:
        tracer = trace.get_tracer(__name__)
        # Parent the span on the Serve request's span, so it joins the trace.
        with tracer.start_as_current_span(
            "reverse_text", context=serve.get_trace_context()
        ) as span:
            span.set_attribute("text.length", len(text))
            return text[::-1]


@serve.deployment
class Frontend:
    def __init__(self, reverser: DeploymentHandle):
        self._reverser = reverser

    async def __call__(self, request: Request) -> str:
        # The call to Reverser is traced and linked to this request.
        return await self._reverser.reverse.remote(request.query_params["text"])


serve.start(tracing_config=TracingConfig(enabled=True, sampling_ratio=1.0))
serve.run(Frontend.bind(Reverser.bind()))

print(requests.get("http://localhost:8000/", params={"text": "hello"}).text)
# __custom_spans_end__

import json
import os

from ray._common.test_utils import wait_for_condition
from ray.serve._private.logging_utils import get_serve_logs_dir

spans_dir = os.path.join(get_serve_logs_dir(), "spans")


def read_spans():
    """Return all spans in this session's span files."""
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


def custom_span_linked() -> bool:
    spans = read_spans()
    by_id = {s["context"]["span_id"]: s for s in spans}
    for span in spans:
        if span["name"] != "reverse_text":
            continue
        # reverse_text -> Reverser's replica span -> ... -> the proxy span.
        parent = by_id.get(span["parent_id"])
        if not parent or not parent["name"].startswith(
            "replica_handle_request Reverser"
        ):
            return False
        names = {
            s["name"].split(" ")[0]
            for s in spans
            if s["context"]["trace_id"] == span["context"]["trace_id"]
        }
        return {
            "proxy_http_request",
            "route_to_replica",
            "replica_handle_request",
        } <= names
    return False


wait_for_condition(custom_span_linked)
serve.shutdown()
