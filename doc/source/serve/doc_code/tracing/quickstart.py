# flake8: noqa

# __quickstart_start__
import requests
from starlette.requests import Request

from ray import serve
from ray.serve.schema import TracingConfig


@serve.deployment
class Greeter:
    def __call__(self, request: Request) -> str:
        return "Hello!"


# Tracing is fixed when Serve starts, so start Serve with it before you deploy.
serve.start(tracing_config=TracingConfig(enabled=True, sampling_ratio=1.0))
serve.run(Greeter.bind())

print(requests.get("http://localhost:8000/").text)
# __quickstart_end__

import json
import os

import yaml

from ray._common.test_utils import wait_for_condition
from ray._common.utils import import_attr
from ray.serve._private.logging_utils import get_serve_logs_dir
from ray.serve.schema import ServeDeploySchema

spans_dir = os.path.join(get_serve_logs_dir(), "spans")


def read_spans(prefix: str):
    """Return the spans in this session's span files for one component."""
    if not os.path.isdir(spans_dir):
        return []
    spans = []
    decoder = json.JSONDecoder()
    for file_name in os.listdir(spans_dir):
        if not file_name.startswith(prefix):
            continue
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


def request_traced() -> bool:
    proxy = [s for s in read_spans("proxy") if "Greeter" in s["name"]]
    replica = [s for s in read_spans("replica") if "Greeter" in s["name"]]
    return bool(
        any(s["name"].startswith("proxy_http_request") for s in proxy)
        and any(s["name"].startswith("replica_handle_request") for s in replica)
        and {s["context"]["trace_id"] for s in proxy}
        & {s["context"]["trace_id"] for s in replica}
    )


wait_for_condition(request_traced)

# The config-file examples in the guide are valid Serve configs, and their
# import paths point at the example modules next to them.
for file_name in ("tracing_config.yaml", "otlp_config.yaml"):
    with open(os.path.join(os.path.dirname(__file__), file_name)) as f:
        config = ServeDeploySchema.model_validate(yaml.safe_load(f))
    assert config.tracing_config.enabled
    for application in config.applications:
        import_attr(application.import_path)
    import_attr(config.tracing_config.exporter_import_path)

serve.shutdown()
