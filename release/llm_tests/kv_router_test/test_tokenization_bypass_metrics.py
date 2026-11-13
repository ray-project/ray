"""The tokenization-bypass counter on a production KV-aware deployment."""

import asyncio
import sys
import uuid

import pytest
import requests

import ray
from ray import serve
from ray._common.test_utils import fetch_prometheus_metrics, wait_for_condition
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.metrics import (
    TokenizationBypassOutcome,
)
from ray.serve.llm.request_router import KVAwareRouter

from utils import MODEL_ID, build_kv_app, build_kv_config, discover_replica_endpoints

APP_NAME = "tokenization_bypass_metrics_test"
METRIC = "ray_serve_llm_tokenization_bypass_requests_total"
NUM_SUCCESS = 4
NUM_MISS = 2


@pytest.fixture(scope="module")
def server_endpoint():
    if not ray.is_initialized():
        ray.init(address="auto")
    serve.shutdown()
    llm_config = build_kv_config(
        request_router_class=KVAwareRouter, kv_events_port_base=22000
    )
    handle = serve.run(build_kv_app(llm_config), name=APP_NAME)
    endpoints = asyncio.run(discover_replica_endpoints(handle, 1))
    yield next(iter(endpoints.values()))
    serve.shutdown()


def _bypass_counts():
    """This app's counter on every node, keyed by (deployment, outcome)."""
    addresses = [
        f"{node['NodeManagerAddress']}:{node['MetricsExportPort']}"
        for node in ray.nodes()
        if node["Alive"]
    ]
    counts = {}
    for sample in fetch_prometheus_metrics(addresses).get(METRIC, []):
        if sample.labels.get("application") == APP_NAME:
            key = (sample.labels["deployment"].split(":")[0], sample.labels["outcome"])
            counts[key] = counts.get(key, 0) + sample.value
    return counts


def _wait_for_counts(expected):
    def check():
        assert _bypass_counts() == expected
        return True

    wait_for_condition(check, timeout=60)


def _post_completion(base_url, headers=None):
    response = requests.post(
        f"{base_url}/v1/completions",
        json={"model": MODEL_ID, "prompt": "The capital of France is", "max_tokens": 4},
        headers=headers,
        timeout=120,
    )
    assert response.status_code == 200, response.text


def test_tokenization_bypass_counts(server_endpoint):
    # Both deployments export every outcome at 0 before any request. Prometheus
    # rate() takes a series' first scraped value as its baseline, so an
    # outcome whose series only appeared with its first event would never count
    # that event.
    expected = {
        (deployment, outcome.value): 0
        for deployment in ("LLMRouter", "LLMServer")
        for outcome in TokenizationBypassOutcome
    }
    _wait_for_counts(expected)

    for _ in range(NUM_SUCCESS):
        _post_completion("http://127.0.0.1:8000")
    # A token key whose tokens never reached the replica, as when they expire.
    host, port = server_endpoint
    for _ in range(NUM_MISS):
        _post_completion(
            f"http://{host}:{port}", {KV_TOKEN_KEY_HEADER: uuid.uuid4().hex}
        )

    expected[("LLMServer", "success")] = NUM_SUCCESS
    expected[("LLMServer", "miss")] = NUM_MISS
    _wait_for_counts(expected)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
