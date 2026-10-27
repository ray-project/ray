"""KV-aware P/D through HAProxy with real NIXL transfers (two P, two D)."""

import json
import sys
import uuid
from collections import Counter

import pytest
import requests

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import (
    RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE,
)

from utils import deploy_pd_apps, pd_completion, prefill_cached_tokens


@pytest.fixture(scope="module")
def model():
    with deploy_pd_apps("serve_pd_kv_router.yaml") as model:
        yield model


def test_generation(model):
    """Streamed completions, chat, and a non-generation route work."""
    with requests.post(
        "http://127.0.0.1:8000/v1/completions",
        json={
            "model": model,
            "prompt": "The capital of France is",
            "max_tokens": 8,
            "temperature": 0,
            "stream": True,
        },
        stream=True,
        timeout=120,
    ) as response:
        assert response.status_code == 200
        text = "".join(
            json.loads(line[6:])["choices"][0]["text"]
            for line in response.iter_lines(decode_unicode=True)
            if line.startswith("data: {")
        )
    assert "Paris" in text

    response = requests.post(
        "http://127.0.0.1:8000/v1/chat/completions",
        json={
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": "What is the capital of France? "
                    "Answer with the city name only.",
                }
            ],
            "max_tokens": 32,
            "temperature": 0,
            "chat_template_kwargs": {"enable_thinking": False},
        },
        timeout=120,
    )
    assert response.status_code == 200, response.text
    assert "Paris" in response.json()["choices"][0]["message"]["content"]

    response = requests.get("http://127.0.0.1:8000/v1/models", timeout=30)
    assert response.status_code == 200
    assert model in [m["id"] for m in response.json()["data"]]


def test_prefill_follows_kv_cache(model):
    """A repeated prompt goes to the P replica that cached it.

    Random selection over two P replicas would pass all four rounds only 1 in 16
    times.
    """
    for _ in range(4):
        prompt = f"Case {uuid.uuid4().hex}: " + "Notes from a mountain station. " * 30
        assert prefill_cached_tokens(pd_completion(model, prompt)) == 0
        assert prefill_cached_tokens(pd_completion(model, prompt)) > 0


def test_truncated_body_rejected(model):
    """A body larger than HAProxy's routing buffer gets a 413 naming the limit."""
    response = requests.post(
        "http://127.0.0.1:8000/v1/completions",
        json={
            "model": model,
            "prompt": "The capital of France is",
            "padding": "x" * (RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE + 1024),
        },
        timeout=120,
    )
    assert response.status_code == 413, response.text
    assert "RAY_SERVE_HAPROXY_INGRESS_REQUEST_ROUTER_BUFSIZE" in response.text


def _router_state(method):
    router = serve.get_deployment_handle("LLMRouter", app_name="pd-router")
    return router.broadcast(method).results()


def _stage_in_flight(stage):
    return [state[stage] for state in _router_state("get_in_flight")]


def _decode_picks():
    return Counter(
        pick for picks in _router_state("get_decode_picks") for pick in picks
    )


def test_stage_loads_stay_separate(model):
    """D picks by decode load only: while one request decodes on a D replica,
    the same prompt goes to the other one even though the busy replica has it
    cached. Each engine's lifecycle events free only its own stage's
    bookings."""
    prompt = f"Case {uuid.uuid4().hex}: count upward from one."
    before = _decode_picks()
    with requests.post(
        "http://127.0.0.1:8000/v1/completions",
        json={
            "model": model,
            "prompt": prompt,
            "max_tokens": 1000,
            "ignore_eos": True,
            "stream": True,
        },
        stream=True,
        timeout=120,
    ) as stream:
        # Hold the line iterator: dropping it closes the connection, which
        # aborts the decode.
        lines = stream.iter_lines()
        next(line for line in lines if line.startswith(b"data: "))
        busy = _decode_picks() - before
        pd_completion(model, prompt)
        idle = _decode_picks() - before - busy
    assert len(busy) == len(idle) == 1
    assert idle.keys() != busy.keys()

    # Closing the stream aborts the decode and frees its booking.
    wait_for_condition(lambda: set(_stage_in_flight("decode")) == {0})
    assert set(_stage_in_flight("prefill")) == {0}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
