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


def _picks():
    return [pick for picks in _router_state("get_picks") for pick in picks]


def _decode_picks():
    return Counter(replica for stage, _, replica in _picks() if stage == "decode")


def _stage_loads(stage):
    """Each router's view of the stage's active requests per worker."""
    return [loads[stage] for loads in _router_state("get_stage_loads")]


def test_routers_sync_over_per_stage_meshes(model):
    """Each router replica peers its prefill tracker with the other routers'
    prefill trackers, and its decode tracker with their decode trackers."""
    routers = _router_state("get_replica_mesh_peers")
    assert len(routers) == 2
    for peers in routers:
        assert len(peers["prefill"]) == len(peers["decode"]) == 1
        assert set(peers["prefill"]).isdisjoint(peers["decode"])


def test_session_pins_each_stage(model):
    """A session binds to a P replica and, separately, to a D replica. Every
    turn reuses both, whichever router replica handles it, since the replica
    mesh shares each stage's bindings."""
    sessions = [uuid.uuid4().hex for _ in range(4)]
    for turn in range(3):
        for session in sessions:
            pd_completion(
                model,
                f"Session {session}, turn {turn}: " + "Tell me more. " * 20,
                headers={"x-session-id": session},
            )
    picks = _picks()
    for session in sessions:
        for stage in ("prefill", "decode"):
            replicas = {r for s, sess, r in picks if s == stage and sess == session}
            assert len(replicas) == 1, (session, stage, replicas)


def test_session_reaches_one_router(model):
    """HAProxy hashes each session to one router replica, so later turns can
    reuse the prefix that replica tokenized for earlier turns. Sessions still
    spread across router replicas."""
    sessions = [uuid.uuid4().hex for _ in range(16)]
    messages = {session: [] for session in sessions}
    for turn in range(3):
        for session in sessions:
            messages[session].append(
                {
                    "role": "user",
                    "content": f"Session {session}, turn {turn}: "
                    + "Tell me more. " * 20,
                }
            )
            response = requests.post(
                "http://127.0.0.1:8000/v1/chat/completions",
                json={
                    "model": model,
                    "messages": messages[session],
                    "max_tokens": 8,
                    "temperature": 0,
                    "chat_template_kwargs": {"enable_thinking": False},
                },
                headers={"x-session-id": session},
                timeout=120,
            )
            assert response.status_code == 200, response.text
            reply = response.json()["choices"][0]["message"]["content"]
            messages[session].append({"role": "assistant", "content": reply})
    routers = _router_state("get_picks")
    owners = {
        session: {
            router
            for router, picks in enumerate(routers)
            if any(sess == session for _, sess, _ in picks)
        }
        for session in sessions
    }
    assert all(len(owner) == 1 for owner in owners.values()), owners
    assert set().union(*owners.values()) == set(range(len(routers)))


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
        # Every router sees the decode booking, wherever it was made.
        (busy_loads,) = {
            frozenset(w for w, n in loads.items() if n)
            for loads in _stage_loads("decode")
        }
        assert len(busy_loads) == 1
    assert len(busy) == len(idle) == 1
    assert idle.keys() != busy.keys()

    # Closing the stream aborts the decode and frees its booking on every router.
    wait_for_condition(lambda: set(_stage_in_flight("decode")) == {0})
    assert set(_stage_in_flight("prefill")) == {0}
    for stage in ("prefill", "decode"):
        wait_for_condition(
            lambda: all(not any(loads.values()) for loads in _stage_loads(stage))
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
