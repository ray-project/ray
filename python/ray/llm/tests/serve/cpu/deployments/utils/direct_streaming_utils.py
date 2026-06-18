"""
Shared helpers for direct-streaming session-affinity tests.
"""

import itertools

import httpx
import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY, SERVE_SESSION_ID
from ray.serve._private.test_utils import (
    check_running,
    get_application_url,
    get_application_urls,
)
from ray.serve.config import RequestRouterConfig

CONSISTENT_HASH_ROUTER = (
    "ray.serve.experimental.consistent_hash_router:ConsistentHashRouter"
)

# Skip unless the direct-streaming + HAProxy env is set
requires_direct_streaming = pytest.mark.skipif(
    not (RAY_SERVE_ENABLE_HA_PROXY and RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING),
    reason="Direct streaming requires RAY_SERVE_ENABLE_HA_PROXY=1 and "
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1.",
)


def consistent_hash_deployment_config() -> dict:
    return {
        "num_replicas": 4,
        "ray_actor_options": {"num_cpus": 0.1},
        "request_router_config": RequestRouterConfig(
            request_router_class=CONSISTENT_HASH_ROUTER,
            request_router_kwargs={
                "num_virtual_nodes": 100,
                "num_fallback_replicas": 2,
            },
        ),
    }


def run_app_through_haproxy(app, timeout_s: int = 60) -> str:
    """Run ``app`` and return its (HAProxy) URL once HAProxy can route to every
    data-plane replica.

    ``ApplicationStatus.RUNNING`` only means the controller sees every replica
    running. HAProxy reloads its data-plane server map asynchronously after
    that, and the ingress request router learns the running replicas through a
    separate long poll that usually runs ahead of the reload. In that window the
    router can pin a replica HAProxy has not loaded yet, which HAProxy rejects
    with a 503 ``unknown_replica_id``. Warm up until a request has reached every
    replica so the test body never races the reload.
    """
    serve.run(app)
    wait_for_condition(check_running, timeout=timeout_s)
    base_url = get_application_url(use_localhost=True)
    _wait_until_all_replicas_routable(base_url, timeout_s=timeout_s)
    return base_url


def _wait_until_all_replicas_routable(base_url: str, timeout_s: int = 60) -> None:
    """Block until HAProxy routes successfully to every data-plane replica.

    Probes distinct sessions so consistent hashing eventually covers every
    replica. A 503 means HAProxy has not loaded that replica yet, so the probe
    is retried under a fresh session until coverage is complete.
    """
    # One HTTP data-plane target per running replica of the ingress deployment.
    expected_replicas = len(get_application_urls(use_localhost=True))
    reached = set()
    sessions = itertools.count()

    def _all_replicas_reached() -> bool:
        for _ in range(max(expected_replicas * 6, 40)):
            resp = _chat_request(base_url, f"warmup-session-{next(sessions)}")
            if resp.status_code == 200:
                reached.add(resp.headers["x-replica-id"])
            if len(reached) >= expected_replicas:
                return True
        return False

    wait_for_condition(_all_replicas_reached, timeout=timeout_s)


def _chat_request(base_url: str, session_id: str, model: str = "test-model"):
    """POST a one-token chat request carrying ``session_id`` through HAProxy."""
    return httpx.post(
        f"{base_url}/v1/chat/completions",
        json={
            "model": model,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1,
        },
        headers={SERVE_SESSION_ID: session_id},
        timeout=30,
    )


def session_chat_response(base_url: str, session_id: str, model: str = "test-model"):
    """POST a one-token chat request carrying ``session_id`` through HAProxy.

    Asserts the request succeeded and the session id survived the HAProxy hop to
    the serving replica. Returns the response so callers can read the serving
    replica from the ``x-replica-id`` header (and, for P/D, the prefill replica
    from ``kv_transfer_params.remote_engine_id``).
    """
    resp = _chat_request(base_url, session_id, model)
    assert resp.status_code == 200, resp.text
    assert resp.headers["x-serve-session-id"] == session_id
    return resp
