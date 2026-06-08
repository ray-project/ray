"""
Shared helpers for direct-streaming session-affinity tests.
"""

import httpx
import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY, SERVE_SESSION_ID
from ray.serve._private.test_utils import check_running, get_application_url
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
        "num_replicas": 10,
        "request_router_config": RequestRouterConfig(
            request_router_class=CONSISTENT_HASH_ROUTER,
            request_router_kwargs={
                "num_virtual_nodes": 100,
                "num_fallback_replicas": 2,
            },
        ),
    }


def run_app_through_haproxy(app, timeout_s: int = 60) -> str:
    """Run ``app`` and return its (HAProxy) URL once all replicas are RUNNING."""
    serve.run(app)
    wait_for_condition(check_running, timeout=timeout_s)
    return get_application_url(use_localhost=True)


def session_chat_response(base_url: str, session_id: str, model: str = "test-model"):
    """POST a one-token chat request carrying ``session_id`` through HAProxy.

    Asserts the request succeeded and the session id survived the HAProxy hop to
    the serving replica. Returns the response so callers can read the serving
    replica from the ``x-replica-id`` header (and, for P/D, the prefill replica
    from ``kv_transfer_params.remote_engine_id``).
    """
    resp = httpx.post(
        f"{base_url}/v1/chat/completions",
        json={
            "model": model,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 1,
        },
        headers={SERVE_SESSION_ID: session_id},
        timeout=30,
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["x-serve-session-id"] == session_id
    return resp
