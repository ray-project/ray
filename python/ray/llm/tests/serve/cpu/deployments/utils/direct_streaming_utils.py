"""
Shared helpers for direct-streaming session-affinity tests.
"""

import hashlib
from typing import List, Optional

import httpx
import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY, SERVE_SESSION_ID
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import FIFOMixin, RequestRouter
from ray.serve._private.test_utils import check_running, get_application_url
from ray.serve.config import RequestRouterConfig

CONSISTENT_HASH_ROUTER = (
    "ray.serve.experimental.consistent_hash_router:ConsistentHashRouter"
)
CONTENT_HASH_ROUTER = (
    "ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils:"
    "ContentHashRouter"
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
    """Run ``app`` and wait for requests to reach multiple replicas."""
    serve.run(app)
    wait_for_condition(check_running, timeout=timeout_s)
    base_url = get_application_url(use_localhost=True)

    def ingress_routing_ready():
        # Wait for HAProxy and the router to learn about multiple replicas.
        replicas = {
            session_chat_response(base_url, f"readiness-session-{i}").headers[
                "x-replica-id"
            ]
            for i in range(16)
        }
        return len(replicas) > 1

    wait_for_condition(ingress_routing_ready, timeout=timeout_s)
    return base_url


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


class ContentHashRouter(FIFOMixin, RequestRouter):
    """Body-aware test policy: the first message's content picks the replica."""

    def initialize_state(self, **kwargs) -> None:
        pass

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        payload = (
            pending_request.args[0]
            if pending_request is not None and pending_request.args
            else None
        )
        messages = getattr(payload, "messages", None)
        if not messages or not candidate_replicas:
            return [candidate_replicas]
        content = str(messages[0].get("content"))
        ordered = sorted(candidate_replicas, key=lambda r: r.replica_id.unique_id)
        index = int(hashlib.sha1(content.encode()).hexdigest(), 16) % len(ordered)
        return [[ordered[index]]]
