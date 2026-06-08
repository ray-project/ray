import sys

import httpx
import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.serve._private.constants import RAY_SERVE_ENABLE_HA_PROXY, SERVE_SESSION_ID
from ray.serve._private.test_utils import check_running, get_application_url
from ray.serve.config import RequestRouterConfig

CONSISTENT_HASH_ROUTER = (
    "ray.serve.experimental.consistent_hash_router:ConsistentHashRouter"
)


@pytest.mark.skipif(
    not (RAY_SERVE_ENABLE_HA_PROXY and RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING),
    reason="Direct streaming requires RAY_SERVE_ENABLE_HA_PROXY=1 and "
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING=1.",
)
class TestDirectStreamingConsistentHashRouting:
    """Session affinity over the full direct-streaming path.

    A request flows through HAProxy and the LLMRouter ``/internal/route``
    decision (ConsistentHashRouter) to a backend replica. The session id
    reaches the chosen replica, and one session pins to one replica.
    """

    @pytest.fixture(name="base_url")
    def run_direct_streaming_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        llm_config = llm_config_with_mock_engine
        llm_config.deployment_config = {
            "num_replicas": 4,
            "request_router_config": RequestRouterConfig(
                request_router_class=CONSISTENT_HASH_ROUTER,
                request_router_kwargs={
                    "num_virtual_nodes": 100,
                    "num_fallback_replicas": 2,
                },
            ),
        }
        serve.run(build_openai_app(LLMServingArgs(llm_configs=[llm_config])))
        wait_for_condition(check_running, timeout=60)
        yield get_application_url(use_localhost=True)

    def _serving_replica(self, base_url, session_id):
        """Return the replica that served a request carrying ``session_id``."""
        resp = httpx.post(
            f"{base_url}/v1/chat/completions",
            json={
                "model": "llm_model_id",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 1,
            },
            headers={SERVE_SESSION_ID: session_id},
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        # The session id survived the HAProxy hop to the replica.
        assert resp.headers["x-serve-session-id"] == session_id
        return resp.headers["x-replica-id"]

    def test_session_affinity(self, base_url):
        replicas = {
            self._serving_replica(base_url, "test-session-id") for _ in range(10)
        }
        assert len(replicas) == 1

    def test_different_sessions_spread(self, base_url):
        replicas = {
            self._serving_replica(base_url, f"test-session-id-{i}") for i in range(10)
        }
        assert len(replicas) > 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
