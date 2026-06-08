import sys

import httpx
import pytest

from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.constants import RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
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
class TestPDDirectStreamingConsistentHashRouting:
    """Session affinity over the full PD direct-streaming path.

    The decode server is the ingress; LLMRouter pins it via ConsistentHashRouter
    and forwards the session id to the prefill handle, so both legs pin per
    session. The decode replica comes from the ``x-replica-id`` header; the
    prefill replica from ``kv_transfer_params.remote_engine_id`` (stamped by the
    prefill, echoed by the decode).
    """

    @pytest.fixture(name="llm_config")
    def _llm_config(self):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id="test-model"))

    @pytest.fixture(name="base_url")
    def run_pd_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        def _pd_config():
            config = llm_config_with_mock_engine.model_copy(deep=True)
            config.engine_kwargs = {
                "kv_transfer_config": {
                    "kv_connector": "NixlConnector",
                    "kv_role": "kv_both",
                }
            }
            config.deployment_config = {
                "num_replicas": 4,
                "request_router_config": RequestRouterConfig(
                    request_router_class=CONSISTENT_HASH_ROUTER,
                    request_router_kwargs={
                        "num_virtual_nodes": 100,
                        "num_fallback_replicas": 2,
                    },
                ),
            }
            return config

        serve.run(
            build_pd_openai_app(
                {"prefill_config": _pd_config(), "decode_config": _pd_config()}
            )
        )
        wait_for_condition(check_running, timeout=60)
        yield get_application_url(use_localhost=True)

    def _serving_replicas(self, base_url, session_id):
        """Return the (decode, prefill) replicas that served a ``session_id`` request."""
        resp = httpx.post(
            f"{base_url}/v1/chat/completions",
            json={
                "model": "test-model",
                "messages": [{"role": "user", "content": "hi"}],
                "max_tokens": 1,
            },
            headers={SERVE_SESSION_ID: session_id},
            timeout=30,
        )
        assert resp.status_code == 200, resp.text
        # The session id survived the HAProxy hop to the decode replica.
        assert resp.headers["x-serve-session-id"] == session_id
        decode_replica = resp.headers["x-replica-id"]
        prefill_replica = resp.json()["kv_transfer_params"]["remote_engine_id"]
        return decode_replica, prefill_replica

    def test_session_affinity(self, base_url):
        pairs = {self._serving_replicas(base_url, "test-session-id") for _ in range(10)}
        assert len(pairs) == 1

    def test_different_sessions_spread(self, base_url):
        pairs = [
            self._serving_replicas(base_url, f"test-session-id-{i}") for i in range(10)
        ]
        assert len({decode for decode, _ in pairs}) > 1
        assert len({prefill for _, prefill in pairs}) > 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
