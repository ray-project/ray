import sys

import pytest

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.builder import (
    build_pd_openai_app,
)
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    consistent_hash_deployment_config,
    requires_direct_streaming,
    run_app_through_haproxy,
    session_chat_response,
)


@requires_direct_streaming
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
            config.deployment_config = consistent_hash_deployment_config()
            return config

        yield run_app_through_haproxy(
            build_pd_openai_app(
                {"prefill_config": _pd_config(), "decode_config": _pd_config()}
            )
        )

    def _serving_replicas(self, base_url, session_id):
        """Return the (decode, prefill) replicas that served a ``session_id`` request."""
        resp = session_chat_response(base_url, session_id)
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
