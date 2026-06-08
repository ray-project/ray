import sys

import pytest

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.serving_patterns.data_parallel.builder import (
    build_dp_openai_app,
)
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    consistent_hash_deployment_config,
    requires_direct_streaming,
    run_app_through_haproxy,
    session_chat_response,
)


@requires_direct_streaming
class TestDPDirectStreamingConsistentHashRouting:
    """Session affinity over the DP direct-streaming path.

    The DPServer is the ingress LLMRouter pins via ConsistentHashRouter, so a
    request flows through HAProxy and the ``/internal/route`` decision to one
    DPServer replica. The session id reaches the chosen replica, and one session
    pins to one replica.
    """

    @pytest.fixture(name="llm_config")
    def _llm_config(self):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id="test-model"))

    @pytest.fixture(name="base_url")
    def run_dp_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        llm_config = llm_config_with_mock_engine
        llm_config.deployment_config = consistent_hash_deployment_config()
        yield run_app_through_haproxy(build_dp_openai_app({"llm_config": llm_config}))

    def test_session_affinity(self, base_url):
        replicas = {
            session_chat_response(base_url, "test-session-id").headers["x-replica-id"]
            for _ in range(10)
        }
        assert len(replicas) == 1

    def test_different_sessions_spread(self, base_url):
        replicas = {
            session_chat_response(base_url, f"test-session-id-{i}").headers[
                "x-replica-id"
            ]
            for i in range(10)
        }
        assert len(replicas) > 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
