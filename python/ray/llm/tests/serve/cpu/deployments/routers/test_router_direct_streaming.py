import sys

import httpx
import pytest

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    LoraConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm.tests.serve.cpu.deployments.utils.direct_streaming_utils import (
    consistent_hash_deployment_config,
    requires_direct_streaming,
    run_app_through_haproxy,
    session_chat_response,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import FakeLoraModelLoader
from ray.serve._private.constants import RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY


class _LoraTestServer(LLMServer):
    """LLMServer test double that avoids downloading an adapter from cloud storage."""

    async def __init__(self, llm_config, **kwargs):
        kwargs["model_downloader"] = FakeLoraModelLoader
        await super().__init__(llm_config, **kwargs)


@requires_direct_streaming
class TestDirectStreamingConsistentHashRouting:
    """Session affinity over the full direct-streaming path.

    A request flows through HAProxy and the LLMRouter ``/internal/route``
    decision (ConsistentHashRouter) to a backend replica. The session id
    reaches the chosen replica, and one session pins to one replica.
    """

    @pytest.fixture(name="llm_config")
    def _llm_config(self):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id="test-model"))

    @pytest.fixture(name="base_url")
    def run_direct_streaming_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        llm_config = llm_config_with_mock_engine
        llm_config.deployment_config = consistent_hash_deployment_config()
        yield run_app_through_haproxy(
            build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        )

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


@requires_direct_streaming
@pytest.mark.skipif(
    not RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY,
    reason="LoRA direct streaming requires "
    "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1.",
)
class TestDirectStreamingLora:
    """LoRA request traverses HAProxy and streams from the selected native ASGI app."""

    @pytest.fixture(name="llm_config")
    def _llm_config(self):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id="test-model"))

    @pytest.fixture(name="base_url")
    def run_direct_streaming_app(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        llm_config = llm_config_with_mock_engine
        llm_config.lora_config = LoraConfig(dynamic_lora_loading_path=None)
        llm_config.server_cls = _LoraTestServer
        yield run_app_through_haproxy(
            build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        )

    def test_lora_request(self, base_url):
        adapter_id = "test-model:adapter"

        with httpx.stream(
            "POST",
            f"{base_url}/v1/completions",
            json={
                "model": adapter_id,
                "prompt": "hello",
                "max_tokens": 2,
                "stream": True,
            },
            timeout=30,
        ) as response:
            assert response.status_code == 200, response.text
            streamed_body = "".join(response.iter_text())

        assert f"[lora_model] {adapter_id}: test_0" in streamed_body
        assert "data: [DONE]" in streamed_body

    def test_unknown_model(self, base_url):
        response = httpx.post(
            f"{base_url}/v1/completions",
            json={"model": "other:adapter", "prompt": "hello", "max_tokens": 1},
            timeout=30,
        )

        assert response.status_code == 404


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
