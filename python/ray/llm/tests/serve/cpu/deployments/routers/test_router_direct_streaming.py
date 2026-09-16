import sys

import httpx
import pytest
from fastapi import HTTPException

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
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
from ray.serve._private.test_utils import wait_for_haproxy_routing_to_replica


class _LoraTestLoader(FakeLoraModelLoader):
    async def load_model_from_config(self, lora_model_id, llm_config):
        if lora_model_id == "test-model:missing-adapter":
            raise HTTPException(404, "Unable to find LoRA adapter config file")
        return await super().load_model_from_config(lora_model_id, llm_config)


class _LoraTestServer(LLMServer):
    """LLMServer test double that avoids downloading an adapter from cloud storage."""

    async def __init__(self, llm_config, **kwargs):
        kwargs["model_downloader"] = _LoraTestLoader
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
        llm_config.deployment_config = {"num_replicas": 2}
        base_url = run_app_through_haproxy(
            build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
        )
        wait_for_haproxy_routing_to_replica()
        yield base_url

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
            assert response.status_code == 200, response.read()
            cold_replica = response.headers["x-replica-id"]
            streamed_body = "".join(response.iter_text())

        assert f"[lora_model] {adapter_id}: test_0" in streamed_body
        assert "data: [DONE]" in streamed_body

        controller = serve.context._get_global_client()._controller

        def adapter_is_advertised():
            deployments = ray.get(controller._all_running_replicas.remote())
            replicas = next(
                replicas
                for deployment, replicas in deployments.items()
                if deployment.name.startswith("_LoraTestServer:")
            )
            assert len(replicas) == 2
            assert {
                replica.replica_id.unique_id
                for replica in replicas
                if adapter_id in replica.multiplexed_model_ids
            } == {cold_replica}
            # Controller publication precedes the router's long-poll update.
            # Probe selection without generating, so waiting cannot warm the
            # other replica and hide a missing adapter-affinity decision.
            router = next(
                replicas[0]
                for deployment, replicas in deployments.items()
                if deployment.name == "LLMRouter"
            )
            response = httpx.post(
                f"http://{router.node_ip}:{router.backend_http_port}/internal/route",
                json={"model": adapter_id, "prompt": "hello"},
                timeout=5,
            )
            assert response.status_code == 200, response.text
            assert response.json()["replica_id"].endswith(f"#{cold_replica}")
            return True

        wait_for_condition(adapter_is_advertised)
        with httpx.Client(base_url=base_url, timeout=30) as client:
            for _ in range(10):
                response = client.post(
                    "/v1/completions",
                    json={"model": adapter_id, "prompt": "hello", "max_tokens": 1},
                )
                assert response.status_code == 200, response.text
                assert response.headers["x-replica-id"] == cold_replica

    def test_base_model_request(self, base_url):
        """Enabling LoRA does not multiplex requests for the base model."""
        response = httpx.post(
            f"{base_url}/v1/completions",
            json={"model": "test-model", "prompt": "hello", "max_tokens": 1},
            timeout=30,
        )

        assert response.status_code == 200, response.text
        assert "[lora_model]" not in response.text

    @pytest.mark.parametrize("model", ["other:adapter", "test-model:missing-adapter"])
    def test_unknown_model(self, base_url, model):
        response = httpx.post(
            f"{base_url}/v1/completions",
            json={"model": model, "prompt": "hello", "max_tokens": 1},
            timeout=30,
        )

        assert response.status_code == 404
        if model == "test-model:missing-adapter":
            assert response.json()["error"]["code"] == 404
            assert (
                "Unable to find LoRA adapter config file"
                in response.json()["error"]["message"]
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
