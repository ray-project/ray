"""HTTP tests for Anthropic Messages API ingress."""

import sys
from typing import List
from unittest.mock import patch

import httpx
import pytest

import ray
from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_anthropic_app,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine

MESSAGES_BODY = {
    "model": "test-model",
    "max_tokens": 4,
    "messages": [{"role": "user", "content": "hi"}],
}

STREAMING_MESSAGES_BODY = {**MESSAGES_BODY, "stream": True}

COUNT_TOKENS_BODY = {
    "model": "test-model",
    "messages": [{"role": "user", "content": "hi there"}],
}


def _sse_event_names(body: str) -> List[str]:
    prefix = "event: "
    return [
        line[len(prefix) :] for line in body.splitlines() if line.startswith(prefix)
    ]


def _mock_llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="test-model"),
        runtime_env={
            "env_vars": {
                "RAYLLM_VLLM_ENGINE_CLS": (
                    "ray.llm.tests.serve.mocks.mock_vllm_engine.MockVLLMEngine"
                )
            }
        },
        log_engine_metrics=False,
    )


@pytest.fixture(scope="module")
def ray_instance():
    if not ray.is_initialized():
        ray.init()
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.fixture
def anthropic_app(ray_instance):
    llm_config = _mock_llm_config()
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        app = build_anthropic_app(LLMServingArgs(llm_configs=[llm_config]))
        serve.run(app, name="anthropic-app")
        yield llm_config.model_id
        serve.delete("anthropic-app", _blocking=True)


class TestAnthropicHttp:
    @pytest.mark.asyncio
    async def test_messages_non_stream(self, anthropic_app):
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "http://localhost:8000/v1/messages",
                json=MESSAGES_BODY,
            )

        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["type"] == "message"
        assert payload["role"] == "assistant"

    @pytest.mark.asyncio
    async def test_messages_stream(self, anthropic_app):
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "http://localhost:8000/v1/messages",
                json=STREAMING_MESSAGES_BODY,
            )

        assert response.status_code == 200, response.text
        assert response.headers["content-type"].startswith("text/event-stream")
        # The ingress peeks at the first chunk to detect errors, so every event
        # must reach the client exactly once and in the order the engine emitted.
        assert _sse_event_names(response.text) == [
            "message_start",
            "content_block_delta",
            "message_stop",
        ]

    @pytest.mark.asyncio
    async def test_count_tokens(self, anthropic_app):
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "http://localhost:8000/v1/messages/count_tokens",
                json=COUNT_TOKENS_BODY,
            )

        assert response.status_code == 200, response.text
        payload = response.json()
        assert "input_tokens" in payload
        assert payload["input_tokens"] > 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("endpoint", "body"),
        [
            ("/v1/messages", MESSAGES_BODY),
            ("/v1/messages/count_tokens", COUNT_TOKENS_BODY),
        ],
    )
    async def test_missing_model_error_envelope(self, anthropic_app, endpoint, body):
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"http://localhost:8000{endpoint}",
                json={
                    **body,
                    "model": "missing-model",
                },
            )

        assert response.status_code == 404
        payload = response.json()
        assert payload["type"] == "error"
        assert payload["error"]["type"] == "not_found_error"
        assert "missing-model" in payload["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "endpoint",
        ["/v1/messages", "/v1/messages/count_tokens"],
    )
    async def test_validation_error_envelope(self, anthropic_app, endpoint):
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"http://localhost:8000{endpoint}",
                json={},
            )

        assert response.status_code == 400
        payload = response.json()
        assert payload["type"] == "error"
        assert payload["error"]["type"] == "invalid_request_error"
        assert payload["error"]["message"]

    @pytest.mark.asyncio
    async def test_direct_streaming_messages(self):
        from fastapi.testclient import TestClient

        llm_config = _mock_llm_config()
        with patch.object(
            VLLMEngineConfig,
            "placement_bundles",
            new_callable=lambda: property(lambda self: []),
        ), patch(
            "ray.llm._internal.serve.core.server.llm_server."
            "push_telemetry_report_for_all_models"
        ):
            server = LLMServer.sync_init(llm_config, engine_cls=MockVLLMEngine)
            await server.start()
            app = await server.__serve_build_asgi_app__()

            with TestClient(app) as client:
                response = client.post("/v1/messages", json=MESSAGES_BODY)

        assert response.status_code == 200, response.text
        assert response.json()["type"] == "message"

    @pytest.mark.asyncio
    async def test_direct_streaming_count_tokens(self):
        from fastapi.testclient import TestClient

        llm_config = _mock_llm_config()
        with patch.object(
            VLLMEngineConfig,
            "placement_bundles",
            new_callable=lambda: property(lambda self: []),
        ), patch(
            "ray.llm._internal.serve.core.server.llm_server."
            "push_telemetry_report_for_all_models"
        ):
            server = LLMServer.sync_init(llm_config, engine_cls=MockVLLMEngine)
            await server.start()
            app = await server.__serve_build_asgi_app__()

            with TestClient(app) as client:
                response = client.post(
                    "/v1/messages/count_tokens", json=COUNT_TOKENS_BODY
                )

        assert response.status_code == 200, response.text
        assert response.json()["input_tokens"] > 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
