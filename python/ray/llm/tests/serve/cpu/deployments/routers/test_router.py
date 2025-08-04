import sys
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import openai
import pytest

from ray import serve
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine


@pytest.fixture(name="llm_config")
def create_llm_config(stream_batching_interval_ms: Optional[int] = None):

    if stream_batching_interval_ms is not None:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "stream_batching_interval_ms": stream_batching_interval_ms,
            },
        )
    else:
        return LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )


@pytest.fixture(name="client")
def create_router(llm_config: LLMConfig):
    ServerDeployment = LLMServer.as_deployment()
    RouterDeployment = LLMRouter.as_deployment(llm_configs=[llm_config])
    server = ServerDeployment.bind(llm_config, engine_cls=MockVLLMEngine)
    router = RouterDeployment.bind(llm_deployments=[server])
    serve.run(router)

    client = openai.Client(base_url="http://localhost:8000/v1", api_key="foo")
    yield client

    serve.shutdown()


class TestRouter:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("stream_batching_interval_ms", [None, 0, 10000])
    @pytest.mark.parametrize("stream", [True, False])
    async def test_chat(self, stream_batching_interval_ms, client, stream):
        """Tests chat streaming with different stream_batching_interval_ms values.

        0ms super fast batching (no batching)
        10000ms basically should be equivalent to non-streaming
        None is default, which is some fixed non-zero value.
        """

        # Generate 1000 chunks
        n_tokens = 1000

        response = client.chat.completions.create(
            model="llm_model_id",
            messages=[dict(role="user", content="Hello")],
            stream=stream,
            max_tokens=n_tokens,
        )

        if stream:
            text = ""
            role = None
            for chunk in response:
                if chunk.choices[0].delta.role is not None and role is None:
                    role = chunk.choices[0].delta.role
                if chunk.choices[0].delta.content:
                    text += chunk.choices[0].delta.content
        else:
            text = response.choices[0].message.content
            role = response.choices[0].message.role

        assert role == "assistant"
        assert text.strip() == " ".join([f"test_{i}" for i in range(n_tokens)])

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stream_batching_interval_ms", [None, 0, 10000])
    @pytest.mark.parametrize("stream", [True, False])
    async def test_completion(self, stream_batching_interval_ms, client, stream):
        """Tests text completions streaming with different stream_batching_interval_ms values."""

        # Generate tokens
        n_tokens = 1000

        response = client.completions.create(
            model="llm_model_id",
            prompt="Hello",
            stream=stream,
            max_tokens=n_tokens,
        )

        if stream:
            text = ""
            for chunk in response:
                text += chunk.choices[0].text
        else:
            text = response.choices[0].text

        # The mock engine produces "test_0 test_1 test_2 ..." pattern
        expected_text = " ".join([f"test_{i}" for i in range(n_tokens)])
        assert text.strip() == expected_text

    def test_router_with_num_router_replicas_config(self):
        """Test the router with num_router_replicas config."""
        # Test with no num_router_replicas config.
        llm_configs = [
            LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="llm_model_id",
                ),
            )
        ]
        llm_router_deployment = LLMRouter.as_deployment(llm_configs=llm_configs)
        autoscaling_config = llm_router_deployment._deployment_config.autoscaling_config
        assert autoscaling_config.min_replicas == 2
        assert autoscaling_config.initial_replicas == 2
        assert autoscaling_config.max_replicas == 2

        # Test with num_router_replicas config on multiple llm configs.
        llm_configs = [
            LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="llm_model_id",
                ),
                experimental_configs={
                    "num_router_replicas": 3,
                },
            ),
            LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="llm_model_id",
                ),
                experimental_configs={
                    "num_router_replicas": 5,
                },
            ),
        ]
        llm_router_deployment = LLMRouter.as_deployment(llm_configs=llm_configs)
        autoscaling_config = llm_router_deployment._deployment_config.autoscaling_config
        assert autoscaling_config.min_replicas == 5
        assert autoscaling_config.initial_replicas == 5
        assert autoscaling_config.max_replicas == 5

    @pytest.mark.asyncio
    async def test_check_health(self, llm_config: LLMConfig):
        """Test health check functionality."""

        server = MagicMock()
        server.llm_config = MagicMock()
        server.llm_config.remote = AsyncMock(return_value=llm_config)
        server.check_health = MagicMock()
        server.check_health.remote = AsyncMock()

        router = LLMRouter(llm_deployments=[server])

        await router.check_health()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
