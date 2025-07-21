import sys
from typing import Optional
from unittest.mock import patch

import pytest

from ray import serve
from ray.llm._internal.serve.configs.server_models import LoraConfig
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.llm.tests.serve.mocks.mock_vllm_engine import (
    FakeLoraModelLoader,
    MockVLLMEngine,
)
from ray.llm.tests.serve.utils.testing_utils import LLMResponseValidator


@pytest.fixture
def serve_handle(mock_llm_config, stream_batching_interval_ms=0):
    mock_llm_config.experimental_configs = {
        "stream_batching_interval_ms": stream_batching_interval_ms,
    }

    app = serve.deployment(LLMServer).bind(mock_llm_config, engine_cls=MockVLLMEngine)
    handle = serve.run(app)
    # We set stream=True because the interfaces are async generators regardless
    # of the stream flag on request.
    handle = handle.options(stream=True)
    yield handle
    serve.shutdown()


@pytest.fixture
def multiplexed_serve_handle(mock_llm_config, stream_batching_interval_ms=0):
    mock_llm_config.experimental_configs = {
        "stream_batching_interval_ms": stream_batching_interval_ms,
    }
    mock_llm_config.lora_config = LoraConfig(
        dynamic_lora_loading_path="s3://my/s3/path_here",
        download_timeout_s=60,
        max_download_tries=3,
    )
    app = serve.deployment(LLMServer).bind(
        mock_llm_config,
        engine_cls=MockVLLMEngine,
        model_downloader=FakeLoraModelLoader,
    )
    handle = serve.run(app)
    handle = handle.options(stream=True, multiplexed_model_id="test_model_id")
    yield handle
    serve.shutdown()


class TestLLMServer:
    @pytest.mark.parametrize("api_type", ["chat", "completion"])
    @pytest.mark.parametrize("stream", [False, True])
    @pytest.mark.parametrize("max_tokens", [5])
    @pytest.mark.parametrize("stream_batching_interval_ms", [0, 10000])
    @pytest.mark.asyncio
    async def test_unified_llm_server(
        self,
        serve_handle,
        mock_llm_config,
        mock_chat_request,
        mock_completion_request,
        api_type: str,
        stream: bool,
        max_tokens: int,
        stream_batching_interval_ms: int,
    ):
        """Unified test for both chat and completion APIs, streaming and non-streaming."""

        # Create request based on API type
        if api_type == "chat":
            request = mock_chat_request
            batched_chunks = serve_handle.chat.remote(request)
        elif api_type == "completion":
            request = mock_completion_request
            batched_chunks = serve_handle.completions.remote(request)

        print(
            f"\n\n_____ {api_type.upper()} ({'STREAMING' if stream else 'NON-STREAMING'}) max_tokens={max_tokens} batching_interval_ms={stream_batching_interval_ms} _____\n\n"
        )

        if stream:
            # Collect responses from the stream
            chunks = []
            async for batch in batched_chunks:
                chunks.extend(batch)

            # Check that we got responses
            assert len(chunks) > 0

            # Validate streaming response
            LLMResponseValidator.validate_streaming_chunks(chunks, api_type, max_tokens)
        else:
            # Collect non-streaming response
            chunks = []
            async for batch in batched_chunks:
                chunks.append(batch)

            # Check that we got one response
            assert len(chunks) == 1

            # Validate non-streaming response
            LLMResponseValidator.validate_non_streaming_response(
                chunks[0], api_type, max_tokens
            )

    @pytest.mark.parametrize("dimensions", [None, 512])
    @pytest.mark.asyncio
    async def test_embedding_llm_server(
        self,
        serve_handle,
        mock_llm_config,
        mock_embedding_request,
        dimensions: Optional[int],
    ):
        """Test embedding API from LLMServer perspective."""

        # Create embedding request
        request = mock_embedding_request

        print(f"\n\n_____ EMBEDDING SERVER dimensions={dimensions} _____\n\n")

        # Get the response
        batched_chunks = serve_handle.embeddings.remote(request)

        # Collect responses (should be just one)
        chunks = []
        async for batch in batched_chunks:
            chunks.append(batch)

        # Check that we got one response
        assert len(chunks) == 1

        # Validate embedding response
        LLMResponseValidator.validate_embedding_response(chunks[0], dimensions)

    @pytest.mark.asyncio
    async def test_check_health(self, create_server, mock_llm_config):
        """Test health check functionality."""

        # Mock the engine's check_health method
        class LocalMockEngine(MockVLLMEngine):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.check_health_called = False

            async def check_health(self):
                self.check_health_called = True

        # Create a server with a mocked engine
        server = await create_server(mock_llm_config, engine_cls=LocalMockEngine)

        # Perform the health check, no exceptions should be raised
        await server.check_health()

        # Check that the health check method was called
        assert server.engine.check_health_called

    @pytest.mark.asyncio
    async def test_llm_config_property(self, create_server, mock_llm_config):
        """Test the llm_config property."""
        server = await create_server(mock_llm_config, engine_cls=MockVLLMEngine)
        llm_config = await server.llm_config()
        assert isinstance(llm_config, type(mock_llm_config))

    @pytest.mark.parametrize("stream", [False])
    @pytest.mark.parametrize("max_tokens", [5])
    @pytest.mark.asyncio
    async def test_request_id_handling(
        self,
        serve_handle,
        mock_llm_config,
        mock_chat_request,
        stream: bool,
        max_tokens: int,
    ):
        """Test that the request id is handled correctly."""

        # Create a chat completion request
        # We should patch get_server_request_id to return a test_request_id
        serve.context._serve_request_context.set(
            serve.context._RequestContext(**{"request_id": "test_request_id"})
        )
        # Get the response
        chunks = []
        async for chunk in serve_handle.chat.remote(mock_chat_request):
            chunks.append(chunk)

        assert len(chunks) == 1
        assert chunks[0].id == "test_request_id"

    @pytest.mark.parametrize("api_type", ["chat", "completion"])
    @pytest.mark.parametrize("stream", [False, True])
    @pytest.mark.parametrize("max_tokens", [5])
    @pytest.mark.parametrize("stream_batching_interval_ms", [0, 10000])
    @pytest.mark.asyncio
    async def test_multiplexed_request_handling(
        self,
        multiplexed_serve_handle,
        mock_chat_request,
        mock_completion_request,
        api_type: str,
        stream: bool,
        max_tokens: int,
        stream_batching_interval_ms: int,
    ):
        """Unified test for multiplexed (LoRA) requests - both chat and completion APIs, streaming and non-streaming."""

        # Create request based on API type and set model ID for multiplexing
        if api_type == "chat":
            request = mock_chat_request
            batched_chunks = multiplexed_serve_handle.chat.remote(request)
        elif api_type == "completion":
            request = mock_completion_request
            batched_chunks = multiplexed_serve_handle.completions.remote(request)

        request.model = "test_model_id"
        print(
            f"\n\n_____ MULTIPLEXED {api_type.upper()} ({'STREAMING' if stream else 'NON-STREAMING'}) max_tokens={max_tokens} batching_interval_ms={stream_batching_interval_ms} _____\n\n"
        )

        if stream:
            # Collect responses from the stream
            chunks = []
            async for batch in batched_chunks:
                if isinstance(batch, list):
                    chunks.extend(batch)
                else:
                    chunks.append(batch)

            # Check that we got responses
            assert len(chunks) > 0

            # Validate streaming response with LoRA model ID
            LLMResponseValidator.validate_streaming_chunks(
                chunks, api_type, max_tokens, lora_model_id=request.model
            )
        else:
            # Collect non-streaming response
            chunks = []
            async for batch in batched_chunks:
                if isinstance(batch, list):
                    chunks.extend(batch)
                else:
                    chunks.append(batch)

            # Check that we got one response
            assert len(chunks) == 1

            # Validate non-streaming response with LoRA model ID
            LLMResponseValidator.validate_non_streaming_response(
                chunks[0], api_type, max_tokens, lora_model_id=request.model
            )

    @pytest.mark.asyncio
    async def test_push_telemetry(self, create_server, mock_llm_config):
        """Test that the telemetry push is called properly."""
        with patch(
            "ray.llm._internal.serve.deployments.llm.llm_server.push_telemetry_report_for_all_models"
        ) as mock_push_telemetry:
            await create_server(mock_llm_config, engine_cls=MockVLLMEngine)
            mock_push_telemetry.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
