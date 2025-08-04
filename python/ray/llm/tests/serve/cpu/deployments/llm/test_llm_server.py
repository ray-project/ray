import asyncio
import sys
import time
from typing import AsyncGenerator, Optional
from unittest.mock import patch

import numpy as np
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
    # Set minimal lora_config to enable multiplexing but avoid telemetry S3 calls
    mock_llm_config.lora_config = LoraConfig(
        dynamic_lora_loading_path=None,  # No S3 path = no telemetry S3 calls
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


async def count_tpot_ms_from_stream(stream: AsyncGenerator) -> list[float]:
    all_tpots_in_ms = []
    start = None
    async for _ in stream:
        now = time.perf_counter()
        if start is not None:
            all_tpots_in_ms.append((now - start) * 1e3)
        start = now
    return all_tpots_in_ms


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
    async def test_check_health(self, mock_llm_config):
        """Test health check functionality."""

        # Mock the engine's check_health method
        class LocalMockEngine(MockVLLMEngine):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.check_health_called = False

            async def check_health(self):
                self.check_health_called = True

        # Create a server with a mocked engine
        server = LLMServer.sync_init(mock_llm_config, engine_cls=LocalMockEngine)
        await server.start()

        # Perform the health check, no exceptions should be raised
        await server.check_health()

        # Check that the health check method was called
        assert server.engine.check_health_called

    @pytest.mark.asyncio
    async def test_llm_config_property(self, mock_llm_config):
        """Test the llm_config property."""
        server = LLMServer.sync_init(mock_llm_config, engine_cls=MockVLLMEngine)
        await server.start()
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
    async def test_push_telemetry(self, mock_llm_config):
        """Test that the telemetry push is called properly."""
        with patch(
            "ray.llm._internal.serve.deployments.llm.llm_server.push_telemetry_report_for_all_models"
        ) as mock_push_telemetry:
            server = LLMServer.sync_init(mock_llm_config, engine_cls=MockVLLMEngine)
            await server.start()
            mock_push_telemetry.assert_called_once()

    @pytest.mark.parametrize("api_type", ["chat", "completions"])
    @pytest.mark.parametrize("stream", [True])
    @pytest.mark.parametrize("max_tokens", [64])
    @pytest.mark.parametrize("concurrency", [1, 16])
    @pytest.mark.parametrize("stream_batching_interval_ms", [0])
    @pytest.mark.asyncio
    async def test_stable_streaming_tpot(
        self,
        serve_handle,
        mock_llm_config,
        mock_chat_request,
        mock_completion_request,
        api_type: str,
        stream: bool,
        max_tokens: int,
        concurrency: int,
        stream_batching_interval_ms: int,
    ):
        """Test that the streaming TPOT is stable when batching is disabled."""

        # Create request based on API type
        if api_type == "chat":
            request = mock_chat_request
        elif api_type == "completions":
            request = mock_completion_request
        batched_chunks: list[AsyncGenerator] = [
            getattr(serve_handle, api_type).remote(request) for _ in range(concurrency)
        ]

        print(
            f"\n\n_____ {api_type.upper()} ({'STREAMING' if stream else 'NON-STREAMING'}) max_tokens={max_tokens} batching_interval_ms={stream_batching_interval_ms} _____\n\n"
        )

        # Collect responses from llm_server
        tpots_ms = await asyncio.gather(
            *[
                count_tpot_ms_from_stream(server_stream)
                for server_stream in batched_chunks
            ]
        )
        mean_llm_server = np.mean(tpots_ms)
        std_var_llm_server = np.std(tpots_ms)

        # Run same request with vllm engine
        vllm_engine = MockVLLMEngine(llm_config=mock_llm_config)
        await vllm_engine.start()
        engine_streams: list[AsyncGenerator] = [
            getattr(vllm_engine, api_type)(request) for _ in range(concurrency)
        ]
        tpots_ms_engine = await asyncio.gather(
            *[
                count_tpot_ms_from_stream(engine_stream)
                for engine_stream in engine_streams
            ]
        )
        mean_engine = np.mean(tpots_ms_engine)
        std_var_engine = np.std(tpots_ms_engine)

        assert np.isclose(
            mean_llm_server, mean_engine, rtol=0.1
        ), f"{mean_llm_server=}, {mean_engine=}"
        assert np.isclose(
            std_var_llm_server, std_var_engine, atol=1.0
        ), f"{std_var_llm_server=}, {std_var_engine=}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
