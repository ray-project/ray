"""This tests the LLM engine by testing the mocked implementations directly.

This implicitly tests the consistency of the engine API through time.
Also tests that our Mock is behaving as expected to ensure that the downstream tests using Mocks are correct from Mock implementation perspective.


We have the following Mock:

- An engine that returns a string of form "test_i" for i in range(max_tokens)
"""

import sys
from typing import Optional

import pytest

from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine
from ray.llm.tests.serve.utils.testing_utils import LLMResponseValidator


class TestMockLLMEngine:
    @pytest.mark.parametrize("api_type", ["chat", "completion"])
    @pytest.mark.parametrize("stream", [False, True])
    @pytest.mark.parametrize("max_tokens", [5])
    @pytest.mark.asyncio
    async def test_unified_llm_engine(
        self,
        mock_llm_config,
        mock_chat_request,
        mock_completion_request,
        api_type: str,
        stream: bool,
        max_tokens: int,
    ):
        """Unified test for both chat and completion APIs, streaming and non-streaming."""
        # Create and start the engine
        engine = MockVLLMEngine(mock_llm_config)
        await engine.start()

        # Create request based on API type
        if api_type == "chat":
            request = mock_chat_request
            response_generator = engine.chat(request)
        elif api_type == "completion":
            request = mock_completion_request
            response_generator = engine.completions(request)

        print(
            f"\n\n_____ {api_type.upper()} ({'STREAMING' if stream else 'NON-STREAMING'}) max_tokens={max_tokens} _____\n\n"
        )

        if stream:
            # Collect streaming chunks
            chunks = []
            async for chunk in response_generator:
                assert isinstance(chunk, str)
                chunks.append(chunk)

            # Validate streaming response
            LLMResponseValidator.validate_streaming_chunks(chunks, api_type, max_tokens)
        else:
            # Validate non-streaming response
            async for response in response_generator:
                LLMResponseValidator.validate_non_streaming_response(
                    response, api_type, max_tokens
                )

    @pytest.mark.parametrize("dimensions", [None, 512])
    @pytest.mark.asyncio
    async def test_embedding_mock_engine(
        self, mock_llm_config, mock_embedding_request, dimensions: Optional[int]
    ):
        """Test embedding API with different dimensions."""
        # Create and start the engine
        engine = MockVLLMEngine(mock_llm_config)
        await engine.start()

        # Create embedding request
        request = mock_embedding_request

        print(f"\n\n_____ EMBEDDING dimensions={dimensions} _____\n\n")

        async for response in engine.embeddings(request):
            LLMResponseValidator.validate_embedding_response(response, dimensions)

    @pytest.mark.asyncio
    async def test_score_mock_engine(self, mock_llm_config, mock_score_request):
        """Test score API for text similarity."""
        # Create and start the engine
        engine = MockVLLMEngine(mock_llm_config)
        await engine.start()

        # Create score request
        request = mock_score_request

        print("\n\n_____ SCORE _____\n\n")

        async for response in engine.score(request):
            LLMResponseValidator.validate_score_response(response)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
