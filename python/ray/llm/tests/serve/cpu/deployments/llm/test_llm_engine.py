"""This tests the LLM engine by testing the mocked implementations directly.

This implicitly tests the consistency of the engine API through time.
Also tests that our Mock is behaving as expected to ensure that the downstream tests using Mocks are correct from Mock implementation perspective.


We have the following Mocks:

- An engine that returns a string of form "test_i" for i in range(max_tokens)
- An engine that echos the sent request in its response
- An engine that excercises the multiplexing logic (e.g. LoRA)
- An engine that excercise the structured output logic (e.g. JSON mode)
- An engine that excercises the prefill-disaggregation logic
"""

from ray.llm.tests.serve.mocks.mock_vllm_engine import MockVLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest, 
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingCompletionRequest,
    EmbeddingResponse
)
import pytest
import re
import json
from typing import Union, List, AsyncGenerator, Optional


class LLMResponseValidator:
    """Reusable validation logic for LLM responses."""
    
    @staticmethod
    def get_expected_content(api_type: str, max_tokens: int) -> str:
        """Get expected content based on API type."""
        return " ".join(f"test_{i}" for i in range(max_tokens))

    @staticmethod
    def validate_non_streaming_response(
        response: Union[ChatCompletionResponse, CompletionResponse], 
        api_type: str, 
        max_tokens: int
    ):
        """Validate non-streaming responses."""
        expected_content = LLMResponseValidator.get_expected_content(api_type, max_tokens)
        
        if api_type == "chat":
            assert isinstance(response, ChatCompletionResponse)
            assert response.choices[0].message.content == expected_content
        elif api_type == "completion":
            assert isinstance(response, CompletionResponse)
            assert response.choices[0].text == expected_content

    @staticmethod
    def validate_streaming_chunks(
        chunks: List[str], 
        api_type: str, 
        max_tokens: int
    ):
        """Validate streaming response chunks."""
        # Should have max_tokens + 1 chunks (tokens + [DONE])
        assert len(chunks) == max_tokens + 1
        
        # Validate each chunk except the last [DONE] chunk
        for chunk_iter, chunk in enumerate(chunks[:-1]):
            pattern = r"data: (.*)\n\n"
            match = re.match(pattern, chunk)
            assert match is not None
            chunk_data = json.loads(match.group(1))
            
            if api_type == "chat":
                delta = chunk_data["choices"][0]["delta"]
                if chunk_iter == 0:
                    assert delta["role"] == "assistant"
                else:
                    assert delta["role"] is None
                assert delta["content"].strip() == f"test_{chunk_iter}"
            elif api_type == "completion":
                text = chunk_data["choices"][0]["text"]
                assert text.strip() == f"test_{chunk_iter}"

    @staticmethod
    def validate_embedding_response(
        response: EmbeddingResponse, 
        expected_dimensions: Optional[int] = None
    ):
        """Validate embedding responses."""
        assert isinstance(response, EmbeddingResponse)
        assert response.object == "list"
        assert len(response.data) == 1
        assert response.data[0].object == "embedding"
        assert isinstance(response.data[0].embedding, list)
        assert len(response.data[0].embedding) > 0  # Should have some embedding dimensions
        assert response.data[0].index == 0
        
        # Check dimensions if specified
        if expected_dimensions:
            assert len(response.data[0].embedding) == expected_dimensions


@pytest.fixture
def llm_config():
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="mock-model"),
        runtime_env={},
        log_engine_metrics=False,
    )


@pytest.fixture
def chat_request(stream, max_tokens):
    """Fixture for creating chat completion requests."""
    return ChatCompletionRequest(
        model="mock-model",
        messages=[
            {"role": "user", "content": "Hello, world!"}
        ],
        max_tokens=max_tokens,
        stream=stream,
    )


@pytest.fixture
def completion_request(stream, max_tokens):
    """Fixture for creating text completion requests."""
    return CompletionRequest(
        model="mock-model",
        prompt="Complete this text:",
        max_tokens=max_tokens,
        stream=stream,
    )


@pytest.fixture
def embedding_request(dimensions):
    """Fixture for creating embedding requests."""
    request = EmbeddingCompletionRequest(
        model="mock-model",
        input="Text to embed",
    )
    if dimensions:
        request.dimensions = dimensions
    return request


class TestMockLLMEngine:

    @pytest.mark.parametrize("api_type", ["chat", "completion"])
    @pytest.mark.parametrize("stream", [False, True])
    @pytest.mark.parametrize("max_tokens", [5, 10, 15])
    @pytest.mark.asyncio
    async def test_unified_llm_engine(
        self, 
        llm_config, 
        chat_request, 
        completion_request,
        api_type: str, 
        stream: bool, 
        max_tokens: int
    ):
        """Unified test for both chat and completion APIs, streaming and non-streaming."""
        # Create and start the engine
        engine = MockVLLMEngine(llm_config)
        await engine.start()
        
        # Create request based on API type
        if api_type == "chat":
            request = chat_request
            response_generator = engine.chat(request)
        elif api_type == "completion":
            request = completion_request
            response_generator = engine.completions(request)
        
        print(f"\n\n_____ {api_type.upper()} ({'STREAMING' if stream else 'NON-STREAMING'}) max_tokens={max_tokens} _____\n\n")
        
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
                LLMResponseValidator.validate_non_streaming_response(response, api_type, max_tokens)

    @pytest.mark.parametrize("dimensions", [None, 512])
    @pytest.mark.asyncio 
    async def test_embedding_mock_engine(
        self, 
        llm_config, 
        embedding_request, 
        dimensions: Optional[int]
    ):
        """Test embedding API with different dimensions."""
        # Create and start the engine
        engine = MockVLLMEngine(llm_config)
        await engine.start()
        
        # Create embedding request
        request = embedding_request
        
        print(f"\n\n_____ EMBEDDING dimensions={dimensions} _____\n\n")
        
        async for response in engine.embeddings(request):
            LLMResponseValidator.validate_embedding_response(response, dimensions)

