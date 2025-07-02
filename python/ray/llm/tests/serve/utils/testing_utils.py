"""Shared testing utilities for Ray LLM serve tests."""

import json
import re
from typing import Union, List, Optional

from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionResponse,
    CompletionResponse,
    EmbeddingResponse
)


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