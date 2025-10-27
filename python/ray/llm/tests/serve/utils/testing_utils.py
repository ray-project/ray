"""Shared testing utilities for Ray LLM serve tests.

This is written with assumptions around how mocks for testing are expected to behave.
"""

import json
import re
from typing import List, Optional, Union

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionResponse,
    CompletionResponse,
    EmbeddingResponse,
    ScoreResponse,
    TranscriptionResponse,
)


class LLMResponseValidator:
    """Reusable validation logic for LLM responses."""

    @staticmethod
    def get_expected_content(
        api_type: str, max_tokens: int, lora_model_id: str = ""
    ) -> str:
        """Get expected content based on API type."""
        expected_content = " ".join(f"test_{i}" for i in range(max_tokens))
        if lora_model_id:
            expected_content = f"[lora_model] {lora_model_id}: {expected_content}"
        return expected_content

    @staticmethod
    def validate_non_streaming_response(
        response: Union[ChatCompletionResponse, CompletionResponse],
        api_type: str,
        max_tokens: int,
        lora_model_id: str = "",
    ):
        """Validate non-streaming responses."""
        expected_content = LLMResponseValidator.get_expected_content(
            api_type, max_tokens, lora_model_id
        )

        if api_type == "chat":
            assert isinstance(response, ChatCompletionResponse)
            assert response.choices[0].message.content == expected_content
        elif api_type == "completion":
            assert isinstance(response, CompletionResponse)
            assert response.choices[0].text == expected_content

    @staticmethod
    def validate_streaming_chunks(
        chunks: List[str], api_type: str, max_tokens: int, lora_model_id: str = ""
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

            expected_chunk = f"test_{chunk_iter}"
            if lora_model_id and chunk_iter == 0:
                expected_chunk = f"[lora_model] {lora_model_id}: {expected_chunk}"

            if api_type == "chat":
                delta = chunk_data["choices"][0]["delta"]
                if chunk_iter == 0:
                    assert delta["role"] == "assistant"
                else:
                    assert delta["role"] is None
                assert delta["content"].strip() == expected_chunk
            elif api_type == "completion":
                text = chunk_data["choices"][0]["text"]
                assert text.strip() == expected_chunk

    @staticmethod
    def validate_embedding_response(
        response: EmbeddingResponse, expected_dimensions: Optional[int] = None
    ):
        """Validate embedding responses."""
        assert isinstance(response, EmbeddingResponse)
        assert response.object == "list"
        assert len(response.data) == 1
        assert response.data[0].object == "embedding"
        assert isinstance(response.data[0].embedding, list)
        assert (
            len(response.data[0].embedding) > 0
        )  # Should have some embedding dimensions
        assert response.data[0].index == 0

        # Check dimensions if specified
        if expected_dimensions:
            assert len(response.data[0].embedding) == expected_dimensions

    @staticmethod
    def validate_score_response(response: ScoreResponse):
        """Validate score responses."""
        assert isinstance(response, ScoreResponse)
        assert response.object == "list"
        assert len(response.data) >= 1

        # Validate each score data element
        for i, score_data in enumerate(response.data):
            assert score_data.object == "score"
            assert isinstance(score_data.score, float)
            assert score_data.index == i  # Index should match position in list

    @staticmethod
    def validate_transcription_response(
        response: Union[TranscriptionResponse, List[str]],
        temperature: float,
        language: Optional[str] = None,
        lora_model_id: str = "",
    ):
        """Validate transcription responses for both streaming and non-streaming."""
        if isinstance(response, list):
            # Streaming response - validate chunks
            LLMResponseValidator.validate_transcription_streaming_chunks(
                response, temperature, language, lora_model_id
            )
        else:
            # Non-streaming response
            assert isinstance(response, TranscriptionResponse)
            assert hasattr(response, "text")
            assert isinstance(response.text, str)
            assert len(response.text) > 0

            # Check that the response contains expected language and temperature info
            expected_text = f"Mock transcription in {language} language with temperature {temperature}"
            if lora_model_id:
                expected_text = f"[lora_model] {lora_model_id}: {expected_text}"
            assert response.text == expected_text

            # Validate usage information
            if hasattr(response, "usage"):
                assert hasattr(response.usage, "seconds")
                assert hasattr(response.usage, "type")
                assert response.usage.seconds > 0
                assert response.usage.type == "duration"

    @staticmethod
    def validate_transcription_streaming_chunks(
        chunks: List[str],
        temperature: float,
        language: Optional[str] = None,
        lora_model_id: str = "",
    ):
        """Validate streaming transcription response chunks."""
        # Should have at least one chunk (transcription text) + final chunk + [DONE]
        assert len(chunks) >= 3

        # Validate each chunk except the last [DONE] chunk
        transcription_chunks = []
        for chunk in chunks[:-1]:  # Exclude the final [DONE] chunk
            pattern = r"data: (.*)\n\n"
            match = re.match(pattern, chunk)
            assert match is not None
            chunk_data = json.loads(match.group(1))

            # Validate chunk structure
            assert "id" in chunk_data
            assert "object" in chunk_data
            assert chunk_data["object"] == "transcription.chunk"
            assert "delta" in chunk_data
            assert chunk_data["delta"] is None
            assert "type" in chunk_data
            assert chunk_data["type"] is None
            assert "logprobs" in chunk_data
            assert chunk_data["logprobs"] is None
            assert "choices" in chunk_data
            assert len(chunk_data["choices"]) == 1

            choice = chunk_data["choices"][0]
            assert "delta" in choice
            assert "content" in choice["delta"]

            # Collect text for final validation
            if choice["delta"]["content"]:
                transcription_chunks.append(choice["delta"]["content"])

        # Validate final transcription text
        full_transcription = "".join(transcription_chunks)
        expected_text = (
            f"Mock transcription in {language} language with temperature {temperature}"
        )
        if lora_model_id:
            expected_text = f"[lora_model] {lora_model_id}: {expected_text}"
        assert full_transcription.strip() == expected_text.strip()

        # Validate final [DONE] chunk
        assert chunks[-1] == "data: [DONE]\n\n"
