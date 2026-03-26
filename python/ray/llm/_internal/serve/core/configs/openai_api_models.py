"""This module contains wrapper classes for OpenAI-compatible protocol models.

Supports both vLLM and SGLang as the underlying engine. vLLM is tried first;
on ImportError, SGLang models are imported as a fallback. If neither is
installed, an ImportError is raised at import time.
"""

import uuid
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

try:
    from vllm.entrypoints.openai.chat_completion.protocol import (
        ChatCompletionRequest as _ChatCompletionRequest,
        ChatCompletionResponse as _ChatCompletionResponse,
        ChatCompletionStreamResponse as _ChatCompletionStreamResponse,
    )
    from vllm.entrypoints.openai.completion.protocol import (
        CompletionRequest as _CompletionRequest,
        CompletionResponse as _CompletionResponse,
        CompletionStreamResponse as _CompletionStreamResponse,
    )
    from vllm.entrypoints.openai.engine.protocol import (
        ErrorInfo as _ErrorInfo,
        ErrorResponse as _ErrorResponse,
    )
    from vllm.entrypoints.openai.speech_to_text.protocol import (
        TranscriptionRequest as _TranscriptionRequest,
        TranscriptionResponse as _TranscriptionResponse,
        TranscriptionStreamResponse as _TranscriptionStreamResponse,
    )
    from vllm.entrypoints.pooling.embed.protocol import (
        EmbeddingChatRequest as _EmbeddingChatRequest,
        EmbeddingCompletionRequest as _EmbeddingCompletionRequest,
        EmbeddingResponse as _EmbeddingResponse,
    )
    from vllm.entrypoints.pooling.score.protocol import (
        ScoreResponse as _ScoreResponse,
        ScoreTextRequest as _ScoreTextRequest,
    )
    from vllm.entrypoints.serve.tokenize.protocol import (
        DetokenizeRequest as _DetokenizeRequest,
        DetokenizeResponse as _DetokenizeResponse,
        TokenizeChatRequest as _TokenizeChatRequest,
        TokenizeCompletionRequest as _TokenizeCompletionRequest,
        TokenizeResponse as _TokenizeResponse,
    )

except ImportError:
    try:
        from sglang.srt.entrypoints.openai.protocol import (
            ChatCompletionRequest as _ChatCompletionRequest,
            ChatCompletionResponse as _ChatCompletionResponse,
            ChatCompletionStreamResponse as _ChatCompletionStreamResponse,
            CompletionRequest as _CompletionRequest,
            CompletionResponse as _CompletionResponse,
            CompletionStreamResponse as _CompletionStreamResponse,
            DetokenizeRequest as _DetokenizeRequest,
            DetokenizeResponse as _DetokenizeResponse,
            EmbeddingRequest as _EmbeddingCompletionRequest,
            EmbeddingResponse as _EmbeddingResponse,
            ScoringRequest as _ScoreTextRequest,
            ScoringResponse as _ScoreResponse,
            TokenizeRequest as _TokenizeCompletionRequest,
            TokenizeResponse as _TokenizeResponse,
        )
    except ImportError:
        raise ImportError(
            "Neither vLLM nor SGLang is installed. At least one is required "
            "for Ray Serve LLM protocol models. Install with: "
            "`pip install ray[llm]` or `pip install sglang[all]`"
        )

    def _unsupported_model(name: str, feature: str = ""):
        """Create a BaseModel stub that raises NotImplementedError on instantiation."""
        msg = f"{name} is not supported with the current backend." + (
            f" {feature}" if feature else ""
        )

        class _Stub(BaseModel):
            model_config = ConfigDict(arbitrary_types_allowed=True)

            def __init__(self, **kwargs):
                raise NotImplementedError(msg)

        _Stub.__name__ = _Stub.__qualname__ = name
        return _Stub

    # SGLang does not provide transcription protocol models.
    _vllm_hint = "Install vLLM to use transcription endpoints."
    _TranscriptionRequest = _unsupported_model("TranscriptionRequest", _vllm_hint)
    _TranscriptionResponse = _unsupported_model("TranscriptionResponse", _vllm_hint)
    _TranscriptionStreamResponse = _unsupported_model(
        "TranscriptionStreamResponse", _vllm_hint
    )

    # SGLang has no equivalent to vLLM's nested ErrorResponse.error -> ErrorInfo
    # pattern, so we define our own.

    class _ErrorInfo(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        message: str
        type: str
        param: Optional[str] = None
        code: int

    class _ErrorResponse(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        error: _ErrorInfo

    _EmbeddingChatRequest = _EmbeddingCompletionRequest
    _TokenizeChatRequest = _TokenizeCompletionRequest


if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


class ChatCompletionRequest(_ChatCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatCompletionResponse(_ChatCompletionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatCompletionStreamResponse(_ChatCompletionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ErrorInfo(_ErrorInfo):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ErrorResponse(_ErrorResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


# TODO (Kourosh): Upstream
class CompletionRequest(_CompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CompletionResponse(_CompletionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CompletionStreamResponse(_CompletionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


# TODO (Kourosh): Upstream
class EmbeddingCompletionRequest(_EmbeddingCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class EmbeddingChatRequest(_EmbeddingChatRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class EmbeddingResponse(_EmbeddingResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TranscriptionRequest(_TranscriptionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    request_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description=(
            "The request_id related to this request. If the caller does "
            "not set it, a random_uuid will be generated. This id is used "
            "through out the inference process and return in response."
        ),
    )


class TranscriptionResponse(_TranscriptionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TranscriptionStreamResponse(_TranscriptionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ScoreRequest(_ScoreTextRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ScoreResponse(_ScoreResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TokenizeCompletionRequest(_TokenizeCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TokenizeChatRequest(_TokenizeChatRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class TokenizeResponse(_TokenizeResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class DetokenizeRequest(_DetokenizeRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class DetokenizeResponse(_DetokenizeResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


EmbeddingRequest = Union[EmbeddingCompletionRequest, EmbeddingChatRequest]

TokenizeRequest = Union[TokenizeCompletionRequest, TokenizeChatRequest]

LLMEmbeddingsResponse = Union[
    AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None],
]

LLMScoreResponse = Union[
    AsyncGenerator[Union[ScoreResponse, ErrorResponse], None],
]

LLMTokenizeResponse = Union[
    AsyncGenerator[Union[TokenizeResponse, ErrorResponse], None],
]

LLMDetokenizeResponse = Union[
    AsyncGenerator[Union[DetokenizeResponse, ErrorResponse], None],
]

LLMChatResponse = Union[
    AsyncGenerator[
        Union[str, ChatCompletionStreamResponse, ChatCompletionResponse, ErrorResponse],
        None,
    ],
]

LLMCompletionsResponse = Union[
    AsyncGenerator[
        Union[str, CompletionStreamResponse, CompletionResponse, ErrorResponse], None
    ],
]

LLMTranscriptionResponse = Union[
    AsyncGenerator[
        Union[str, TranscriptionStreamResponse, TranscriptionResponse, ErrorResponse],
        None,
    ],
]


# TODO: remove this class
class OpenAIHTTPException(Exception):
    def __init__(
        self,
        status_code: int,
        message: str,
        type: str = "Unknown",
        internal_message: Optional[str] = None,
    ) -> None:
        self.status_code = status_code
        self.message = message
        self.type = type
        self.internal_message = internal_message


# TODO: upstream metadata for ModelData
# Compared to vLLM this has a metadata field.
class ModelCard(BaseModel):
    model_config = ConfigDict(
        protected_namespaces=tuple(), arbitrary_types_allowed=True
    )

    id: str
    object: str
    owned_by: str
    permission: List[str]
    metadata: Dict[str, Any]

    @property
    def model_type(self) -> str:
        return self.metadata["engine_config"]["model_type"]


class ModelList(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    data: List[ModelCard]
    object: str = "list"


def to_model_metadata(
    model_id: str,
    model_config: "LLMConfig",
    overrides: Optional[Dict[str, Any]] = None,
) -> ModelCard:
    """Creates an OpenAI-compatible ModelData object.

    Args:
        model_id: The ID of the model. Should contain the suffix if the model
            is LoRA fine-tuned. For example:
                meta-llama/Llama-2-7b-chat-hf:my_suffix:aBc1234
        model_config: The model's YAML config.
        overrides: should only be set for LoRA fine-tuned models. The
            overrides of the fine-tuned model metadata.

    Returns:
        A ModelCard object.
    """
    metadata = {
        "model_id": model_config.model_id,
        "input_modality": model_config.input_modality,
        "max_request_context_length": model_config.max_request_context_length,
    }

    if overrides:
        metadata.update(overrides)

    return ModelCard(
        id=model_id,
        object="model",
        owned_by="organization-owner",
        permission=[],
        metadata=metadata,
    )
