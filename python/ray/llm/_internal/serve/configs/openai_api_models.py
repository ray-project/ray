"""This module contains the wrapper classes for vLLM's OpenAI implementation.

If there are any major differences in the interface, the expectation is that
they will be upstreamed to vLLM.
"""

from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
from vllm.entrypoints.openai.protocol import (
    ChatCompletionRequest as vLLMChatCompletionRequest,
    ChatCompletionResponse as vLLMChatCompletionResponse,
    ChatCompletionStreamResponse as vLLMChatCompletionStreamResponse,
    CompletionRequest as vLLMCompletionRequest,
    CompletionResponse as vLLMCompletionResponse,
    CompletionStreamResponse as vLLMCompletionStreamResponse,
    EmbeddingChatRequest as vLLMEmbeddingChatRequest,
    EmbeddingCompletionRequest as vLLMEmbeddingCompletionRequest,
    EmbeddingResponse as vLLMEmbeddingResponse,
    ErrorResponse as vLLMErrorResponse,
)
from vllm.utils import random_uuid

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.server_models import LLMConfig


class ChatCompletionRequest(vLLMChatCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatCompletionResponse(vLLMChatCompletionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ChatCompletionStreamResponse(vLLMChatCompletionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class ErrorResponse(vLLMErrorResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


# TODO (Kourosh): Upstream
class CompletionRequest(vLLMCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    request_id: str = Field(
        default_factory=lambda: f"{random_uuid()}",
        description=(
            "The request_id related to this request. If the caller does "
            "not set it, a random_uuid will be generated. This id is used "
            "through out the inference process and return in response."
        ),
    )


class CompletionResponse(vLLMCompletionResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class CompletionStreamResponse(vLLMCompletionStreamResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


# TODO (Kourosh): Upstream
class EmbeddingCompletionRequest(vLLMEmbeddingCompletionRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    request_id: str = Field(
        default_factory=lambda: f"{random_uuid()}",
        description=(
            "The request_id related to this request. If the caller does "
            "not set it, a random_uuid will be generated. This id is used "
            "through out the inference process and return in response."
        ),
    )


class EmbeddingChatRequest(vLLMEmbeddingChatRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class EmbeddingResponse(vLLMEmbeddingResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


EmbeddingRequest = Union[EmbeddingCompletionRequest, EmbeddingChatRequest]

LLMEmbeddingsResponse = Union[
    AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None],
]

LLMChatResponse = Union[
    AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None],
]

LLMCompletionsResponse = Union[
    AsyncGenerator[
        Union[CompletionStreamResponse, CompletionResponse, ErrorResponse], None
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
