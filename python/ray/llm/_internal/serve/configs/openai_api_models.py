from typing import Union, AsyncGenerator, Optional, Dict, Any, List

from pydantic import (
    BaseModel,
    ConfigDict,
)

from vllm.entrypoints.openai.protocol import (
    ChatCompletionRequest as vLLMChatCompletionRequest,
    ChatCompletionResponse as vLLMChatCompletionResponse,
    ChatCompletionStreamResponse as vLLMChatCompletionStreamResponse,
    ErrorResponse as vLLMErrorResponse,
    CompletionRequest as vLLMCompletionRequest,
    CompletionResponse as vLLMCompletionResponse,
    CompletionStreamResponse as vLLMCompletionStreamResponse,
    EmbeddingCompletionRequest as vLLMEmbeddingCompletionRequest,
    EmbeddingChatRequest as vLLMEmbeddingChatRequest,
    EmbeddingResponse as vLLMEmbeddingResponse,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.server_models import LLMConfig


class ChatCompletionRequest(vLLMChatCompletionRequest):
    pass


class ChatCompletionResponse(vLLMChatCompletionResponse):
    pass


class ChatCompletionStreamResponse(vLLMChatCompletionStreamResponse):
    pass


class ErrorResponse(vLLMErrorResponse):
    pass


class CompletionRequest(vLLMCompletionRequest):
    pass


class CompletionResponse(vLLMCompletionResponse):
    pass


class CompletionStreamResponse(vLLMCompletionStreamResponse):
    pass


class EmbeddingCompletionRequest(vLLMEmbeddingCompletionRequest):
    pass


class EmbeddingChatRequest(vLLMEmbeddingChatRequest):
    pass


class EmbeddingResponse(vLLMEmbeddingResponse):
    pass


EmbeddingRequest = Union[EmbeddingCompletionRequest, EmbeddingChatRequest]

LLMEmbeddingsResponse = Union[
    AsyncGenerator[Union[EmbeddingResponse, ErrorResponse], None],
]

LLMChatResponse = Union[
    AsyncGenerator[
        Union[ChatCompletionStreamResponse, ChatCompletionResponse, ErrorResponse], None
    ],
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
    model_config = ConfigDict(protected_namespaces=tuple())

    id: str
    object: str
    owned_by: str
    permission: List[str]
    metadata: Dict[str, Any]

    @property
    def model_type(self) -> str:
        return self.metadata["engine_config"]["model_type"]


class ModelList(BaseModel):
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
