from typing import Any, Dict, Optional

from ray.llm._internal.serve.observability.logging import get_logger

# TODO (genesu): double check if LLMConfig, LLMRawResponse need to be lazy imported
from ray.llm._internal.serve.configs.server_models import (
    ModelData,
    LLMConfig,
    LLMRawResponse,
)


logger = get_logger(__name__)


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

    @classmethod
    def from_model_response(cls, response: LLMRawResponse) -> "OpenAIHTTPException":
        return cls(
            status_code=response.error.code,
            message=response.error.message,
            type=response.error.type,
            internal_message=response.error.internal_message,
        )


def to_model_metadata(
    model_id: str,
    model_config: LLMConfig,
    overrides: Optional[Dict[str, Any]] = None,
):
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
        "generation": model_config.generation_config.model_dump(),
        "max_request_context_length": model_config.max_request_context_length,
    }

    if overrides:
        metadata.update(overrides)

    return ModelData(
        id=model_id,
        rayllm_metadata=metadata,
        object="model",
        owned_by="organization-owner",
        permission=[],
    )
