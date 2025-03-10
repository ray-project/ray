from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field

from ray.llm._internal.serve.config_generator.utils.gpu import (
    GPUType,
)

MODEL_ID_TO_DEFAULT_CONFIG_FILE: Dict[str, str] = {
    "meta-llama/Meta-Llama-3.1-8B-Instruct": "meta-llama--Llama-3.1-8b-Instruct.yaml",
    "meta-llama/Meta-Llama-3.1-70B-Instruct": "meta-llama--Llama-3.1-70b-Instruct.yaml",
    "mistralai/Mistral-7B-Instruct-v0.1": "mistralai--Mistral-7B-Instruct-v0.1.yaml",
    "mistralai/Mixtral-8x7B-Instruct-v0.1": "mistralai--Mixtral-8x7B-Instruct-v0.1.yaml",
    "mistralai/Mixtral-8x22B-Instruct-v0.1": "mistralai--Mixtral-8x22B-Instruct-v0.1.yaml",
    "mistral-community/pixtral-12b": "mistral-community--pixtral-12b.yaml",
    "meta-llama/Llama-3.2-11B-Vision-Instruct": "meta-llama--Llama-3.2-11b-Vision-Instruct.yaml",
    "meta-llama/Llama-3.2-90B-Vision-Instruct": "meta-llama--Llama-3.2-90b-Vision-Instruct.yaml",
}


class DeploymentConfig(BaseModel):
    max_ongoing_requests: int
    max_num_batched_tokens: Optional[int] = None
    tensor_parallelism: int
    enable_chunked_prefill: Optional[bool] = None

    @property
    def target_ongoing_requests(self) -> int:
        """
        We currently apply this heuristic that target_ongoing_requests is 0.5 of max_ongoing_requests.
        """
        return int(self.max_ongoing_requests * 0.5)


TEXT_COMPLETION_MODEL_TYPE: Literal["TextCompletion"] = "TextCompletion"

ModelType = Literal["TextCompletion"]


class ServeModel(BaseModel):
    id: str
    hf_token: Optional[str] = None
    remote_storage_uri: Optional[str] = None
    gpu_type: GPUType


class TextCompletionLoraModelConfig(BaseModel):
    max_num_lora_per_replica: int
    uri: Optional[str] = Field(
        None,
        description="If not provided, we default to `$ANYSCALE_ARTIFACT_STORAGE/lora_fine_tuning`",
    )


class TextCompletionModelConfig(ServeModel):
    type: Literal["TextCompletion"] = TEXT_COMPLETION_MODEL_TYPE

    reference_model_id: Optional[str] = Field(
        None,
        description="This field only exists for custom user entered models whose serving defaults we don't have.",
    )
    tensor_parallelism: int
    lora_config: Optional[TextCompletionLoraModelConfig] = None
