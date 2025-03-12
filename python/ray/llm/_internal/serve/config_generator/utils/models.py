from typing import Literal, Optional

from pydantic import BaseModel, Field

from ray.llm._internal.serve.config_generator.utils.gpu import GPUType


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
        description="If not provided, we default to `<RAYLLM_HOME_DIR>/lora_ckpts`",
    )


class TextCompletionModelConfig(ServeModel):
    type: Literal["TextCompletion"] = TEXT_COMPLETION_MODEL_TYPE

    reference_model_id: Optional[str] = Field(
        None,
        description="This field only exists for custom user entered models whose serving defaults we don't have.",
    )
    tensor_parallelism: int
    lora_config: Optional[TextCompletionLoraModelConfig] = None
