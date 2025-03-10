from typing import Dict, Literal, Optional, Any

from pydantic import BaseModel, Field

from ray.llm._internal.serve.config_generator.utils.gpu import (
    GPUType,
)
import os
import yaml


from ray.llm._internal.serve.config_generator.utils.constants import (
    DEFAULT_DEPLOYMENT_CONFIGS_FILE,
    MODEL_CONFIGS_DIR,
    REFERENCE_BASE_MODEL_ID,
    TEMPLATE_DIR,
)

from ray.llm._internal.serve.configs.server_models import LLMConfig

def read_model_id_to_gpu_mapping() -> Dict[str, str]:
    file_path = os.path.join(TEMPLATE_DIR, DEFAULT_DEPLOYMENT_CONFIGS_FILE)
    with open(file_path, "r") as stream:
        configs = yaml.safe_load(stream)
    model_id_configs = configs["model_id_to_gpu_deployment_configs"]
    return {model_id: gpu_type for model_id, gpu_type in model_id_configs.items()}
    
    
MODEL_ID_TO_DEFAULT_CONFIG_FILE: Dict[str, str] = read_model_id_to_gpu_mapping()
# {
#     "meta-llama/Meta-Llama-3.1-8B-Instruct": "meta-llama--Llama-3.1-8b-Instruct.yaml",
#     "meta-llama/Meta-Llama-3.1-70B-Instruct": "meta-llama--Llama-3.1-70b-Instruct.yaml",
#     "mistralai/Mistral-7B-Instruct-v0.1": "mistralai--Mistral-7B-Instruct-v0.1.yaml",
#     "mistralai/Mixtral-8x7B-Instruct-v0.1": "mistralai--Mixtral-8x7B-Instruct-v0.1.yaml",
#     "mistralai/Mixtral-8x22B-Instruct-v0.1": "mistralai--Mixtral-8x22B-Instruct-v0.1.yaml",
#     "mistral-community/pixtral-12b": "mistral-community--pixtral-12b.yaml",
#     "meta-llama/Llama-3.2-11B-Vision-Instruct": "meta-llama--Llama-3.2-11b-Vision-Instruct.yaml",
#     "meta-llama/Llama-3.2-90B-Vision-Instruct": "meta-llama--Llama-3.2-90b-Vision-Instruct.yaml",
# }


class DeploymentConfig(LLMConfig):
    pass
    # class Config:
    #     arbitrary_types_allowed=True
    
    # deployment_config: Dict[str, Any] = Field(
    #     default_factory=dict,
    #     description="Deployment config for the model.",
    # )
    # engine_kwargs: Dict[str, Any] = Field(
    #     default_factory=dict,
    #     description="Engine kwargs for the engine.",
    # )
    
    


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
