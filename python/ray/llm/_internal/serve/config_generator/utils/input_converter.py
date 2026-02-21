from typing import Optional

from ray.llm._internal.serve.config_generator.utils.gpu import GPUType
from ray.llm._internal.serve.config_generator.utils.models import (
    TextCompletionLoraModelConfig,
    TextCompletionModelConfig,
)


def convert_inputs_to_text_completion_model(
    *,
    model_id: str,
    gpu_type: GPUType,
    tensor_parallelism: int,
    hf_token: Optional[str] = None,
    remote_storage_uri: Optional[str] = None,
    lora_config: Optional[TextCompletionLoraModelConfig] = None,
    reference_model_id: Optional[str] = None,
) -> TextCompletionModelConfig:
    return TextCompletionModelConfig(
        id=model_id,
        hf_token=hf_token,
        remote_storage_uri=remote_storage_uri,
        gpu_type=gpu_type,
        tensor_parallelism=tensor_parallelism,
        lora_config=lora_config,
        reference_model_id=reference_model_id,
    )
