import os
from typing import List, Dict

import yaml
from ray.llm._internal.serve.configs.server_models import GPUType


from ray.llm._internal.serve.config_generator.utils.constants import (
    DEFAULT_DEPLOYMENT_CONFIGS_FILE,
    TEMPLATE_DIR,
)


# All practical GPUs
ALL_GPU_TYPES = [
    GPUType.NVIDIA_L4,
    GPUType.NVIDIA_L40S,
    GPUType.NVIDIA_TESLA_A10G,
    GPUType.NVIDIA_A100_40G,
    GPUType.NVIDIA_A100_80G,
    GPUType.NVIDIA_H100,
]


def read_model_id_to_gpu_mapping() -> Dict[str, List[str]]:
    file_path = os.path.join(TEMPLATE_DIR, DEFAULT_DEPLOYMENT_CONFIGS_FILE)
    with open(file_path, "r") as stream:
        configs = yaml.safe_load(stream)
    model_id_configs = configs["model_id_to_gpu_deployment_configs"]
    return {
        model_id: list(model_config.keys())
        for model_id, model_config in model_id_configs.items()
    }


DEFAULT_MODEL_ID_TO_GPU: Dict[str, str] = read_model_id_to_gpu_mapping()


def get_available_gpu_types(model_id: str) -> List[GPUType]:
    """
    If we know the models run only on larger GPUs, we exclude the smaller GPUs from the options.
    """
    if model_id in DEFAULT_MODEL_ID_TO_GPU:
        gpus = [GPUType(gpu) for gpu in DEFAULT_MODEL_ID_TO_GPU[model_id]]
    else:
        gpus = ALL_GPU_TYPES
    return gpus
