import os
from typing import List

from ray.llm._internal.serve.configs.server_models import GPUType

_MODEL_ID_TO_GPU_TYPES = {
    "meta-llama/Meta-Llama-3.1-70B-Instruct": [
        GPUType.NVIDIA_A100_40G,
        GPUType.NVIDIA_A100_80G,
        GPUType.NVIDIA_H100,
    ],
    "meta-llama/Meta-Llama-3.1-405B-Instruct-FP8": [GPUType.NVIDIA_H100],
    "mistralai/Mixtral-8x7B-Instruct-v0.1": [
        GPUType.NVIDIA_A100_40G,
        GPUType.NVIDIA_A100_80G,
        GPUType.NVIDIA_H100,
    ],
    "mistralai/Mixtral-8x22B-Instruct-v0.1": [
        GPUType.NVIDIA_A100_80G,
        GPUType.NVIDIA_H100,
    ],
    "mistral-community/pixtral-12b": [
        GPUType.NVIDIA_L40S,
        GPUType.NVIDIA_A100_40G,
        GPUType.NVIDIA_A100_80G,
        GPUType.NVIDIA_H100,
    ],
    "meta-llama/Llama-3.2-11B-Vision-Instruct": [
        GPUType.NVIDIA_L40S,
        GPUType.NVIDIA_A100_40G,
        GPUType.NVIDIA_A100_80G,
        GPUType.NVIDIA_H100,
    ],
    "meta-llama/Llama-3.2-90B-Vision-Instruct": [
        GPUType.NVIDIA_A100_40G,
        GPUType.NVIDIA_A100_80G,
        GPUType.NVIDIA_H100,
    ],
}

AWS_GPU_TYPES = [
    GPUType.NVIDIA_L4,
    GPUType.NVIDIA_L40S,
    GPUType.NVIDIA_TESLA_A10G,
    GPUType.NVIDIA_A100_40G,
    GPUType.NVIDIA_A100_80G,
    GPUType.NVIDIA_H100,
]
GCP_GPU_TYPES = [
    GPUType.NVIDIA_L4,
    GPUType.NVIDIA_A100_40G,
    GPUType.NVIDIA_A100_80G,
    GPUType.NVIDIA_H100,
]


def validate_gpu_type_on_cloud_env(gpu_type: GPUType):
    artifact_storage = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "")
    if artifact_storage.startswith("s3"):
        assert gpu_type in AWS_GPU_TYPES, f"Please select a GPU from {AWS_GPU_TYPES}"
    elif artifact_storage.startswith("gs"):
        assert gpu_type in GCP_GPU_TYPES, f"Please select a GPU from {GCP_GPU_TYPES}"


def _get_available_gpu_types_from_cloud() -> List[GPUType]:
    """
    Returns a list of available GPU types based on cloud env.
    """

    return list(set(AWS_GPU_TYPES).union(set(GCP_GPU_TYPES)))
    # artifact_storage = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", "")
    # if artifact_storage.startswith("s3"):
    #     return AWS_GPU_TYPES
    # elif artifact_storage.startswith("gs"):
    #     return GCP_GPU_TYPES
    # else:
    #     raise RuntimeError("Unexpected cloud environment. Please reach out to support")


def get_available_gpu_types(model_id: str) -> List[GPUType]:
    """
    If we know the models run only on larger GPUs, we exclude the smaller GPUs from the options.
    """
    if model_id in _MODEL_ID_TO_GPU_TYPES:
        gpus = _MODEL_ID_TO_GPU_TYPES[model_id]
    else:
        gpus = _get_available_gpu_types_from_cloud()

    return gpus
