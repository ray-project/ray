import os
from functools import cache
from typing import Any, Dict, Optional, Union

import yaml

from ray.llm._internal.serve.config_generator.utils.constants import (
    DEFAULT_DEPLOYMENT_CONFIGS_FILE,
    MODEL_CONFIGS_DIR,
    REFERENCE_BASE_MODEL_ID,
    TEMPLATE_DIR,
)
from ray.llm._internal.serve.config_generator.utils.gpu import GPUType
from ray.llm._internal.serve.config_generator.utils.models import (
    MODEL_ID_TO_DEFAULT_CONFIG_FILE,
    DeploymentConfig,
    TextCompletionModelConfig,
)


def get_model_default_config(model_id: str) -> Dict[str, Any]:
    
    return {
        "model_loading_config": {
            "model_id": model_id,
        },
    }
    # if model_id not in MODEL_ID_TO_DEFAULT_CONFIG_FILE:
    #     raise RuntimeError(f"Defaults for {model_id} are not provided.")
    # # Construct the full path to the YAML file
    # file_name = MODEL_ID_TO_DEFAULT_CONFIG_FILE[model_id]
    # file_path = os.path.join(MODEL_CONFIGS_DIR, file_name)

    # with open(file_path, "r") as stream:
    #     return yaml.safe_load(stream)


@cache
def get_default_deployment_configs(
    model_id: str, gpu_type: GPUType
) -> DeploymentConfig:
    file_path = os.path.join(TEMPLATE_DIR, DEFAULT_DEPLOYMENT_CONFIGS_FILE)
    with open(file_path, "r") as stream:
        configs = yaml.safe_load(stream)
    return DeploymentConfig.model_validate(
        configs["model_id_to_gpu_deployment_configs"][model_id][gpu_type.value]
    )


def populate_text_completion_model_config(
    input_model_config: TextCompletionModelConfig,
) -> Dict[str, Any]:
    """
    This method constructs the model config for text completion models.

    If the model configs are provided, we read the files from disks.
    If the reference model id is 'other', we default to using the config from REFERENCE_BASE_MODEL_ID.
    """
    if input_model_config.id in MODEL_ID_TO_DEFAULT_CONFIG_FILE:
        base_config = get_model_default_config(input_model_config.id)
        deployment_configs = get_default_deployment_configs(
            model_id=input_model_config.id, gpu_type=input_model_config.gpu_type
        )
    else:
        assert input_model_config.reference_model_id
        input_ref_model_id = input_model_config.reference_model_id
        ref_model_id = (
            REFERENCE_BASE_MODEL_ID
            if input_ref_model_id == "other"
            else input_ref_model_id
        )
        base_config = get_model_default_config(ref_model_id)
        deployment_configs = get_default_deployment_configs(
            model_id=ref_model_id, gpu_type=input_model_config.gpu_type
        )

    if input_model_config.hf_token:
        base_config.setdefault("runtime_env", {}).setdefault("env_vars", {})[
            "HUGGING_FACE_HUB_TOKEN"
        ] = input_model_config.hf_token

    base_config["accelerator_type"] = input_model_config.gpu_type.value

    # base_config.setdefault("deployment_config", {}).update(deployment_configs.deployment_config)
    base_config["deployment_config"] = _populate_deployment_configs(
        base_config.setdefault("deployment_config", {}),
        deployment_configs,
    )
    base_config["model_loading_config"] = _populate_model_loading_config(
        model_id=input_model_config.id,
        remote_storage_uri=input_model_config.remote_storage_uri,
        tp_degree=input_model_config.tensor_parallelism,
    )
    _populate_engine_kwargs(base_config, deployment_configs, input_model_config)

    return base_config


def _populate_deployment_configs(
    configs: Dict[str, Any],
    deployment_configs: DeploymentConfig,
) -> Dict[str, Any]:
    
    # configs.setdefault("autoscaling_config", {})[
    #     "target_ongoing_requests"
    # ] = deployment_configs.target_ongoing_requests
    # configs["max_ongoing_requests"] = deployment_configs.max_ongoing_requests
    configs.update(deployment_configs.deployment_config)
    return configs


def _populate_model_loading_config(
    *, model_id: str, remote_storage_uri: Optional[str], tp_degree: int
) -> Dict[str, Union[str, Dict[str, str]]]:
    model_loading_config = {"model_id": model_id}
    if remote_storage_uri:
        model_loading_config["model_source"] = {"bucket_uri": remote_storage_uri}
    else:
        model_loading_config["model_source"] = model_id

    return model_loading_config


def _populate_engine_kwargs(
    configs: Dict[str, Any],
    deployment_configs: DeploymentConfig,
    input_model_config: TextCompletionModelConfig,
) -> None:
    
    engine_kwargs = configs.setdefault("engine_kwargs", {})
    engine_kwargs.update(deployment_configs.engine_kwargs)
    engine_kwargs.update({
        "tensor_parallel_size": input_model_config.tensor_parallelism,
    })
    
    return engine_kwargs
    
    
    # lora_config = input_model_config.lora_config

    # configs.setdefault("engine_kwargs", {})[
    #     "max_num_seqs"
    # ] = deployment_configs.deployment_config.max_ongoing_requests
    # configs.setdefault("engine_kwargs", {})[
    #     "max_num_batched_tokens"
    # ] = deployment_configs.max_num_batched_tokens

    # configs.setdefault("engine_kwargs", {})[
    #     "tensor_parallel_size"
    # ] = input_model_config.tensor_parallelism

    # # Chunked prefill is not compatible with LoRA. We can only enable it
    # # if LoRA is disabled.
    # if lora_config is None and deployment_configs.enable_chunked_prefill is not None:
    #     configs.setdefault("engine_kwargs", {})[
    #         "enable_chunked_prefill"
    #     ] = deployment_configs.enable_chunked_prefill

    # # max_model_len should not be bigger than max_num_batched_tokens, unless
    # # chunked prefill is enabled.
    # existing_max_model_len = configs.get("engine_kwargs", {}).get("max_model_len", None)
    # chunked_prefill_enabled = configs.get("engine_kwargs", {}).get(
    #     "enable_chunked_prefill", False
    # )
    # if (
    #     existing_max_model_len is not None
    #     and deployment_configs.max_num_batched_tokens
    #     and existing_max_model_len > deployment_configs.max_num_batched_tokens
    #     and not chunked_prefill_enabled
    # ):
    #     configs.setdefault("engine_kwargs", {})[
    #         "max_model_len"
    #     ] = deployment_configs.max_num_batched_tokens
