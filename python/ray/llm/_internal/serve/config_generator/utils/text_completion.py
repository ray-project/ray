import os
from functools import cache
from typing import Any, Dict, Optional, Union

import yaml

from ray.llm._internal.serve.config_generator.utils.constants import (
    DEFAULT_DEPLOYMENT_CONFIGS_FILE,
    TEMPLATE_DIR,
)
from ray.llm._internal.serve.config_generator.utils.gpu import (
    DEFAULT_MODEL_ID_TO_GPU,
    GPUType,
)
from ray.llm._internal.serve.config_generator.utils.models import (
    TextCompletionModelConfig,
)
from ray.llm._internal.serve.configs.server_models import LLMConfig


def get_model_default_config(model_id: str) -> Dict[str, Any]:
    return {
        "model_loading_config": {
            "model_id": model_id,
        },
    }


@cache
def get_default_llm_config(model_id: str, gpu_type: GPUType) -> LLMConfig:
    file_path = os.path.join(TEMPLATE_DIR, DEFAULT_DEPLOYMENT_CONFIGS_FILE)
    with open(file_path, "r") as stream:
        configs = yaml.safe_load(stream)
    return LLMConfig.model_validate(
        configs["model_id_to_gpu_deployment_configs"][model_id][gpu_type.value]
    )


def populate_text_completion_model_config(
    input_model_config: TextCompletionModelConfig,
) -> Dict[str, Any]:
    """
    This method constructs the model config for text completion models.

    If the model configs are provided, we read the files from disks.
    If the reference model id is 'other', we use the default config for the model.
    """
    if input_model_config.id in DEFAULT_MODEL_ID_TO_GPU:
        base_config = get_model_default_config(input_model_config.id)
        llm_config = get_default_llm_config(
            model_id=input_model_config.id, gpu_type=input_model_config.gpu_type
        )
    else:
        assert input_model_config.reference_model_id
        base_config = get_model_default_config(input_model_config.id)
        llm_config = LLMConfig.model_validate(base_config)

    if input_model_config.hf_token:
        base_config.setdefault("runtime_env", {}).setdefault("env_vars", {})[
            "HF_TOKEN"
        ] = input_model_config.hf_token

    base_config["accelerator_type"] = input_model_config.gpu_type.value

    base_config["deployment_config"] = _populate_deployment_config(
        base_config.setdefault("deployment_config", {}),
        llm_config,
    )
    base_config["model_loading_config"] = _populate_model_loading_config(
        model_id=input_model_config.id,
        remote_storage_uri=input_model_config.remote_storage_uri,
        tp_degree=input_model_config.tensor_parallelism,
    )
    _populate_engine_kwargs(base_config, llm_config, input_model_config)

    return base_config


def _populate_deployment_config(
    deployment_config: Dict[str, Any],
    llm_config: LLMConfig,
) -> Dict[str, Any]:
    deployment_config.update(llm_config.deployment_config)
    return deployment_config


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
    llm_config: LLMConfig,
    input_model_config: TextCompletionModelConfig,
) -> None:

    engine_kwargs = configs.setdefault("engine_kwargs", {})
    engine_kwargs.update(llm_config.engine_kwargs)
    engine_kwargs.update(
        {
            "tensor_parallel_size": input_model_config.tensor_parallelism,
        }
    )

    return engine_kwargs
