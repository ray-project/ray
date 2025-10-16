import copy
import os
from typing import Any, Dict

import yaml

from ray.llm._internal.serve.config_generator.utils.constants import TEMPLATE_DIR
from ray.llm._internal.serve.config_generator.utils.models import (
    TextCompletionModelConfig,
)
from ray.llm._internal.serve.config_generator.utils.overrides import (
    ask_and_merge_model_overrides,
)
from ray.llm._internal.serve.config_generator.utils.text_completion import (
    populate_text_completion_model_config,
)
from ray.serve._private.constants import DEFAULT_TARGET_ONGOING_REQUESTS
from ray.serve.config import AutoscalingConfig


def get_model_base_config(
    input_model_config: TextCompletionModelConfig,
) -> Dict[str, Any]:
    """
    This method returns the base model config based on user inputs.
    """
    return populate_text_completion_model_config(input_model_config)


def get_model_config(input_model_config: TextCompletionModelConfig) -> Dict[str, Any]:
    """
    This method decorates the base model config based on serving type, such as Lora or function calling.
    """

    model_config = get_model_base_config(input_model_config)

    model_config = maybe_get_lora_enabled_config(
        input_model_config,
        model_config,
    )

    return model_config


def get_serve_config(
    model_config_path: str,
):
    """
    This util method constructs the final serve config.

    It doesn't inline the model config and, instead, just puts the file path based on the type of models.
    """
    serve_config = get_serve_base_config()
    serve_config["applications"][0]["args"]["llm_configs"] = [model_config_path]
    return serve_config


def override_model_configs(base_model_config: Dict[str, Any]) -> Dict[str, Any]:
    existing_autoscaling_config = base_model_config["deployment_config"].setdefault(
        "autoscaling_config", {}
    )
    # Getting the default target ongoing requests from the base model config.
    # If not present, default to 2 (same as Ray Serve's default).
    default_ongoing_requests = (
        base_model_config.get("deployment_config", {})
        .get("autoscaling_config", {})
        .get("target_ongoing_requests", DEFAULT_TARGET_ONGOING_REQUESTS)
    )
    final_autoscaling_config = ask_and_merge_model_overrides(
        existing_configs=existing_autoscaling_config,
        model_cls=AutoscalingConfig,
        fields=["min_replicas", "max_replicas", "target_ongoing_requests"],
        defaults={"target_ongoing_requests": default_ongoing_requests},
    )
    final_autoscaling_config["initial_replicas"] = final_autoscaling_config[
        "min_replicas"
    ]
    base_model_config["deployment_config"][
        "autoscaling_config"
    ] = final_autoscaling_config

    # Make sure max_ongoing request is always double of target_ongoing_requests
    base_model_config["deployment_config"]["max_ongoing_requests"] = (
        2 * final_autoscaling_config["target_ongoing_requests"]
    )

    return base_model_config


def maybe_get_lora_enabled_config(
    input_model_config: TextCompletionModelConfig,
    base_config: Dict[str, Any],
):
    """
    If the input model contains lora config, this method decorates the base
    config with lora-specific configurations.
    """
    if input_model_config.lora_config:
        max_num_adapters_per_replica = (
            input_model_config.lora_config.max_num_lora_per_replica
        )

        res = copy.deepcopy(base_config)
        res["lora_config"] = {
            "dynamic_lora_loading_path": input_model_config.lora_config.uri,
            "max_num_adapters_per_replica": max_num_adapters_per_replica,
        }

        res.setdefault("engine_kwargs", {})["enable_lora"] = True
        res.setdefault("engine_kwargs", {})["max_lora_rank"] = 32
        res.setdefault("engine_kwargs", {})["max_loras"] = max_num_adapters_per_replica
        return res
    else:
        return base_config


_BASE_SERVE_CONFIG = "base_serve_config.yaml"


def get_serve_base_config() -> Dict[str, Any]:
    # Construct the full path to the YAML file
    file_path = os.path.join(TEMPLATE_DIR, _BASE_SERVE_CONFIG)
    with open(file_path, "r") as stream:
        return yaml.safe_load(stream)
