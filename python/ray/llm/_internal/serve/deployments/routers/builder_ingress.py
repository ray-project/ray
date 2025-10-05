import os
from typing import Any, Dict, List, Optional, Sequence, Type, Union, overload

import pydantic
from pydantic import Field

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.dict_utils import deep_merge_dicts
from ray.llm._internal.serve.configs.constants import (
    DEFAULT_LLM_ROUTER_INITIAL_REPLICAS,
    DEFAULT_LLM_ROUTER_MAX_REPLICAS,
    DEFAULT_LLM_ROUTER_MIN_REPLICAS,
    DEFAULT_MAX_ONGOING_REQUESTS,
    DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    DEFAULT_ROUTER_TO_MODEL_REPLICA_RATIO,
)
from ray.llm._internal.serve.configs.server_models import LLMConfig, LLMEngine
from ray.llm._internal.serve.deployments.llm.builder_llm_server import (
    build_llm_deployment,
)
from ray.llm._internal.serve.deployments.routers.router import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.config import AutoscalingConfig
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)

import pprint

DEFAULT_INGRESS_OPTIONS = {
    "autoscaling_config": {
        "target_ongoing_requests": DEFAULT_MAX_TARGET_ONGOING_REQUESTS,
    },
    "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
}


def _is_yaml_file(filename: str) -> bool:
    yaml_extensions = [".yml", ".yaml", ".json"]
    for s in yaml_extensions:
        if filename.endswith(s):
            return True
    return False


def _parse_path_args(path: str) -> List[LLMConfig]:
    assert os.path.exists(
        path
    ), f"Could not load model from {path}, as it does not exist."
    if os.path.isfile(path):
        with open(path, "r") as f:
            llm_config = LLMConfig.parse_yaml(f)
            return [llm_config]
    elif os.path.isdir(path):
        apps = []
        for root, _dirs, files in os.walk(path):
            for p in files:
                if _is_yaml_file(p):
                    with open(os.path.join(root, p), "r") as f:
                        llm_config = LLMConfig.parse_yaml(f)
                        apps.append(llm_config)
        return apps
    else:
        raise ValueError(
            f"Could not load model from {path}, as it is not a file or directory."
        )


def parse_args(
    args: Union[str, LLMConfig, Any, Sequence[Union[LLMConfig, str, Any]]],
) -> List[LLMConfig]:
    """Parse the input args and return a standardized list of LLMConfig objects

    Supported args format:
    1. The path to a yaml file defining your LLMConfig
    2. The path to a folder containing yaml files, which define your LLMConfigs
    3. A list of yaml files defining multiple LLMConfigs
    4. A dict or LLMConfig object
    5. A list of dicts or LLMConfig objects
    """

    raw_models = [args]
    if isinstance(args, list):
        raw_models = args

    # For each
    models: List[LLMConfig] = []
    for raw_model in raw_models:
        if isinstance(raw_model, str):
            if os.path.exists(raw_model):
                parsed_models = _parse_path_args(raw_model)
            else:
                try:
                    llm_config = LLMConfig.parse_yaml(raw_model)
                    parsed_models = [llm_config]
                except pydantic.ValidationError as e:
                    raise ValueError(
                        f"Could not parse string as yaml. If you are "
                        "specifying a path, make sure it exists and can be "
                        f"reached. raw_model: {raw_model}"
                    ) from e
        else:
            try:
                llm_config = LLMConfig.model_validate(raw_model)
                parsed_models = [llm_config]
            except pydantic.ValidationError:
                parsed_models = [LLMConfig.model_validate(raw_model)]
        models += parsed_models

    return models


class LLMServingArgs(BaseModelExtended):
    llm_configs: List[Union[str, LLMConfig]] = Field(
        description="A list of LLMConfigs, or paths to LLMConfigs, to run.",
    )

    def parse_args(self) -> "LLMServingArgs":
        """Converts this LLMServingArgs object into an DeployArgs object."""

        llm_configs = []
        for config in self.llm_configs:
            parsed_config = parse_args(config)[0]
            if not isinstance(parsed_config, LLMConfig):
                raise ValueError(
                    "When using the new Serve config format, all model "
                    "configs must also use the new model config format. Got "
                    "a model config that doesn't match new format. Type: "
                    f"{type(parsed_config)}. Contents: {parsed_config}."
                )
            llm_configs.append(parsed_config)

        return LLMServingArgs(llm_configs=llm_configs)


def _get_llm_deployments(
    llm_base_models: Sequence[LLMConfig],
    bind_kwargs: Optional[dict] = None,
) -> List[DeploymentHandle]:
    llm_deployments = []
    for llm_config in llm_base_models:
        if llm_config.llm_engine == LLMEngine.vLLM:
            llm_deployments.append(
                build_llm_deployment(llm_config, bind_kwargs=bind_kwargs)
            )
        else:
            # Note (genesu): This should never happen because we validate the engine
            # in the config.
            raise ValueError(f"Unsupported engine: {llm_config.llm_engine}")

    return llm_deployments


def infer_num_ingress_replicas(llm_configs: Optional[List[LLMConfig]] = None) -> dict:
    """Infer the number of ingress replicas based on the LLM configs.

    Based on our internal benchmark, we are currently bottleneck
    by the router replicas during high concurrency situation. We are setting the
    router replicas to be ~2x the total model replicas and making it scale faster.

    Args:
        llm_configs: The LLM configs to infer the number of ingress replicas from.

    Returns:
        A dictionary containing the autoscaling config for the ingress deployment.
    """
    llm_configs = llm_configs or []
    min_replicas = DEFAULT_LLM_ROUTER_MIN_REPLICAS
    initial_replicas = DEFAULT_LLM_ROUTER_INITIAL_REPLICAS
    max_replicas = DEFAULT_LLM_ROUTER_MAX_REPLICAS
    num_ingress_replicas = 0

    if llm_configs:
        model_min_replicas = 0
        model_initial_replicas = 0
        model_max_replicas = 0
        for llm_config in llm_configs:
            num_ingress_replicas = max(
                num_ingress_replicas,
                llm_config.experimental_configs.get("num_ingress_replicas", 0),
            )

            if "autoscaling_config" in llm_config.deployment_config:
                autoscaling_config = llm_config.deployment_config["autoscaling_config"]
                if isinstance(autoscaling_config, dict):
                    autoscaling_config = AutoscalingConfig(
                        **llm_config.deployment_config["autoscaling_config"]
                    )
            else:
                # When autoscaling config is not provided, we use the default.
                autoscaling_config = AutoscalingConfig()
            model_min_replicas += autoscaling_config.min_replicas
            model_initial_replicas += (
                autoscaling_config.initial_replicas or autoscaling_config.min_replicas
            )
            model_max_replicas += autoscaling_config.max_replicas
        min_replicas = num_ingress_replicas or int(
            model_min_replicas * DEFAULT_ROUTER_TO_MODEL_REPLICA_RATIO
        )
        initial_replicas = num_ingress_replicas or int(
            model_initial_replicas * DEFAULT_ROUTER_TO_MODEL_REPLICA_RATIO
        )
        max_replicas = num_ingress_replicas or int(
            model_max_replicas * DEFAULT_ROUTER_TO_MODEL_REPLICA_RATIO
        )

    return {
        "autoscaling_config": {
            "min_replicas": min_replicas,
            "initial_replicas": initial_replicas,
            "max_replicas": max_replicas,
        }
    }


def infer_default_ingress_options(
    llm_configs: Optional[List[LLMConfig]] = None,
) -> dict:
    return deep_merge_dicts(
        DEFAULT_INGRESS_OPTIONS, infer_num_ingress_replicas(llm_configs)
    )


@overload
def build_openai_app(
    llm_serving_args: Dict[str, Any],
    *,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    ingress_cls: Optional[Type[OpenAiIngress]] = OpenAiIngress,
) -> Application:
    ...


def build_openai_app(
    llm_serving_args: LLMServingArgs,
    *,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    ingress_cls: Optional[Type[OpenAiIngress]] = OpenAiIngress,
) -> Application:

    bind_kwargs = bind_kwargs or {}
    rayllm_args = LLMServingArgs.model_validate(llm_serving_args).parse_args()

    llm_configs = rayllm_args.llm_configs
    model_ids = {m.model_id for m in llm_configs}
    if len(model_ids) != len(llm_configs):
        raise ValueError("Duplicate models found. Make sure model ids are unique.")

    if len(llm_configs) == 0:
        logger.error(
            "List of models is empty. Maybe some parameters cannot be parsed into the LLMConfig config."
        )

    llm_deployments = _get_llm_deployments(llm_configs)

    ingress_options = infer_default_ingress_options(llm_configs)

    if override_serve_options:
        ingress_options = ingress_options.update(override_serve_options)

    ingress_cls = make_fastapi_ingress(ingress_cls)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=llm_deployments, **bind_kwargs
    )
