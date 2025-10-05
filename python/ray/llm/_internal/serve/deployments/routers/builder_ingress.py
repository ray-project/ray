import os
import pprint
from typing import Any, Dict, List, Optional, Sequence, Type, Union, overload

import pydantic
from pydantic import Field

from ray import serve
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.serve.configs.server_models import LLMConfig, LLMEngine
from ray.llm._internal.serve.deployments.llm.builder_llm_server import (
    build_llm_deployment,
)
from ray.llm._internal.serve.deployments.routers.router import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)


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

    ingress_options = OpenAiIngress.get_deployment_options(llm_configs)

    if override_serve_options:
        ingress_options.update(override_serve_options)

    ingress_cls = make_fastapi_ingress(ingress_cls)

    logger.info("============== Ingress Options ==============")
    logger.info(pprint.pformat(ingress_options))

    return serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=llm_deployments, **bind_kwargs
    )
