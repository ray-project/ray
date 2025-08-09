from typing import List, Optional, Sequence

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMEngine,
    LLMServingArgs,
)
from ray.llm._internal.serve.deployments.data_parallel.dp_llm_server import DPLLMServer
from ray.llm._internal.serve.deployments.data_parallel.dp_rank_assigner import (
    DPRankAssigner,
)
from ray.llm._internal.serve.deployments.llm.llm_server import LLMDeployment
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle

logger = get_logger(__name__)


def build_llm_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    deployment_kwargs: Optional[dict] = None,
) -> Application:
    dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
    if dp_size == 1:
        name_prefix = name_prefix or "LLMDeployment:"
        deployment_kwargs = deployment_kwargs or {}

        deployment_options = llm_config.get_serve_options(
            name_prefix=name_prefix,
        )
        return LLMDeployment.options(**deployment_options).bind(
            llm_config=llm_config, **deployment_kwargs
        )

    # Build data parallel LLM deployment.
    dp_rank_assigner = DPRankAssigner.bind(dp_size=dp_size)
    name_prefix = name_prefix or "DPLLMDeployment:"
    name = name_prefix + llm_config._get_deployment_name()
    # TODO(rui): support data_parallel_backend=ray and unify
    # deployment_options handling with LLMDeployment.
    deployment_options = {
        "name": name,
        "num_replicas": dp_size,
    }
    return DPLLMServer.as_deployment(deployment_options).bind(
        llm_config=llm_config, dp_rank_assigner=dp_rank_assigner
    )


def _get_llm_deployments(
    llm_base_models: Sequence[LLMConfig],
    deployment_kwargs: Optional[dict] = None,
) -> List[DeploymentHandle]:
    llm_deployments = []
    for llm_config in llm_base_models:
        if llm_config.llm_engine == LLMEngine.vLLM:
            llm_deployments.append(
                build_llm_deployment(llm_config, deployment_kwargs=deployment_kwargs)
            )
        else:
            # Note (genesu): This should never happen because we validate the engine
            # in the config.
            raise ValueError(f"Unsupported engine: {llm_config.llm_engine}")

    return llm_deployments


def build_openai_app(llm_serving_args: LLMServingArgs) -> Application:
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

    return LLMRouter.as_deployment(llm_configs=llm_configs).bind(
        llm_deployments=llm_deployments
    )
