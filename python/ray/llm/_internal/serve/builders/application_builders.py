from typing import List, Optional, Sequence

import ray
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle

from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.deployments.llm.vllm.vllm_deployment import VLLMDeployment
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMServingArgs,
    LLMEngine,
)
from ray.llm._internal.serve.deployments.routers.router import LLMModelRouterDeployment
from ray.llm._internal.serve.configs.constants import (
    ENABLE_WORKER_PROCESS_SETUP_HOOK,
)

logger = get_logger(__name__)


def _set_deployment_placement_options(llm_config: LLMConfig) -> dict:
    deployment_config = llm_config.deployment_config.model_copy(deep=True).model_dump()
    engine_config = llm_config.get_engine_config()

    ray_actor_options = deployment_config["ray_actor_options"] or {}
    deployment_config["ray_actor_options"] = ray_actor_options

    replica_actor_resources = {
        "CPU": ray_actor_options.get("num_cpus", 1),
        "GPU": ray_actor_options.get("num_gpus", 0),
        **ray_actor_options.get("resources", {}),
    }
    if "memory" in ray_actor_options:
        replica_actor_resources["memory"] = ray_actor_options["memory"]

    if (
        "placement_group_bundles" in deployment_config
        or "placement_group_strategy" in deployment_config
    ):
        raise ValueError(
            "placement_group_bundles and placement_group_strategy must not be specified in deployment_config. "
            "Use scaling_config to configure replica placement group."
        )

    # TODO (Kourosh): There is some test code leakage happening here that should be removed.
    try:
        # resources.mock_resource is a special key we used in tests to skip placement
        # group on the gpu nodes.
        if "mock_resource" in ray_actor_options.get("resources", {}):
            bundles = []
        else:
            bundles = engine_config.placement_bundles
    except ValueError:
        # May happen if all bundles are empty.
        bundles = []

    bundles = [replica_actor_resources] + bundles
    deployment_config.update(
        {
            "placement_group_bundles": bundles,
            "placement_group_strategy": engine_config.placement_strategy,
        }
    )

    return deployment_config


def _get_deployment_name(llm_config: LLMConfig, name_prefix: str):
    unsanitized_deployment_name = name_prefix + llm_config.model_id
    return unsanitized_deployment_name.replace("/", "--").replace(".", "_")


def get_serve_deployment_args(
    llm_config: LLMConfig,
    *,
    name_prefix: str,
    default_runtime_env: Optional[dict] = None,
):
    deployment_config = _set_deployment_placement_options(llm_config)

    if default_runtime_env:
        ray_actor_options = deployment_config.get("ray_actor_options", {})
        ray_actor_options["runtime_env"] = {
            **default_runtime_env,
            # Existing runtime_env should take precedence over the default.
            **ray_actor_options.get("runtime_env", {}),
            **(llm_config.runtime_env if llm_config.runtime_env else {}),
        }
        deployment_config["ray_actor_options"] = ray_actor_options

    # Set the name of the deployment config to map to the model ID.
    deployment_config["name"] = _get_deployment_name(llm_config, name_prefix)
    return deployment_config


def build_vllm_deployment(
    llm_config: LLMConfig,
    deployment_kwargs: Optional[dict] = None,
) -> Application:
    if deployment_kwargs is None:
        deployment_kwargs = {}

    default_runtime_env = ray.get_runtime_context().runtime_env
    if ENABLE_WORKER_PROCESS_SETUP_HOOK:
        default_runtime_env[
            "worker_process_setup_hook"
        ] = "ray.llm._internal.serve._worker_process_setup_hook"

    deployment_options = get_serve_deployment_args(
        llm_config,
        name_prefix="VLLMDeployment:",
        default_runtime_env=default_runtime_env,
    )

    return VLLMDeployment.options(**deployment_options).bind(
        llm_config=llm_config, **deployment_kwargs
    )


def _get_llm_deployments(
    llm_base_models: Optional[Sequence[LLMConfig]] = None,
    deployment_kwargs: Optional[dict] = None,
) -> List[DeploymentHandle]:
    llm_deployments = []
    for llm_config in llm_base_models:
        if llm_config.llm_engine == LLMEngine.VLLM:
            llm_deployments.append(build_vllm_deployment(llm_config, deployment_kwargs))
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

    return LLMModelRouterDeployment.bind(llm_deployments=llm_deployments)
