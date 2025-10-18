import logging
from typing import Optional

from ray import serve
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.llm.builder_llm_server import (
    build_llm_deployment,
)
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.llm._internal.serve.deployments.routers.router import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.serve.deployment import Application

logger = logging.getLogger(__name__)


class DPServer(LLMServer):
    """
    Data Parallel LLM Server.

    This class is used to serve data parallel attention (DP Attention)
    deployment paradigm, where the attention layers are replicated and
    the MoE layers are sharded. DP Attention is typically used for models
    like DeepSeek-V3.

    Each Serve replica represents one complete DP group. When data_parallel_size > 1,
    vLLM will spawn that many worker actors internally using Ray's distributed executor.
    Multiple Serve replicas create independent DP groups that Ray Serve load balances between.
    """

    async def __init__(self, llm_config: LLMConfig):
        replica_ctx = serve.get_replica_context()
        serve_replica_rank = replica_ctx.rank
        dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)

        logger.info(
            f"Serve replica {serve_replica_rank}: Initializing DP group with "
            f"{dp_size} workers (vLLM will spawn them internally via Ray backend)"
        )

        # Enforce that data_parallel_backend is set to "ray"
        dp_backend = llm_config.engine_kwargs.get("data_parallel_backend", None)
        if dp_backend is None:
            llm_config.update_engine_kwargs(data_parallel_backend="ray")
        elif dp_backend != "ray":
            raise ValueError(
                f"data_parallel_backend must be set to 'ray' for DPServer, but got: {dp_backend}"
            )

        # Each Serve replica is a self-contained DP group
        # vLLM's Ray distributed executor will spawn dp_size workers internally
        # No coordination needed between Serve replicas
        await super().__init__(llm_config)

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        deployment_options = super().get_deployment_options(llm_config)

        dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
        if not (isinstance(dp_size, int) and dp_size > 0):
            raise ValueError(
                f"Invalid data_parallel_size: {dp_size}, expecting positive integer."
            )

        if dp_size != 1:
            if "autoscaling_config" in deployment_options:
                raise ValueError(
                    "autoscaling_config is not supported for DP deployment."
                )

            # Each Serve replica is one independent DP group with dp_size workers
            # num_replicas controls how many independent DP groups to create
            num_replicas = deployment_options.get("num_replicas", 1)

            logger.info(
                f"DP deployment: {num_replicas} Serve replica(s), "
                f"each with data_parallel_size={dp_size} "
                f"(vLLM will spawn {dp_size} workers per replica)"
            )

            if deployment_options["placement_group_strategy"] != "STRICT_PACK":
                logger.warning(
                    f"DP deployment with placement_strategy={deployment_options['placement_group_strategy']} "
                    "is not supported. Using STRICT_PACK instead."
                )
                deployment_options["placement_group_strategy"] = "STRICT_PACK"

        return deployment_options


def build_dp_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    override_serve_options: Optional[dict] = None,
) -> Application:
    """Build a data parallel LLM deployment.

    Each Serve replica represents one complete DP group. vLLM will spawn
    data_parallel_size worker actors internally using Ray's distributed executor.

    Args:
        llm_config: LLM configuration with engine_kwargs.data_parallel_size set.
        name_prefix: Optional prefix for the deployment name.
        override_serve_options: Optional overrides for Serve deployment options.

    Returns:
        A Serve Application for the DP deployment.

    Example:
        # Single Serve replica with DP group of 8 workers
        llm_config = LLMConfig(engine_kwargs={"data_parallel_size": 8})
        app = build_dp_deployment(llm_config)  # Creates 1 Serve replica (8 workers internally)

        # Two Serve replicas, each with DP group of 8 workers (2x DPEP8)
        llm_config = LLMConfig(
            engine_kwargs={"data_parallel_size": 8},
            deployment_config={"num_replicas": 2}  # 2 independent DP groups
        )
        app = build_dp_deployment(llm_config)
    """
    dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
    if dp_size == 1:
        raise ValueError(
            "data_parallel_size should be greater than 1 for DP deployment."
        )

    # Each Serve replica is self-contained - no coordination needed
    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        override_serve_options=override_serve_options,
        deployment_cls=DPServer,
    )


def build_openai_dp_app(llm_config: LLMConfig) -> Application:

    dp_deployment = build_dp_deployment(llm_config)
    ingress_options = OpenAiIngress.get_deployment_options([llm_config])

    ingress_cls = make_fastapi_ingress(OpenAiIngress)
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[dp_deployment]
    )

    return ingress_app
