import logging
import time
from typing import Optional

from ray import serve
from ray.experimental.collective.util import get_address_and_port
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.llm._internal.serve.core.server.builder import (
    build_llm_deployment,
)
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_rank_assigner import (
    DPRankAssigner,
)
from ray.runtime_context import get_runtime_context
from ray.serve.deployment import Application
from ray.serve.handle import DeploymentHandle

logger = logging.getLogger(__name__)


class DPServer(LLMServer):
    """
    Data Parallel LLM Server.

    This class is used to serve data parallel attention (DP Attention)
    deployment paradigm, where the attention layers are replicated and
    the MoE layers are sharded. DP Attention is typically used for models
    like DeepSeek-V3.
    """

    async def __init__(self, llm_config: LLMConfig, dp_rank_assigner: DeploymentHandle):
        self.dp_rank_assigner = dp_rank_assigner

        replica_ctx = serve.get_replica_context()
        node_id = get_runtime_context().get_node_id()
        self.dp_rank = await self.dp_rank_assigner.register.remote(replica_ctx, node_id)

        logger.info(f"DP rank {self.dp_rank} registered with rank assigner")

        if self.dp_rank == 0:
            self.dp_address, self.dp_rpc_port = get_address_and_port()
            await self.dp_rank_assigner.set_dp_master_info.remote(
                self.dp_address, self.dp_rpc_port
            )
            logger.info(
                f"DP rank {self.dp_rank} has set DP master info: "
                f"data_parallel_address={self.dp_address}, "
                f"data_parallel_rpc_port={self.dp_rpc_port}"
            )
        else:
            timestamp = time.time()
            (
                self.dp_address,
                self.dp_rpc_port,
            ) = await self.dp_rank_assigner.get_dp_master_info.remote()
            logger.info(
                f"DP rank {self.dp_rank} got DP master info: "
                f"data_parallel_address={self.dp_address}, "
                f"data_parallel_rpc_port={self.dp_rpc_port}, "
                f"waited {time.time() - timestamp:.3f} seconds"
            )

        # Update the engine_kwargs to assign the DP information
        llm_config.update_engine_kwargs(
            data_parallel_rank=self.dp_rank,
            data_parallel_address=self.dp_address,
            data_parallel_rpc_port=self.dp_rpc_port,
        )

        await super().__init__(llm_config)

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        deployment_options = super().get_deployment_options(llm_config)

        dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
        if not (isinstance(dp_size, int) and dp_size > 0):
            raise ValueError(
                f"Invalid data_parallel_size: {dp_size}, expecting " "positive integer."
            )
        if dp_size != 1:
            if "num_replicas" in deployment_options:
                raise ValueError(
                    "num_replicas should not be specified for DP deployment, "
                    f"use engine_kwargs.data_parallel_size={dp_size} instead."
                )
            if "autoscaling_config" in deployment_options:
                raise ValueError(
                    "autoscaling_config is not supported for DP deployment, "
                    "remove autoscaling_config instead. The `num_replicas` "
                    "will be set to `data_parallel_size`."
                )
            deployment_options["num_replicas"] = dp_size
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
    """Build a data parallel LLM deployment."""
    dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
    if dp_size == 1:
        raise ValueError(
            "data_parallel_size should be greater than 1 for DP deployment."
        )

    # TODO(rui): figure out a better way to pass in dp_size_per_node.
    # NOTE: we cannot use engine_kwargs.data_parallel_size_local to specify
    # the number of ranks per node because that has special semantics in vLLM.
    dp_size_per_node = llm_config.experimental_configs.get("dp_size_per_node", None)

    dp_rank_assigner = DPRankAssigner.bind(
        dp_size=dp_size, dp_size_per_node=dp_size_per_node
    )

    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        bind_kwargs=dict(dp_rank_assigner=dp_rank_assigner),
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
