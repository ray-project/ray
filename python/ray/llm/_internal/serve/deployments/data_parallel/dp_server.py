import logging
import time
from typing import Optional

from ray import serve
from ray.experimental.collective.util import get_address_and_port
from ray.llm._internal.serve.configs.constants import DEFAULT_MAX_ONGOING_REQUESTS
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.data_parallel.dp_rank_assigner import (
    DPRankAssigner,
)
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
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
        self.dp_rank = await self.dp_rank_assigner.register.remote(replica_ctx)
        logger.info(f"DP rank {self.dp_rank} has registered")

        if self.dp_rank == 0:
            self.dp_address, self.dp_rpc_port = get_address_and_port()
            await self.dp_rank_assigner.set_dp_master_info.remote(
                self.dp_address, self.dp_rpc_port
            )
            logger.info(
                f"DP rank {self.dp_rank} has set DP master info: {self.dp_address}, {self.dp_rpc_port}"
            )
        else:
            timestamp = time.time()
            (
                self.dp_address,
                self.dp_rpc_port,
            ) = await self.dp_rank_assigner.get_dp_master_info.remote()
            logger.info(
                f"DP rank {self.dp_rank} got DP master info: {self.dp_address}, {self.dp_rpc_port}, "
                f"after waiting for {time.time() - timestamp:.3f} seconds"
            )

        # override the engine_kwargs to assign the DP rank.
        llm_config.engine_kwargs["data_parallel_rank"] = self.dp_rank
        llm_config.engine_kwargs["data_parallel_address"] = self.dp_address
        llm_config.engine_kwargs["data_parallel_rpc_port"] = self.dp_rpc_port

        await super().__init__(llm_config)

    @classmethod
    def as_deployment(cls, deployment_options: dict) -> serve.Deployment:
        return serve.deployment(cls).options(**deployment_options)


def build_dp_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
) -> Application:
    """Build a data parallel LLM deployment."""
    dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
    if dp_size == 1:
        raise ValueError(
            "data_parallel_size should be greater than 1 for DP deployment."
        )
    dp_rank_assigner = DPRankAssigner.bind(dp_size=dp_size)
    name_prefix = name_prefix or "DPLLMDeployment:"
    name = name_prefix + llm_config._get_deployment_name()
    if "num_replicas" in llm_config.deployment_config:
        raise ValueError(
            "num_replicas should not be specified for DP deployment, "
            "use engine_kwargs.data_parallel_size instead."
        )
    if "autoscaling_config" in llm_config.deployment_config:
        raise ValueError(
            "autoscaling_config is not supported for DP deployment, "
            "use engine_kwargs.data_parallel_size to set a fixed number "
            "of replicas instead."
        )
    # TODO(rui): support data_parallel_backend=ray and unify
    # deployment_options handling with LLMDeployment.
    deployment_options = {
        "name": name,
        "num_replicas": dp_size,
        "max_ongoing_requests": DEFAULT_MAX_ONGOING_REQUESTS,
    }
    return DPServer.as_deployment(deployment_options).bind(
        llm_config=llm_config, dp_rank_assigner=dp_rank_assigner
    )
