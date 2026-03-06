import logging
import time

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.runtime_context import get_runtime_context
from ray.serve.handle import DeploymentHandle
from ray.util.collective.collective import get_address_and_port

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

        node_id = get_runtime_context().get_node_id()
        self.dp_rank = await self.dp_rank_assigner.register.remote(node_id)

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
