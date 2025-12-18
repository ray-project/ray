import logging
import time

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.llm_server import LLMServer
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

    async def __init__(
        self,
        llm_config: LLMConfig,
        dp_group_manager: DeploymentHandle,
    ):
        self.dp_group_manager = dp_group_manager

        # Get replica context for deterministic rank assignment
        replica_context = serve.get_replica_context()
        self.replica_rank = replica_context.rank
        self.replica_id = replica_context.replica_id.unique_id

        # Register with DPGroupManager and get dp_rank and group_index
        self.dp_rank, self.group_index = await self.dp_group_manager.register.remote(
            replica_rank=self.replica_rank,
            replica_id=self.replica_id,
        )

        logger.info(
            f"DP rank {self.dp_rank} registered to group {self.group_index} "
            f"(replica_rank={self.replica_rank})"
        )

        if self.dp_rank == 0:
            self.dp_address, self.dp_rpc_port = get_address_and_port()
            await self.dp_group_manager.set_dp_master_info.remote(
                group_index=self.group_index,
                dp_address=self.dp_address,
                dp_rpc_port=self.dp_rpc_port,
            )
            logger.info(
                f"DP rank {self.dp_rank} (group {self.group_index}) has set DP master info: "
                f"data_parallel_address={self.dp_address}, "
                f"data_parallel_rpc_port={self.dp_rpc_port}"
            )
        else:
            timestamp = time.time()
            (
                self.dp_address,
                self.dp_rpc_port,
            ) = await self.dp_group_manager.get_dp_master_info.remote(
                group_index=self.group_index
            )
            logger.info(
                f"DP rank {self.dp_rank} (group {self.group_index}) got DP master info: "
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

        dp_group_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
        if not (isinstance(dp_group_size, int) and dp_group_size > 0):
            raise ValueError(
                f"Invalid data_parallel_size: {dp_group_size}, expecting "
                "positive integer."
            )

        if dp_group_size != 1:
            if "autoscaling_config" in deployment_options:
                raise ValueError(
                    "autoscaling_config is not supported for DP deployment, "
                    "remove autoscaling_config instead."
                )

            # num_replicas is the total number of replicas across all DP groups.
            # If not specified, default to a single DP group (num_replicas = dp_group_size).
            num_replicas = deployment_options.get("num_replicas", dp_group_size)

            if num_replicas % dp_group_size != 0:
                raise ValueError(
                    f"num_replicas ({num_replicas}) must be divisible by "
                    f"data_parallel_size ({dp_group_size})."
                )

            num_groups = num_replicas // dp_group_size
            logger.info(
                f"DP deployment: {num_replicas} total replicas, "
                f"{num_groups} DP group(s) of size {dp_group_size}"
            )

            deployment_options["num_replicas"] = num_replicas

            if deployment_options["placement_group_strategy"] != "STRICT_PACK":
                logger.warning(
                    f"DP deployment with placement_strategy="
                    f"{deployment_options['placement_group_strategy']} "
                    "is not supported. Using STRICT_PACK instead."
                )
                deployment_options["placement_group_strategy"] = "STRICT_PACK"

        return deployment_options
