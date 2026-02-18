import asyncio
import logging
import os
import time

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.serve.config import GangPlacementStrategy, GangSchedulingConfig, GangRuntimeFailurePolicy
from ray.serve.handle import DeploymentHandle
from ray.util.collective.collective import get_address_and_port

logger = logging.getLogger(__name__)


@serve.deployment(num_replicas=1)
class _GangDPMasterInfoBroker:
    """Holds the DP master address/port for other DP replicas to retrieve."""

    def __init__(self):
        self.dp_address = None
        self.dp_rpc_port = None
        self.event = asyncio.Event()

    async def set_master_info(self, dp_address: str, dp_rpc_port: int):
        self.dp_address = dp_address
        self.dp_rpc_port = dp_rpc_port
        self.event.set()

    async def get_master_info(self):
        await self.event.wait()
        return self.dp_address, self.dp_rpc_port


class GangDPServer(LLMServer):
    """
    Gang-scheduled Data Parallel LLM Server.

    Uses Ray Serve's gang scheduling so that if any replica in a DP group deployment
    fails, the entire group is restarted atomically.
    """

    async def __init__(
        self, llm_config: LLMConfig, master_info_broker: DeploymentHandle
    ):
        ctx = serve.get_replica_context()
        gang_context = ctx.gang_context

        if gang_context is None:
            raise RuntimeError(
                "GangDPServer requires gang scheduling to be enabled. "
                "Set gang_scheduling_config in the deployment options."
            )

        self.dp_rank = gang_context.rank
        self.gang_id = gang_context.gang_id
        dp_size = gang_context.world_size

        logger.info(
            f"GangDPServer replica initialized: dp_rank={self.dp_rank}, "
            f"dp_size={dp_size}, gang_id={self.gang_id}"
        )

        if self.dp_rank == 0:
            self.dp_address, self.dp_rpc_port = get_address_and_port()
            await master_info_broker.set_master_info.remote(
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
            ) = await master_info_broker.get_master_info.remote()
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

        # Direct vLLM to use this replica's bundle within the gang placement group
        os.environ["VLLM_RAY_BUNDLE_INDICES"] = str(self.dp_rank)

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
            deployment_options["gang_scheduling_config"] = GangSchedulingConfig(
                gang_size=dp_size,
                gang_placement_strategy=GangPlacementStrategy.PACK,
                runtime_failure_policy=GangRuntimeFailurePolicy.RESTART_GANG,
            )
            # Remove per-replica placement_group_strategy
            deployment_options.pop("placement_group_strategy", None)

        return deployment_options
