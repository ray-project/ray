"""SGLang Wide-EP (Elastic Expert Parallelism) server for Ray Serve.

Provides SGLangDPServer, a gang-scheduled server that reuses GangMasterInfoRegistry
from DPServer while handling SGLang-specific differences:
- Combined dist_init_addr format (vs separate address+port)
- moe_dp_size, enable_dp_attention engine kwargs
- No VLLM_RAY_BUNDLE_INDICES (SGLang RayEngine handles placement internally)
"""

import logging
import time
from typing import Optional, Type

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.engine.protocol import LLMEngine
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import (
    TIMEOUT_SECONDS,
    GangMasterInfoRegistry,
)
from ray.serve.config import (
    AutoscalingConfig,
    GangPlacementStrategy,
    GangRuntimeFailurePolicy,
    GangSchedulingConfig,
)
from ray.util.collective import get_address_and_port

logger = logging.getLogger(__name__)


class SGLangDPServer(LLMServer):
    """Gang-scheduled SGLang server for Wide-EP deployments.

    Key differences from DPServer:
    1. Reuses GangMasterInfoRegistry (same storage format)
    2. Combines address+port into dist_init_addr for SGLang
    3. NO VLLM_RAY_BUNDLE_INDICES computation (SGLang handles internally)
    4. Inject dist_init_addr into engine_kwargs (reuse SGLangEngine)
    """

    async def __init__(
        self,
        llm_config: LLMConfig,
        *,
        engine_cls: Optional[Type[LLMEngine]] = None,
    ):
        # Get gang context (same as DPServer)
        ctx = serve.get_replica_context()
        gang_context = ctx.gang_context

        if gang_context is None:
            raise RuntimeError(
                "SGLangDPServer requires gang scheduling. "
                "Set moe_dp_size > 1 in engine_kwargs to enable gang scheduling."
            )

        self.dp_rank = gang_context.rank
        self.gang_id = gang_context.gang_id
        self.dp_size = gang_context.world_size

        logger.info(
            f"SGLangDPServer initializing: rank={self.dp_rank}, "
            f"gang_id={self.gang_id}, world_size={self.dp_size}"
        )

        # Address discovery - REUSE GangMasterInfoRegistry
        # DPServer stores {address, port} separately
        # SGLang needs combined dist_init_addr = "{address}:{port}"
        if self.dp_rank == 0:
            address, port = get_address_and_port()
            GangMasterInfoRegistry.register(self.gang_id, address, port)
            dist_init_addr = f"{address}:{port}"
            logger.info(f"Rank 0 registered dist_init_addr={dist_init_addr}")
        else:
            timestamp = time.time()
            address, port = await GangMasterInfoRegistry.get(
                self.gang_id, timeout=TIMEOUT_SECONDS
            )
            dist_init_addr = f"{address}:{port}"
            logger.info(
                f"Rank {self.dp_rank} got dist_init_addr={dist_init_addr}, "
                f"waited {time.time() - timestamp:.3f} seconds"
            )

        # Inject dist_init_addr into engine_kwargs (same pattern as DPServer)
        llm_config.engine_kwargs["dist_init_addr"] = dist_init_addr

        # Initialize engine (reuse SGLangEngine - same engine class for all)
        from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangEngine

        await super().__init__(
            llm_config,
            engine_cls=engine_cls or SGLangEngine,
        )

    @classmethod
    def get_deployment_options(cls, llm_config: LLMConfig) -> dict:
        """Get deployment options with gang scheduling for Wide-EP.

        Parallel to DPServer.get_deployment_options().
        """
        deployment_options = super().get_deployment_options(llm_config)

        moe_dp_size = llm_config.engine_kwargs.get("moe_dp_size", 1)
        if not (isinstance(moe_dp_size, int) and moe_dp_size > 0):
            raise ValueError(
                f"Invalid moe_dp_size: {moe_dp_size}, expecting positive integer."
            )

        if moe_dp_size != 1:
            num_replicas = deployment_options.get("num_replicas")
            has_autoscaling = num_replicas == "auto" or (
                num_replicas is None and "autoscaling_config" in deployment_options
            )

            if has_autoscaling:
                autoscaling_config = AutoscalingConfig.default().dict()
                user_config = deployment_options.get("autoscaling_config")
                if user_config is not None:
                    autoscaling_config.update(user_config)

                logger.warning(
                    "In Wide-EP deployment, a replica refers to a DP group (gang). "
                    "Multiplying autoscaling_config's min_replicas, max_replicas, and "
                    f"initial_replicas by moe_dp_size ({moe_dp_size})."
                )
                for key in ["min_replicas", "max_replicas", "initial_replicas"]:
                    if autoscaling_config.get(key) is not None:
                        autoscaling_config[key] *= moe_dp_size

                deployment_options["autoscaling_config"] = autoscaling_config
            elif num_replicas is not None:
                logger.warning(
                    "In Wide-EP deployment, num_replicas refers to the number of DP groups. "
                    f"Multiplying num_replicas ({num_replicas}) by moe_dp_size ({moe_dp_size}) "
                    f"to get the total number of serve replicas ({num_replicas * moe_dp_size})."
                )
                deployment_options["num_replicas"] = num_replicas * moe_dp_size
            else:
                deployment_options["num_replicas"] = moe_dp_size

            deployment_options["gang_scheduling_config"] = GangSchedulingConfig(
                gang_size=moe_dp_size,
                gang_placement_strategy=GangPlacementStrategy.PACK,
                runtime_failure_policy=GangRuntimeFailurePolicy.RESTART_GANG,
            )

            # Remove per-replica placement_group_strategy
            if "placement_group_strategy" in deployment_options:
                logger.warning(
                    "placement_group_strategy configured in the deployment config is ignored. "
                    "Wide-EP deployment uses PACK strategy for scheduling DP groups."
                )
                deployment_options.pop("placement_group_strategy", None)

            logger.info(f"Wide-EP deployment: gang_size={moe_dp_size}")

        return deployment_options
