"""SGLang WideEP Server for Ray Serve LLM.

Gang-scheduled WideEP server wrapping SGLang RayEngine with gang_size=1.
WideEP is enabled via moe_a2a_backend="deepep" which automatically sets
ep_size = tp_size.
"""

import copy
import os

from ray import serve
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangServer
from ray.llm._internal.serve.engines.sglang.sglang_models import SGLangEngineConfig
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.utils.pg_utils import get_bundle_indices_sorted_by_node
from ray.serve.config import (
    GangPlacementStrategy,
    GangRuntimeFailurePolicy,
    GangSchedulingConfig,
)
from ray.util.placement_group import get_placement_group

logger = get_logger(__name__)


class SGLangWideEPServer(SGLangServer):
    """Gang-scheduled WideEP server wrapping SGLang RayEngine with gang_size=1.

    Inherits all API methods (chat, completions, embeddings, etc.) from SGLangServer.
    Only overrides __init__ for WideEP-specific setup and get_deployment_options
    for gang scheduling configuration.
    """

    def __init__(self, llm_config: LLMConfig):
        """Initialize WideEP server with gang scheduling and RayEngine.

        Args:
            llm_config: LLM configuration with moe_a2a_backend="deepep" to enable WideEP.

        Raises:
            RuntimeError: If gang scheduling is not enabled.
            ValueError: If WideEP configuration constraints are violated.
        """
        # Validate WideEP configuration
        engine_config = SGLangEngineConfig.from_llm_config(llm_config)
        engine_config.validate_wideep_config()

        ctx = serve.get_replica_context()
        gang_context = ctx.gang_context

        if gang_context is None:
            raise RuntimeError(
                "SGLangWideEPServer requires gang scheduling to be enabled. "
                "Set gang_scheduling_config in the deployment options."
            )

        self.gang_id = gang_context.gang_id
        logger.info(
            f"SGLangWideEPServer replica initialized: gang_id={self.gang_id}, "
            f"gang_size={gang_context.world_size}"
        )

        # Get gang placement group and pass to RayEngine
        pg = get_placement_group(gang_context.pg_name)

        # Sort bundle indices by node to ensure TP ranks are co-located
        sorted_indices = get_bundle_indices_sorted_by_node(pg)
        tp_size = llm_config.engine_kwargs.get("tp_size", 1)
        pp_size = llm_config.engine_kwargs.get("pp_size", 1)
        num_devices = tp_size * pp_size

        # Set SGLANG_RAY_BUNDLE_INDICES for RayEngine to place SchedulerActors
        os.environ["SGLANG_RAY_BUNDLE_INDICES"] = ",".join(
            str(idx) for idx in sorted_indices[:num_devices]
        )
        logger.info(
            f"SGLANG_RAY_BUNDLE_INDICES set to: {os.environ['SGLANG_RAY_BUNDLE_INDICES']}"
        )

        # Pass placement_group to RayEngine for SchedulerActor placement
        llm_config.engine_kwargs["placement_group"] = pg

        # Import SGLang and call parent with RayEngine class
        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e

        super().__init__(llm_config, engine_cls=sglang.RayEngine)

        logger.info(
            f"SGLang RayEngine initialized with tp_size={tp_size}, "
            f"nnodes={llm_config.engine_kwargs.get('nnodes', 1)}, "
            f"moe_a2a_backend={llm_config.engine_kwargs.get('moe_a2a_backend')}"
        )

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        """Get deployment options for SGLang WideEP.

        Returns gang_scheduling_config with gang_size=1 and placement_group_bundles
        based on tp_size/pp_size/nnodes.

        Args:
            llm_config: LLM configuration.

        Returns:
            dict with placement_group_bundles, placement_group_strategy,
            and gang_scheduling_config.
        """
        # Start with user's deployment_config to preserve settings like autoscaling_config
        deployment_options = copy.deepcopy(llm_config.deployment_config)

        tp_size = llm_config.engine_kwargs.get("tp_size", 1)
        pp_size = llm_config.engine_kwargs.get("pp_size", 1)
        nnodes = llm_config.engine_kwargs.get("nnodes", 1)
        num_devices = tp_size * pp_size

        if tp_size < 1 or pp_size < 1:
            raise ValueError(
                f"Invalid configuration: tp_size={tp_size} and pp_size={pp_size}. "
                f"Both must be >= 1."
            )

        # Placement group bundles
        if nnodes > 1:
            gpus_per_node = num_devices // nnodes
            pg_bundles = [{"GPU": gpus_per_node} for _ in range(nnodes)]
        else:
            pg_bundles = [{"GPU": 1} for _ in range(num_devices)]

        deployment_options["placement_group_bundles"] = pg_bundles

        deployment_options["gang_scheduling_config"] = GangSchedulingConfig(
            gang_size=1,
            gang_placement_strategy=(
                GangPlacementStrategy.SPREAD
                if nnodes > 1
                else GangPlacementStrategy.PACK
            ),
            runtime_failure_policy=GangRuntimeFailurePolicy.RESTART_GANG,
        )

        deployment_options.pop("placement_group_strategy", None)

        logger.info(
            f"SGLangWideEPServer deployment options: "
            f"gang_size=1, pg_bundles={len(pg_bundles)}"
        )

        return deployment_options
