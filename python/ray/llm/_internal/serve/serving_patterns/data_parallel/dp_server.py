import asyncio
import json
import logging
import os
import time
from typing import Tuple

from ray import serve
from ray.experimental.internal_kv import (
    _internal_kv_del,
    _internal_kv_get,
    _internal_kv_put,
)
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.llm_server import LLMServer
from ray.serve.config import (
    GangPlacementStrategy,
    GangRuntimeFailurePolicy,
    GangSchedulingConfig,
)
from ray.util.collective.collective import get_address_and_port

logger = logging.getLogger(__name__)

TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 0.5


class GangMasterInfoRegistry:
    """Registry for gang DP master info using GCS KV store."""

    _KEY_PREFIX = "LLMServeRegistry:serve_global:gang_dp_master/"

    @classmethod
    def _make_key(cls, gang_id: str) -> bytes:
        return (cls._KEY_PREFIX + gang_id).encode("utf-8")

    @classmethod
    def register(cls, gang_id: str, address: str, port: int) -> None:
        """Store the DP master info in GCS KV store."""
        key = cls._make_key(gang_id)
        value = json.dumps({"address": address, "port": port}).encode("utf-8")
        _internal_kv_put(key, value, overwrite=True)

    @classmethod
    def unregister(cls, gang_id: str) -> None:
        """Remove the DP master info from GCS KV store."""
        key = cls._make_key(gang_id)
        try:
            _internal_kv_del(key)
        except Exception:
            logger.warning(
                f"Failed to unregister gang master info for gang {gang_id}.",
                exc_info=True,
            )

    @classmethod
    async def get(
        cls,
        gang_id: str,
        timeout: float = TIMEOUT_SECONDS,
        poll_interval: float = POLL_INTERVAL_SECONDS,
    ) -> Tuple[str, int]:
        """Retrieve the DP master info for gang_id, polling until available.

        Args:
            gang_id: The ID of the gang.
            timeout: The timeout in seconds.
            poll_interval: The poll interval in seconds.

        Returns:
            A tuple of (address, port).

        Raises:
            TimeoutError: If the info is not available within timeout_seconds seconds.
        """
        key = cls._make_key(gang_id)
        deadline = time.monotonic() + timeout
        while True:
            data = _internal_kv_get(key)
            if data is not None:
                info = json.loads(data)
                return info["address"], info["port"]
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for DP master info for gang {gang_id} "
                    f"after {timeout}s."
                )
            await asyncio.sleep(poll_interval)


class DPServer(LLMServer):
    """
    Gang-scheduled Data Parallel LLM Server.

    Uses Ray Serve's gang scheduling so that if any replica in a DP group deployment
    fails, the entire group is restarted atomically.
    """

    async def __init__(self, llm_config: LLMConfig):
        ctx = serve.get_replica_context()
        gang_context = ctx.gang_context

        if gang_context is None:
            raise RuntimeError(
                "DPServer requires gang scheduling to be enabled. "
                "Set gang_scheduling_config in the deployment options."
            )

        self.dp_rank = gang_context.rank
        self.gang_id = gang_context.gang_id
        self.dp_size = gang_context.world_size

        logger.info(
            f"DPServer replica initialized: dp_rank={self.dp_rank}, "
            f"dp_size={self.dp_size}, gang_id={self.gang_id}"
        )

        if self.dp_rank == 0:
            self.dp_address, self.dp_rpc_port = get_address_and_port()
            GangMasterInfoRegistry.register(
                self.gang_id, self.dp_address, self.dp_rpc_port
            )
            logger.info(
                f"DP rank {self.dp_rank} has set DP master info: "
                f"data_parallel_address={self.dp_address}, "
                f"data_parallel_rpc_port={self.dp_rpc_port}"
            )
        else:
            timestamp = time.time()
            self.dp_address, self.dp_rpc_port = await GangMasterInfoRegistry.get(
                self.gang_id
            )
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

        engine_config = llm_config.get_engine_config()

        # Direct vLLM to use this replica's bundles within the gang placement group.
        # Gang placement group concatenates per-replica bundles for all ranks, so
        # rank i owns bundles [i*B, i*B+1, ..., i*B+B-1] where B is the number of
        # bundles per DP replica.
        #
        # Example: dp_size=2, tp_size=2
        #   Gang placement group = [{GPU: 1}, {GPU: 1}, {GPU: 1}, {GPU: 1}]
        #                           ^^^ DP rank 0 ^^^   ^^^ DP rank 1 ^^^
        #
        # placement_bundles below is the gang placement group, and therefore
        # get_current_placement_group from the actor yields the gang placement group,
        # not the per-replica placement group.
        bundles_per_replica = len(engine_config.placement_bundles)
        os.environ["VLLM_RAY_BUNDLE_INDICES"] = self._compute_bundle_indices(
            self.dp_rank, bundles_per_replica
        )

        await super().__init__(llm_config)

    @staticmethod
    def _compute_bundle_indices(dp_rank: int, bundles_per_replica: int) -> str:
        start = dp_rank * bundles_per_replica
        return ",".join(str(start + i) for i in range(bundles_per_replica))

    @classmethod
    def get_deployment_options(cls, llm_config: "LLMConfig"):
        deployment_options = super().get_deployment_options(llm_config)

        dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
        if not (isinstance(dp_size, int) and dp_size > 0):
            raise ValueError(
                f"Invalid data_parallel_size: {dp_size}, expecting positive integer."
            )
        if dp_size != 1:
            num_replicas = deployment_options.get("num_replicas")
            if num_replicas != "auto":
                if num_replicas is not None:
                    if num_replicas % dp_size != 0:
                        raise ValueError(
                            f"num_replicas ({num_replicas}) must be a multiple of "
                            f"data_parallel_size ({dp_size}) for gang DP deployment."
                        )
                else:
                    deployment_options["num_replicas"] = dp_size

            deployment_options["gang_scheduling_config"] = GangSchedulingConfig(
                gang_size=dp_size,
                gang_placement_strategy=GangPlacementStrategy.PACK,
                runtime_failure_policy=GangRuntimeFailurePolicy.RESTART_GANG,
            )
            # Remove per-replica placement_group_strategy
            deployment_options.pop("placement_group_strategy", None)

        return deployment_options
