import logging

from ray import serve
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.serve.handle import DeploymentHandle

logger = logging.getLogger(__name__)


class DPLLMServer(LLMServer):
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
        logger.info(f"DP rank: {self.dp_rank}")

        # override the engine_kwargs to assign the DP rank.
        llm_config.engine_kwargs["data_parallel_rank"] = self.dp_rank

        await super().__init__(llm_config)

    def _push_telemetry_report(self):
        # Only push telemetry report for the first DP replica.
        if self.dp_rank == 0:
            # TODO(rui): refine the telemetry report for DP deployment.
            super()._push_telemetry_report()

    @classmethod
    def as_deployment(cls, deployment_options: dict) -> serve.Deployment:
        return serve.deployment(cls).options(**deployment_options)
