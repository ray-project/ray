import logging

from ray import serve
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.llm.llm_server import LLMServer
from ray.serve.handle import DeploymentHandle

logger = logging.getLogger(__name__)


class DPLLMServer(LLMServer):
    async def __init__(self, llm_config: LLMConfig, dp_rank_assigner: DeploymentHandle):

        # in DP case, in here, we wait to get all replica names.
        # we assign a rank to each replica.
        # Then locally construct dp rank based on current replica name and the
        # consistent rank assignment. Since the logic is similar the mapping
        # should be identical given the global view of replica names.

        # For engine, set dp_size and dp_rank.
        self.dp_rank_assigner = dp_rank_assigner

        replica_ctx = serve.get_replica_context()
        self.dp_rank = await self.dp_rank_assigner.register.remote(replica_ctx)
        logger.info(f"DP rank: {self.dp_rank}")

        # override the engine_kwargs
        llm_config.engine_kwargs["data_parallel_rank"] = self.dp_rank

        await super().__init__(llm_config)

    async def _start_engine(self):
        if self.engine is None:
            raise ValueError("Engine is not set")

        await self.engine.start()

    @classmethod
    def as_deployment(cls, num_replicas: int) -> serve.Deployment:
        return serve.deployment(cls).options(num_replicas=num_replicas)
