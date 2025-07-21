from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.base import (
    BaseConnectorBackend,
)


class LMCacheConnectorV1Backend(BaseConnectorBackend):
    def setup(self) -> None:
        """
        No manipulations with LMCacheConnectorV1 is needed.
        vLLM will configure the LMCache connector.
        """
        pass
