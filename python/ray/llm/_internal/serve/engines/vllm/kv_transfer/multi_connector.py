from typing import TYPE_CHECKING

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


class MultiConnectorBackend(BaseConnectorBackend):
    def __init__(self, llm_config: "LLMConfig"):
        super().__init__(llm_config)

    def setup(self) -> None:
        """Setup all connectors listed in the kv_transfer_config."""
        kv_transfer_config = self.kv_transfer_config
        connectors = kv_transfer_config.get("kv_connector_extra_config", {}).get(
            "connectors", []
        )

        for connector in connectors:
            connector_backend_str = connector.get("kv_connector")
            if connector_backend_str is None:
                raise ValueError("kv_connector is not set in the connector")

            # Use factory to get backend class lazily
            connector_backend = KVConnectorBackendFactory.create_backend(
                connector_backend_str, self.llm_config
            )
            connector_backend.setup()
