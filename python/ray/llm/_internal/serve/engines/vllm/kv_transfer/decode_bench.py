from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class DecodeBenchConnectorBackend(BaseConnectorBackend):
    """Backend for DecodeBenchConnector.

    DecodeBenchConnector is a KV connector for decode instance performance testing.
    It fills the KV cache with dummy values to emulate a prefill-decode disaggregated
    setting, allowing measurement of decoder performance under larger input sequence
    lengths in resource-limited environments.
    """

    def setup(self) -> None:
        """Initialize the DecodeBenchConnector backend.

        This connector requires minimal setup as it operates locally without
        external dependencies. It uses configuration from kv_connector_extra_config
        for fill parameters (fill_mean, fill_std) which are passed directly to vLLM.
        """
        logger.info("DecodeBenchConnector backend initialized for performance testing")

        # Log configuration if present
        if "kv_connector_extra_config" in self.kv_transfer_config:
            extra_config = self.kv_transfer_config["kv_connector_extra_config"]
            fill_mean = extra_config.get("fill_mean", 0.015)
            fill_std = extra_config.get("fill_std", 0.0)
            logger.info(
                f"DecodeBenchConnector config: fill_mean={fill_mean}, fill_std={fill_std}"
            )
