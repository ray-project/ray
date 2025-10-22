from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


def _check_lmcache_installed():
    try:
        import lmcache  # noqa: F401
    except ImportError:
        raise ImportError(
            "LMCache is not installed. Please install it with `pip install lmcache`."
        )


class LMCacheConnectorV1Backend(BaseConnectorBackend):

    KV_CONNECTOR_EXTRA_CONFIG_FIELD_NAME = "kv_connector_extra_config"
    LMCACHE_RPC_PORT_FIELD_NAME = "lmcache_rpc_port"
    DEFAULT_LMCACHE_RPC_PORT_NAME = "lmcache_rpc_port"

    def setup(self) -> None:
        """Initialize the LMCache connector backend.

        Creates a unique LMCache RPC port name across replicas by appending
        a random suffix to the base port name.

        Raises:
            ImportError: If LMCache is not installed.
        """
        _check_lmcache_installed()

        if (
            LMCacheConnectorV1Backend.KV_CONNECTOR_EXTRA_CONFIG_FIELD_NAME
            not in self.kv_transfer_config
        ):
            return

        kv_connector_extra_config = self.kv_transfer_config[
            LMCacheConnectorV1Backend.KV_CONNECTOR_EXTRA_CONFIG_FIELD_NAME
        ]
        base_value = kv_connector_extra_config.get(
            LMCacheConnectorV1Backend.LMCACHE_RPC_PORT_FIELD_NAME,
            LMCacheConnectorV1Backend.DEFAULT_LMCACHE_RPC_PORT_NAME,
        )

        # Append random suffix for uniqueness
        lmcache_rpc_port_value = str(base_value) + self._get_unique_suffix()
        if (
            LMCacheConnectorV1Backend.LMCACHE_RPC_PORT_FIELD_NAME
            in kv_connector_extra_config
        ):
            logger.info(
                f"Setting unique lmcache_rpc_port={lmcache_rpc_port_value} for current replica."
            )

        kv_connector_extra_config[
            LMCacheConnectorV1Backend.LMCACHE_RPC_PORT_FIELD_NAME
        ] = lmcache_rpc_port_value
