import abc
from typing import Any, Dict


class BaseConnectorBackend(abc.ABC):
    def __init__(self, kv_transfer_config: Dict[str, Any]):
        """Base class for connector backends.

        Args:
            kv_transfer_config: Configuration for the KV transfer.
        """
        self.kv_transfer_config = kv_transfer_config

    @abc.abstractmethod
    def setup(self) -> None:
        """Setup the connector backend.

        This method is called to setup the connector backend.
        """
        pass
