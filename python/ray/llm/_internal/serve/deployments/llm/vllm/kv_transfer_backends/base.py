import abc
import random
import string
from typing import Any, Dict


class BaseConnectorBackend(abc.ABC):
    def __init__(self, kv_transfer_config: Dict[str, Any]):
        """Base class for connector backends.

        Args:
            kv_transfer_config: Configuration for the KV transfer.
        """
        self.kv_transfer_config = kv_transfer_config

    def _get_unique_suffix(self, len: int = 6) -> str:
        """Generates unique alphanumeric suffix.

        Args:
            len: Length of the suffix to generate.
        Returns:
            A unique alphanumeric suffix string of specified length.
        """
        return "".join(random.choices(string.ascii_letters + string.digits, k=len))

    @abc.abstractmethod
    def setup(self) -> None:
        """Setup the connector backend.

        This method is called to setup the connector backend.
        """
        pass
