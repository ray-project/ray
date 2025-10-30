import abc
import random
import string
from typing import TYPE_CHECKING, Any, Dict

from ray import serve

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig


class BaseConnectorBackend(abc.ABC):
    def __init__(self, llm_config: "LLMConfig"):
        """Base class for connector backends.

        Args:
            llm_config: The llm configuration for this engine
        """
        self.llm_config = llm_config

    @property
    def kv_transfer_config(self) -> Dict[str, Any]:
        engine_kwargs = self.llm_config.engine_kwargs
        kv_transfer_config = engine_kwargs.get("kv_transfer_config")
        assert (
            kv_transfer_config is not None
        ), "In Connector backend, kv_transfer_config is not set"
        return kv_transfer_config

    def _get_unique_suffix(self, len: int = 6) -> str:
        """Generates unique alphanumeric suffix.

        Args:
            len: Length of the suffix to generate.
        Returns:
            A unique alphanumeric suffix string of specified length.
        """
        return "".join(random.choices(string.ascii_letters + string.digits, k=len))

    def _compute_port_offset(self) -> int:
        """Compute a deterministic port offset for this replica.

        Uses data_parallel_rank if DP case, otherwise falls back to
        the replica rank assigned by Ray Serve (TP/PP case).

        Returns:
            Non-negative integer offset to add to a base port.
        """
        # Prefer explicit DP rank when available
        dp_rank = self.llm_config.engine_kwargs.get("data_parallel_rank")
        if isinstance(dp_rank, int) and dp_rank >= 0:
            return dp_rank

        # Fall back to Serve replica rank for TP/PP cases
        try:
            rc = serve.get_replica_context()
            if rc and hasattr(rc, "rank"):
                return rc.rank
        except Exception:
            # Best-effort fallback; avoid introducing failures in setup paths
            pass

        return 0

    @abc.abstractmethod
    def setup(self) -> None:
        """Setup the connector backend.

        This method is called to setup the connector backend.
        """
        pass
