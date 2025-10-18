import abc
import random
import string
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.server_models import LLMConfig


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
        """Compute a deterministic port offset for this replica/process.

        Priority:
        1) data_parallel_rank if present (set by DPServer).
        2) Stable hash of Serve replica tag (avoids cross-replica collisions when TP/PP only).

        Returns:
            A small non-negative integer to add to a base port.
        """
        # Prefer explicit DP rank when available
        dp_rank = self.llm_config.engine_kwargs.get("data_parallel_rank")
        if isinstance(dp_rank, int) and dp_rank >= 0:
            return dp_rank

        # Fall back to a stable hash of the Serve replica tag if available
        try:
            # Import locally to avoid import-time side effects
            from ray import serve  # type: ignore

            rc = serve.get_replica_context()
            if rc and getattr(rc, "replica_tag", None):
                import zlib

                # Keep the offset bounded to avoid large jumps
                return zlib.adler32(rc.replica_tag.encode("utf-8")) % 1024
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
