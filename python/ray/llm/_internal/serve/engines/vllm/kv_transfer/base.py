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

        For TP/PP cases, multiply by num_devices (tp × pp) to reserve
        sufficient port space, since each worker needs a unique port.
        Each TP worker adds its tp_rank (0, 1, ..., tp_size-1) to the
        base port at bind time, and PP stages also need separate ports.

        Returns:
            Non-negative integer offset to add to a base port.
        """
        # Prefer explicit DP rank when available
        dp_rank = self.llm_config.engine_kwargs.get("data_parallel_rank")
        if isinstance(dp_rank, int) and dp_rank >= 0:
            # vLLM already accounts for TP spacing in DP offset calculation
            # (data_parallel_rank × tp_size), don't multiply here
            return dp_rank

        # Fall back to Serve replica rank for TP/PP cases
        try:
            rc = serve.get_replica_context()
            if rc and hasattr(rc, "rank"):
                # Use num_devices (tp × pp) to reserve ports for all workers
                # Each replica spawns num_devices workers, each needing a unique port
                engine_config = self.llm_config.get_engine_config()
                num_devices = engine_config.num_devices
                return rc.rank * num_devices
        except Exception:
            # Best-effort fallback; avoid introducing failures in setup paths
            pass

        return 0

    def setup(self) -> None:
        """Setup the connector backend.

        This method is called to setup the connector backend.
        """
        pass
