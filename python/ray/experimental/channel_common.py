import threading
from dataclasses import dataclass
from typing import Optional

from ray.experimental.nccl_group import _NcclGroup
from ray.util.annotations import DeveloperAPI

# The context singleton on this process.
_default_context: "Optional[ChannelContext]" = None
_context_lock = threading.Lock()


@DeveloperAPI
@dataclass
class ChannelContext:
    nccl_group: Optional["_NcclGroup"] = None

    @staticmethod
    def get_current() -> "ChannelContext":
        """Get or create a singleton context.

        If the context has not yet been created in this process, it will be
        initialized with default settings.
        """

        global _default_context

        with _context_lock:
            if _default_context is None:
                _default_context = ChannelContext()

            return _default_context
