from dataclasses import dataclass
import os
import threading
from typing import Optional

# The context singleton on this process.
_default_context: "Optional[DAGContext]" = None
_context_lock = threading.Lock()

# Default buffer size is 100MB.
DEFAULT_BUFFER_SIZE_BYTES = int(os.environ.get("RAY_DAG_buffer_size_bytes", 100 * 1e6))
# Default async_max_queue_size is 0, which means no limit.
DEFAULT_ASYNC_MAX_QUEUE_SIZE = int(os.environ.get("RAY_DAG_async_max_queue_size", 0))

DEFAULT_MAX_BUFFERED_RESULTS = int(os.environ.get("RAY_DAG_max_buffered_results", 1000))


@dataclass
class DAGContext:
    buffer_size_bytes: int = DEFAULT_BUFFER_SIZE_BYTES
    async_max_queue_size: int = DEFAULT_ASYNC_MAX_QUEUE_SIZE
    max_buffered_results: int = DEFAULT_MAX_BUFFERED_RESULTS

    @staticmethod
    def get_current() -> "DAGContext":
        global _default_context

        with _context_lock:
            if _default_context is None:
                _default_context = DAGContext()

            return _default_context
