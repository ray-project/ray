from dataclasses import dataclass
import os
import threading
from typing import Optional
from ray.util.annotations import DeveloperAPI

# The context singleton on this process.
_default_context: "Optional[DAGContext]" = None
_context_lock = threading.Lock()

# Default buffer size is 100MB.
DEFAULT_BUFFER_SIZE_BYTES = int(os.environ.get("RAY_DAG_buffer_size_bytes", 100 * 1e6))
# Default async_max_queue_size is 0, which means no limit.
DEFAULT_ASYNC_MAX_QUEUE_SIZE = int(os.environ.get("RAY_DAG_async_max_queue_size", 0))

DEFAULT_MAX_BUFFERED_RESULTS = int(os.environ.get("RAY_DAG_max_buffered_results", 1000))


@DeveloperAPI
@dataclass
class DAGContext:
    """Global settings for Ray DAG.

    Args:
        buffer_size_bytes: The maximum size of messages that can be passed
            between tasks in the DAG.
        async_max_queue_size: The max queue size for the async execution.
        max_buffered_results: The maximum number of execution results that
            are allowed to be buffered. Setting a higher value allows more
            DAGs to be executed before `ray.get()` must be called but also
            increases the memory usage. Note that if the number of ongoing
            executions is beyond the DAG capacity, the new execution would
            be blocked in the first place; therefore, this limit is only
            enforced when it is smaller than the DAG capacity.
    """

    buffer_size_bytes: int = DEFAULT_BUFFER_SIZE_BYTES
    async_max_queue_size: int = DEFAULT_ASYNC_MAX_QUEUE_SIZE
    max_buffered_results: int = DEFAULT_MAX_BUFFERED_RESULTS

    @staticmethod
    def get_current() -> "DAGContext":
        """Get or create a singleton context.

        If the context has not yet been created in this process, it will be
        initialized with default settings.
        """
        global _default_context

        with _context_lock:
            if _default_context is None:
                _default_context = DAGContext()

            return _default_context
