import os
import threading
from dataclasses import dataclass
from typing import Optional

from ray.util.annotations import DeveloperAPI

# The context singleton on this process.
_default_context: "Optional[DAGContext]" = None
_context_lock = threading.Lock()

DEFAULT_SUBMIT_TIMEOUT_S = int(os.environ.get("RAY_CGRAPH_submit_timeout", 10))
DEFAULT_GET_TIMEOUT_S = int(os.environ.get("RAY_CGRAPH_get_timeout", 10))
DEFAULT_TEARDOWN_TIMEOUT_S = int(os.environ.get("RAY_CGRAPH_teardown_timeout", 30))
DEFAULT_READ_ITERATION_TIMEOUT_S = float(
    os.environ.get("RAY_CGRAPH_read_iteration_timeout_s", 0.1)
)
# Default buffer size is 1MB.
DEFAULT_BUFFER_SIZE_BYTES = int(os.environ.get("RAY_CGRAPH_buffer_size_bytes", 1e6))
# The default number of in-flight executions that can be submitted before consuming the
# output.
DEFAULT_MAX_INFLIGHT_EXECUTIONS = int(
    os.environ.get("RAY_CGRAPH_max_inflight_executions", 10)
)

# The default number of results that can be buffered at the driver.
DEFAULT_MAX_BUFFERED_RESULTS = int(
    os.environ.get("RAY_CGRAPH_max_buffered_results", 1000)
)

DEFAULT_OVERLAP_GPU_COMMUNICATION = bool(
    os.environ.get("RAY_CGRAPH_overlap_gpu_communication", 0)
)


@DeveloperAPI
@dataclass
class DAGContext:
    """Global settings for Ray DAG.

    You can configure parameters in the DAGContext by setting the environment
    variables, `RAY_CGRAPH_<param>` (e.g., `RAY_CGRAPH_buffer_size_bytes`) or Python.

    Examples:
        >>> from ray.dag import DAGContext
        >>> DAGContext.get_current().buffer_size_bytes
        1000000
        >>> DAGContext.get_current().buffer_size_bytes = 500
        >>> DAGContext.get_current().buffer_size_bytes
        500

    Args:
        submit_timeout: The maximum time in seconds to wait for execute()
            calls.
        get_timeout: The maximum time in seconds to wait when retrieving
            a result from the DAG during `ray.get`. This should be set to a
            value higher than the expected time to execute the entire DAG.
        teardown_timeout: The maximum time in seconds to wait for the DAG to
            cleanly shut down.
        read_iteration_timeout: The timeout in seconds for each read iteration
            that reads one of the input channels. If the timeout is reached, the
            read operation will be interrupted and will try to read the next
            input channel. It must be less than or equal to `get_timeout`.
        buffer_size_bytes: The initial buffer size in bytes for messages
            that can be passed between tasks in the DAG. The buffers will
            be automatically resized if larger messages are written to the
            channel.
        max_inflight_executions: The maximum number of in-flight executions that
            can be submitted via `execute` or `execute_async` before consuming
            the output using `ray.get()`. If the caller submits more executions,
            `RayCgraphCapacityExceeded` is raised.
        overlap_gpu_communication: (experimental) Whether to overlap GPU
            communication with computation during DAG execution. If True, the
            communication and computation can be overlapped, which can improve
            the performance of the DAG execution.
    """

    submit_timeout: int = DEFAULT_SUBMIT_TIMEOUT_S
    get_timeout: int = DEFAULT_GET_TIMEOUT_S
    teardown_timeout: int = DEFAULT_TEARDOWN_TIMEOUT_S
    read_iteration_timeout: float = DEFAULT_READ_ITERATION_TIMEOUT_S
    buffer_size_bytes: int = DEFAULT_BUFFER_SIZE_BYTES
    max_inflight_executions: int = DEFAULT_MAX_INFLIGHT_EXECUTIONS
    max_buffered_results: int = DEFAULT_MAX_BUFFERED_RESULTS
    overlap_gpu_communication: bool = DEFAULT_OVERLAP_GPU_COMMUNICATION

    def __post_init__(self):
        if self.read_iteration_timeout > self.get_timeout:
            raise ValueError(
                "RAY_CGRAPH_read_iteration_timeout_s "
                f"({self.read_iteration_timeout}) must be less than or equal to "
                f"RAY_CGRAPH_get_timeout ({self.get_timeout})"
            )

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
