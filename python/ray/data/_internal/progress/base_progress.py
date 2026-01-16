import logging
import threading
import typing
from abc import ABC, abstractmethod
from typing import Any, List, Optional

import ray

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology
    from ray.types import ObjectRef

logger = logging.getLogger(__name__)

# Used a signal to cancel execution.
_canceled_threads = set()
_canceled_threads_lock = threading.Lock()


def _extract_num_rows(result: Any) -> int:
    """Extract the number of rows from a result object.

    Args:
        result: The result object from which to extract the number of rows.

    Returns:
        The number of rows, defaulting to 1 if it cannot be determined.
    """
    if hasattr(result, "num_rows"):
        return result.num_rows
    elif hasattr(result, "__len__"):
        # For output is DataFrame,i.e. sort_sample
        return len(result)
    else:
        return 1


class BaseProgressBar(ABC):
    """Base class to define a progress bar."""

    def block_until_complete(self, remaining: List["ObjectRef"]) -> None:
        t = threading.current_thread()
        while remaining:
            done, remaining = ray.wait(
                remaining, num_returns=len(remaining), fetch_local=False, timeout=0.1
            )
            total_rows_processed = 0
            for _, result in zip(done, ray.get(done)):
                num_rows = _extract_num_rows(result)
                total_rows_processed += num_rows
            self.update(total_rows_processed)

            with _canceled_threads_lock:
                if t in _canceled_threads:
                    break

    def fetch_until_complete(self, refs: List["ObjectRef"]) -> List[Any]:
        ref_to_result = {}
        remaining = refs
        t = threading.current_thread()
        # Triggering fetch_local redundantly for the same object is slower.
        # We only need to trigger the fetch_local once for each object,
        # raylet will persist these fetch requests even after ray.wait returns.
        # See https://github.com/ray-project/ray/issues/30375.
        fetch_local = True
        while remaining:
            done, remaining = ray.wait(
                remaining,
                num_returns=len(remaining),
                fetch_local=fetch_local,
                timeout=0.1,
            )
            if fetch_local:
                fetch_local = False
            total_rows_processed = 0
            for ref, result in zip(done, ray.get(done)):
                ref_to_result[ref] = result
                num_rows = _extract_num_rows(result)
                total_rows_processed += num_rows
            self.update(total_rows_processed)

            with _canceled_threads_lock:
                if t in _canceled_threads:
                    break

        return [ref_to_result[ref] for ref in refs]

    @abstractmethod
    def set_description(self, name: str) -> None:
        ...

    @abstractmethod
    def get_description(self) -> str:
        ...

    @abstractmethod
    def update(self, increment: int = 0, total: Optional[int] = None) -> None:
        ...

    def refresh(self):
        pass

    def close(self):
        pass


class BaseExecutionProgressManager(ABC):
    """Base Data Execution Progress Display Manager"""

    # If the name/description of the progress bar exceeds this length,
    # it will be truncated.
    MAX_NAME_LENGTH = 100

    # Total progress refresh rate (update interval in scheduling step)
    # refer to `streaming_executor.py::StreamingExecutor::_scheduling_loop_step`
    TOTAL_PROGRESS_REFRESH_EVERY_N_STEPS = 50

    @abstractmethod
    def __init__(self, dataset_id: str, topology: "Topology"):
        """Initialize the progress manager, create all necessary progress bars
        and sub-progress bars for the given topology. Sub-progress bars are
        created for operators that implement the SubProgressBarMixin.

        Args:
            dataset_id: id of Dataset
            topology: operation topology built via `build_streaming_topology`
        """
        ...

    @abstractmethod
    def start(self) -> None:
        """Start the progress manager."""
        ...

    @abstractmethod
    def refresh(self) -> None:
        """Refresh displayed progress."""
        ...

    @abstractmethod
    def close_with_finishing_description(self, desc: str, success: bool) -> None:
        """Close the progress manager with a finishing message.

        Args:
            desc: description to display
            success: whether the dataset execution was successful
        """
        ...

    @abstractmethod
    def update_total_progress(self, new_rows: int, total_rows: Optional[int]) -> None:
        """Update the total progress rows.

        Args:
            new_rows: new rows processed by the streaming_executor
            total_rows: total rows to be processed (if known)
        """
        ...

    @abstractmethod
    def update_total_resource_status(self, resource_status: str) -> None:
        """Update the total resource usage statistics.

        Args:
            resource_status: resource status information string.
        """
        ...

    @abstractmethod
    def update_operator_progress(
        self, opstate: "OpState", resource_manager: "ResourceManager"
    ) -> None:
        """Update individual operator progress.

        Args:
            opstate: opstate of the operator.
            resource_manager: the ResourceManager.
        """
        ...
