import typing
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import OpTask
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data.block import BlockStats

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        TaskExecDriverStats,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar
    from ray.data.block import TaskExecWorkerStats


@dataclass
class ShuffleCoreState:
    """Shared mutable state owned by ShuffleOperatorCore and read/written via
    ShuffleHooks.  The engine never touches this directly."""

    output_queue: Deque[RefBundle] = field(default_factory=deque)
    shuffle_metrics: Optional[OpRuntimeMetrics] = None
    reduce_metrics: Optional[OpRuntimeMetrics] = None
    shuffle_bar: Optional["BaseProgressBar"] = None
    reduce_bar: Optional["BaseProgressBar"] = None
    shuffled_block_stats: List[BlockStats] = field(default_factory=list)
    output_block_stats: List[BlockStats] = field(default_factory=list)


class ShuffleHooks:
    """Callback bundle passed from ShuffleOperatorCore to a ShuffleEngine.

    Each hook wraps metrics, progress-bar, and queue updates so the engine
    never imports ``OpRuntimeMetrics`` or ``BaseProgressBar``.
    """

    def on_shuffle_task_submitted(
        self,
        task_index: int,
        input_bundle: RefBundle,
        task_id: str,
    ) -> None:
        """Called when a shuffle (map-side) task is submitted."""

    def on_shuffle_task_finished(
        self,
        task_index: int,
        exception: Optional[Exception],
        task_exec_stats: Optional["TaskExecWorkerStats"],
        task_exec_driver_stats: Optional["TaskExecDriverStats"],
    ) -> None:
        """Called when a shuffle task completes."""

    def on_shuffle_task_output(
        self,
        task_index: int,
        output_bundle: RefBundle,
    ) -> None:
        """Called when intermediate shuffle output is available (CPU only)."""

    def on_shuffle_progress(
        self,
        increment: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        """Update the shuffle-phase progress bar."""

    def on_shuffled_block(self, block_stats: BlockStats) -> None:
        """Record stats for a block that has been shuffled."""

    def on_output_ready(
        self,
        bundle: RefBundle,
        task_index: int,
    ) -> None:
        """Enqueue a finalized output bundle."""

    def on_reduce_task_submitted(
        self,
        task_index: int,
        input_bundle: RefBundle,
        task_id: str,
    ) -> None:
        """Called when a reduce / extraction task is submitted."""

    def on_reduce_task_finished(
        self,
        task_index: int,
        exception: Optional[Exception],
        task_exec_stats: Optional["TaskExecWorkerStats"],
        task_exec_driver_stats: Optional["TaskExecDriverStats"],
    ) -> None:
        """Called when a reduce task completes."""

    def on_reduce_progress(
        self,
        increment: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        """Update the reduce-phase progress bar."""

    def on_output_estimated(
        self,
        num_output_bundles: Optional[int],
        num_output_rows: Optional[int],
    ) -> None:
        """Update PhysicalOperator output estimates."""

    def _get_shuffle_metrics(self) -> Optional["OpRuntimeMetrics"]:
        """Access shuffle metrics for progress estimation (engine internal use)."""
        return None

    def _get_reduce_metrics(self) -> Optional["OpRuntimeMetrics"]:
        """Access reduce metrics for progress estimation (engine internal use)."""
        return None


class ShuffleEngine(ABC):
    """Interface for the transport/lifecycle layer of a shuffle operator.

    Engines encapsulate actor management, task submission, finalization, and
    resource accounting.  They communicate back to the operator shell
    exclusively through :class:`ShuffleHooks` — never by holding a reference
    to the operator or its state.
    """

    @abstractmethod
    def start(self) -> None:
        """Create actors / pools and perform any blocking setup."""
        ...

    @abstractmethod
    def submit_input(
        self,
        bundle: RefBundle,
        input_index: int,
        hooks: ShuffleHooks,
        upstream_op_num_outputs: int,
    ) -> None:
        """Route *bundle* to the appropriate actor(s) for shuffling."""
        ...

    @abstractmethod
    def try_finalize(
        self,
        hooks: ShuffleHooks,
        inputs_complete: bool,
        upstream_op_num_outputs: int,
    ) -> None:
        """Schedule extraction / aggregation if all shuffle tasks are done."""
        ...

    @abstractmethod
    def get_active_tasks(self) -> List[OpTask]:
        """Return all currently active tasks (shuffle + reduce)."""
        ...

    @abstractmethod
    def has_completed(self) -> bool:
        """True when all finalization tasks have finished."""
        ...

    @abstractmethod
    def base_resource_usage(self) -> ExecutionResources:
        """Fixed resource footprint of long-lived actors."""
        ...

    @abstractmethod
    def incremental_resource_usage(self) -> ExecutionResources:
        """Resources required per additional input task."""
        ...

    def min_scheduling_resources(self) -> ExecutionResources:
        """Minimum resources to schedule a single task."""
        return self.incremental_resource_usage()

    def current_processor_usage(self) -> ExecutionResources:
        """Running sum of active shuffle-task resources (above base)."""
        return ExecutionResources.zero()

    @abstractmethod
    def shutdown(self, force: bool) -> None:
        """Release all actors and cancel pending tasks."""
        ...

    def debug_stats(self) -> Dict[str, Any]:
        """Optional engine-specific debug stats."""
        return {}

    @abstractmethod
    def progress_bar_names(self) -> List[str]:
        """Return ``[shuffle_bar_name, reduce_bar_name]``."""
        ...
