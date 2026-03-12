import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import OpTask
from ray.data.block import BlockStats

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        TaskExecDriverStats,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar
    from ray.data.block import TaskExecWorkerStats


@dataclass(frozen=True)
class ProgressEstimate:
    """Estimated totals for a phase's output."""

    num_blocks: Optional[int] = None
    num_bundles: Optional[int] = None
    num_rows: Optional[int] = None


class PhaseHooks:
    """Shared task-lifecycle callbacks common to both shuffle and reduce phases.

    Subclassed by :class:`ShufflePhaseHooks` and :class:`ReducePhaseHooks`
    which add phase-specific hooks.
    """

    def on_task_submitted(
        self,
        task_index: int,
        input_bundle: RefBundle,
        task_id: str,
    ) -> None:
        """Called when a task is submitted."""

    def on_task_finished(
        self,
        task_index: int,
        exception: Optional[Exception],
        task_exec_stats: Optional["TaskExecWorkerStats"],
        task_exec_driver_stats: Optional["TaskExecDriverStats"],
    ) -> None:
        """Called when a task completes."""

    def on_progress(
        self,
        increment: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        """Update the phase's progress bar."""

    def set_progress_bar(self, bar: "BaseProgressBar") -> None:
        """Attach a progress bar to this phase."""

    def metrics_as_dict(self) -> Dict[str, Any]:
        """Return the phase's runtime metrics as a plain dict."""
        return {}

    def estimate_progress(
        self,
        num_tasks_submitted: int,
        upstream_op_num_outputs: int,
        total_num_tasks: Optional[int] = None,
    ) -> ProgressEstimate:
        """Estimate total output blocks, output bundles, and output rows."""
        return ProgressEstimate()


class ShufflePhaseHooks(PhaseHooks):
    """Callbacks for the shuffle (map) phase.

    Passed to :meth:`ShuffleEngine.submit_input`.  Inherits shared task
    lifecycle hooks from :class:`PhaseHooks` and adds shuffle-specific
    callbacks.

    Dataflow::

        Engine                           Hooks
        ──────                           ─────
        operator receives input
          └─ on_input_received ────────► metrics.on_input_received

        submit task to actor
          ├─ on_task_submitted ────────► metrics.on_task_submitted
          └─ on_progress ──────────────► bar.update(total=…)
                                           │
        task completes (callback)          │
          ├─ on_block_shuffled ─────────► block_stats.append
          ├─ on_task_output ────────────► metrics.on_output_taken
          │                                + on_task_output_generated
          ├─ on_task_finished ──────────► metrics.on_task_finished
          └─ on_progress ──────────────► bar.update(increment=…)
    """

    def on_input_received(self, bundle: RefBundle) -> None:
        """Record that the operator received an input bundle."""

    def on_task_output(
        self,
        task_index: int,
        output_bundle: RefBundle,
    ) -> None:
        """Called when intermediate shuffle output is available."""

    def on_block_shuffled(self, block_stats: BlockStats) -> None:
        """Record stats for a block that has been shuffled."""


class ReducePhaseHooks(PhaseHooks):
    """Callbacks for the reduce (finalize / extraction) phase.

    Passed to :meth:`ShuffleEngine.try_finalize`.  Inherits shared task
    lifecycle hooks from :class:`PhaseHooks` and adds reduce-specific
    callbacks.

    Dataflow::

        Engine                           Hooks
        ──────                           ─────
        schedule finalization task
          └─ on_task_submitted ────────► metrics.on_task_submitted
                                           │
        output bundle ready (callback)     │
          ├─ on_output_ready ───────────► output_queue.append
          │                                + metrics.on_output_queued
          │                                + metrics.on_task_output_generated
          ├─ on_output_estimated ───────► callback(num_bundles, num_rows)
          └─ on_progress ──────────────► bar.update(increment=…, total=…)
                                           │
        task completes (callback)          │
          └─ on_task_finished ──────────► metrics.on_task_finished

        operator takes output
          ├─ on_output_dequeued ────────► metrics.on_output_dequeued
          └─ on_output_taken ───────────► metrics.on_output_taken
    """

    def on_output_ready(
        self,
        bundle: RefBundle,
        task_index: int,
    ) -> None:
        """Enqueue a finalized output bundle."""

    def on_output_dequeued(self, bundle: RefBundle) -> None:
        """Called when an output bundle is dequeued from the output queue."""

    def on_output_taken(self, bundle: RefBundle) -> None:
        """Called when an output bundle is taken from the operator."""

    def on_output_estimated(
        self,
        num_output_bundles: Optional[int],
        num_output_rows: Optional[int],
    ) -> None:
        """Update PhysicalOperator output estimates."""


class ShuffleEngine(ABC):
    """Interface for the transport/lifecycle layer of a shuffle operator.

    Engines encapsulate actor management, task submission, finalization, and
    resource accounting.  They communicate back to the operator shell
    exclusively through phase-specific hooks — never by holding a reference
    to the operator or its state.

    Lifecycle driven by ``HashShufflingOperatorBase``::

        HashShufflingOperatorBase            ShuffleEngine
        ────────────────────────             ─────────────
        start(options)
          └─ engine.start() ─────────► create actors / pools

        _add_input_inner(bundle)
          ├─ shuffle_hooks.on_input_received
          └─ engine.submit_input() ──► route blocks to actors
                                        └─ shuffle_hooks.on_* (callbacks)

        has_next()
          └─ engine.try_finalize() ──► if inputs_complete and shuffle done:
                                        schedule reduce / extraction tasks
                                        └─ reduce_hooks.on_* (callbacks)
                                        └─ reduce_hooks.on_output_ready (→ queue)

        _get_next_inner()
          ├─ reduce_hooks.on_output_dequeued
          ├─ reduce_hooks.on_output_taken
          └─ pop from output_queue

        _do_shutdown()
          └─ engine.shutdown() ──────► kill actors, clear tasks

    The engine never reads ``PhysicalOperator`` fields directly.
    ``_inputs_complete`` and ``upstream_op_num_outputs()`` are passed as
    method parameters by ``HashShufflingOperatorBase``.
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
        hooks: ShufflePhaseHooks,
        upstream_op_num_outputs: int,
    ) -> None:
        """Route *bundle* to the appropriate actor(s) for shuffling."""
        ...

    @abstractmethod
    def try_finalize(
        self,
        hooks: ReducePhaseHooks,
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
