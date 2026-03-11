import typing
from typing import Any, Dict, List, Optional

from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import OpTask
from ray.data._internal.execution.operators.shuffle_engine import (
    ShuffleCoreState,
    ShuffleEngine,
    ShuffleHooks,
)
from ray.data._internal.execution.operators.sub_progress import SubProgressBarMixin
from ray.data._internal.stats import OpRuntimeMetrics
from ray.data.block import BlockStats, to_stats
from ray.data.context import DataContext

if typing.TYPE_CHECKING:
    from ray.data._internal.execution.interfaces.physical_operator import (
        TaskExecDriverStats,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar
    from ray.data.block import TaskExecWorkerStats

StatsDict = Dict[str, List[BlockStats]]


class _BoundShuffleHooks(ShuffleHooks):
    """Concrete hooks that wire engine callbacks to a ``ShuffleCoreState``
    and ``PhysicalOperator`` output-estimation fields."""

    def __init__(
        self,
        state: ShuffleCoreState,
        operator: "ShuffleOperatorCore",
    ):
        self._state = state
        self._operator = operator

    def on_shuffle_task_submitted(
        self,
        task_index: int,
        input_bundle: RefBundle,
        task_id: str,
    ) -> None:
        self._state.shuffle_metrics.on_task_submitted(
            task_index, input_bundle, task_id=task_id
        )

    def on_shuffle_task_finished(
        self,
        task_index: int,
        exception: Optional[Exception],
        task_exec_stats: Optional["TaskExecWorkerStats"],
        task_exec_driver_stats: Optional["TaskExecDriverStats"],
    ) -> None:
        self._state.shuffle_metrics.on_task_finished(
            task_index,
            exception,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )

    def on_shuffle_task_output(
        self,
        task_index: int,
        output_bundle: RefBundle,
    ) -> None:
        self._state.shuffle_metrics.on_output_taken(output_bundle)
        self._state.shuffle_metrics.on_task_output_generated(task_index, output_bundle)

    def on_shuffle_progress(
        self,
        increment: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        if self._state.shuffle_bar is not None:
            self._state.shuffle_bar.update(increment=increment or 0, total=total)

    def on_shuffled_block(self, block_stats: BlockStats) -> None:
        self._state.shuffled_block_stats.append(block_stats)

    def on_output_ready(
        self,
        bundle: RefBundle,
        task_index: int,
    ) -> None:
        self._state.output_queue.append(bundle)
        self._state.reduce_metrics.on_output_queued(bundle)
        self._state.reduce_metrics.on_task_output_generated(
            task_index=task_index, output=bundle
        )

    def on_reduce_task_submitted(
        self,
        task_index: int,
        input_bundle: RefBundle,
        task_id: str,
    ) -> None:
        self._state.reduce_metrics.on_task_submitted(
            task_index, input_bundle, task_id=task_id
        )

    def on_reduce_task_finished(
        self,
        task_index: int,
        exception: Optional[Exception],
        task_exec_stats: Optional["TaskExecWorkerStats"],
        task_exec_driver_stats: Optional["TaskExecDriverStats"],
    ) -> None:
        self._state.reduce_metrics.on_task_finished(
            task_index,
            exception,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )

    def on_reduce_progress(
        self,
        increment: Optional[int] = None,
        total: Optional[int] = None,
    ) -> None:
        if self._state.reduce_bar is not None:
            self._state.reduce_bar.update(increment=increment or 0, total=total)

    def on_output_estimated(
        self,
        num_output_bundles: Optional[int],
        num_output_rows: Optional[int],
    ) -> None:
        self._operator._estimated_num_output_bundles = num_output_bundles
        self._operator._estimated_output_num_rows = num_output_rows

    def _get_shuffle_metrics(self):
        return self._state.shuffle_metrics

    def _get_reduce_metrics(self):
        return self._state.reduce_metrics


class ShuffleOperatorCore(PhysicalOperator, SubProgressBarMixin):
    """Generic operator shell for hash-shuffle-based operators.

    All lifecycle plumbing (metrics, progress bars, output queue, stats,
    completion tracking) lives here.  Transport-specific logic is delegated
    to a pluggable :class:`ShuffleEngine`.
    """

    def __init__(
        self,
        name: str,
        input_ops: List[PhysicalOperator],
        data_context: DataContext,
        engine: ShuffleEngine,
    ):
        super().__init__(
            name=name,
            input_dependencies=input_ops,
            data_context=data_context,
        )
        self._engine = engine
        self._state = ShuffleCoreState(
            shuffle_metrics=OpRuntimeMetrics(self),
            reduce_metrics=OpRuntimeMetrics(self),
        )
        self._hooks = _BoundShuffleHooks(self._state, self)

    # ------------------------------------------------------------------
    # PhysicalOperator lifecycle
    # ------------------------------------------------------------------

    def start(self, options: ExecutionOptions) -> None:
        super().start(options)
        self._engine.start()

    def _add_input_inner(self, bundle: RefBundle, input_index: int) -> None:
        self._state.shuffle_metrics.on_input_received(bundle)
        self._engine.submit_input(
            bundle,
            input_index,
            self._hooks,
            self.upstream_op_num_outputs(),
        )

    def has_next(self) -> bool:
        self._engine.try_finalize(
            self._hooks,
            self._inputs_complete,
            self.upstream_op_num_outputs(),
        )
        return len(self._state.output_queue) > 0

    def _get_next_inner(self) -> RefBundle:
        bundle = self._state.output_queue.popleft()
        self._state.reduce_metrics.on_output_dequeued(bundle)
        self._state.reduce_metrics.on_output_taken(bundle)
        self._state.output_block_stats.extend(to_stats(bundle.metadata))
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return self._engine.get_active_tasks()

    def has_completed(self) -> bool:
        return self._engine.has_completed() and super().has_completed()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _do_shutdown(self, force: bool = False) -> None:
        # Kill actors before cancelling tasks: the base method blocks on
        # ray.get() for each active task during forced shutdown, which hangs
        # if the actors backing those tasks are still alive.
        self._engine.shutdown(force=force)
        super()._do_shutdown(force=force)

    # ------------------------------------------------------------------
    # Resource accounting
    # ------------------------------------------------------------------

    @property
    def base_resource_usage(self) -> ExecutionResources:
        return self._engine.base_resource_usage()

    def current_logical_usage(self) -> ExecutionResources:
        return self._engine.base_resource_usage().add(
            self._engine.current_processor_usage()
        )

    def incremental_resource_usage(self) -> ExecutionResources:
        return self._engine.incremental_resource_usage()

    def min_scheduling_resources(self) -> ExecutionResources:
        return self._engine.min_scheduling_resources()

    # ------------------------------------------------------------------
    # SubProgressBarMixin
    # ------------------------------------------------------------------

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return self._engine.progress_bar_names()

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        names = self._engine.progress_bar_names()
        if len(names) >= 1 and name == names[0]:
            self._state.shuffle_bar = pg
        elif len(names) >= 2 and name == names[1]:
            self._state.reduce_bar = pg

    # ------------------------------------------------------------------
    # Stats / metrics
    # ------------------------------------------------------------------

    def get_stats(self) -> StatsDict:
        shuffle_name = f"{self._name}_shuffle"
        reduce_name = f"{self._name}_finalize"
        return {
            shuffle_name: self._state.shuffled_block_stats,
            reduce_name: self._state.output_block_stats,
        }

    def _extra_metrics(self) -> Dict[str, Any]:
        shuffle_name = f"{self._name}_shuffle"
        finalize_name = f"{self._name}_finalize"
        return {
            shuffle_name: self._state.shuffle_metrics.as_dict(),
            finalize_name: self._state.reduce_metrics.as_dict(),
        }
