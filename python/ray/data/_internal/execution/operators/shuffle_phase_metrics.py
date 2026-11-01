import typing
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from ray.data._internal.execution.interfaces import RefBundle
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    OpRuntimeMetrics,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    estimate_total_num_of_blocks,
)
from ray.data._internal.stats import StatsDict
from ray.data.block import BlockMetadata, BlockStats

if typing.TYPE_CHECKING:
    import ray
    from ray.data._internal.execution.interfaces.physical_operator import (
        PhysicalOperator,
        TaskExecDriverStats,
    )
    from ray.data._internal.progress.base_progress import BaseProgressBar
    from ray.data.block import TaskExecWorkerStats


@dataclass(frozen=True)
class PhaseSpec:
    """Static description of one phase of a shuffle-family operator.

    Attributes:
        name: Key used in stats, extra metrics and Prometheus tags.
        progress_bar_name: Display name of the phase's sub-progress bar.
        is_output_phase: Whether this phase produces the operator's outputs.
            Exactly one phase per operator must be the output phase.
        total_num_tasks: Number of tasks the phase will run, if known upfront.
            Used for output estimates.
    """

    name: str
    progress_bar_name: str
    is_output_phase: bool = False
    total_num_tasks: Optional[int] = None


class ShufflePhaseMetrics:
    """Metrics, block stats and progress bar for one phase of an operator.

    Only the output phase forwards its events to the operator-level metrics,
    so the operator's averages and task durations describe one kind of task.
    A single-phase operator aliases its phase to the operator's metrics and
    forwards nothing.
    """

    def __init__(
        self,
        spec: PhaseSpec,
        *,
        metrics: OpRuntimeMetrics,
        op_metrics: OpRuntimeMetrics,
        forward: bool,
    ):
        self.spec = spec
        self.metrics = metrics
        self.block_stats: List[BlockStats] = []
        # Shuffle-specific counters surfaced next to the OpRuntimeMetrics fields.
        self.custom: Dict[str, Union[int, float]] = {}
        self.total_num_tasks = spec.total_num_tasks

        self._op_metrics = op_metrics
        self._forward = forward
        self._bar: Optional["BaseProgressBar"] = None

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def is_output_phase(self) -> bool:
        return self.spec.is_output_phase

    def on_task_submitted(
        self,
        task_index: int,
        inputs: RefBundle,
        task_id: Optional["ray.TaskID"] = None,
    ) -> None:
        self.metrics.on_task_submitted(task_index, inputs, task_id=task_id)
        if self._forward:
            self._op_metrics.on_task_submitted(task_index, inputs, task_id=task_id)

    def on_task_output(self, task_index: int, output: RefBundle) -> None:
        self.metrics.on_task_output_generated(task_index, output)
        if self._forward:
            self._op_metrics.on_task_output_generated(task_index, output)

    def on_task_finished(
        self,
        task_index: int,
        exception: Optional[Exception],
        task_exec_stats: Optional["TaskExecWorkerStats"],
        task_exec_driver_stats: Optional["TaskExecDriverStats"],
    ) -> None:
        self.metrics.on_task_finished(
            task_index,
            exception,
            task_exec_stats=task_exec_stats,
            task_exec_driver_stats=task_exec_driver_stats,
        )
        if self._forward:
            self._op_metrics.on_task_finished(
                task_index,
                exception,
                task_exec_stats=task_exec_stats,
                task_exec_driver_stats=task_exec_driver_stats,
            )

    def on_output_queued(self, output: RefBundle) -> None:
        self.metrics.on_output_queued(output)
        if self._forward:
            self._op_metrics.on_output_queued(output)

    def on_output_dequeued(self, output: RefBundle) -> None:
        self.metrics.on_output_dequeued(output)
        if self._forward:
            self._op_metrics.on_output_dequeued(output)
            # A dequeue only happens inside ``get_next``, which records the take
            # on the operator but cannot see a separate phase object.
            self.metrics.on_output_taken(output)

    def record_block_stats(self, *metadata: BlockMetadata) -> None:
        self.block_stats.extend(m.to_stats() for m in metadata)

    def set_progress_bar(self, bar: "BaseProgressBar") -> None:
        self._bar = bar

    def update_progress(self, increment: int = 0, total: Optional[int] = None) -> None:
        if self._bar is not None:
            self._bar.update(increment=increment, total=total)

    def estimate_totals(
        self, num_tasks_submitted: int, upstream_op_num_outputs: int
    ) -> Tuple[int, int]:
        """Estimate this phase's total output bundles and rows."""
        if self.total_num_tasks is not None:
            # ``estimate_total_num_of_blocks`` returns 0 when tasks have empty
            # input bundles, as finalize tasks do, so scale by task count instead.
            outputs_per_task = self.metrics.average_num_outputs_per_task
            rows_per_task = self.metrics.average_rows_outputs_per_task
            if not outputs_per_task or rows_per_task is None:
                return 0, 0
            return (
                round(self.total_num_tasks * outputs_per_task),
                round(self.total_num_tasks * rows_per_task),
            )
        _, num_bundles, num_rows = estimate_total_num_of_blocks(
            num_tasks_submitted,
            upstream_op_num_outputs,
            self.metrics,
            total_num_tasks=None,
        )
        return num_bundles, num_rows

    def as_dict(self) -> Dict[str, Any]:
        # Resource usage belongs to the whole operator, not to one phase.
        return {
            **self.metrics.as_dict(include_resource_usage=False),
            **self.custom,
        }


class ShuffleOperatorMetrics:
    """The phases of one operator, and the operator-level views built from them.

    Operators delegate to this object for managing and aggregating the metrics of
    their constituent phases.
    """

    def __init__(
        self,
        op: "PhysicalOperator",
        op_metrics: OpRuntimeMetrics,
        specs: Sequence[PhaseSpec],
    ):
        """Create one phase per spec, aliasing a lone phase to ``op_metrics``.

        Args:
            op: The owning operator.
            op_metrics: The operator's ``self._metrics``, passed explicitly
                because ``op.metrics`` calls ``_extra_metrics()``, which
                delegates back here.
            specs: One spec per phase, in progress bar order.
        """
        if not specs:
            raise ValueError("At least one phase is required")
        names = [spec.name for spec in specs]
        if len(set(names)) != len(names):
            raise ValueError(f"Phase names must be unique, got {names}")
        bar_names = [spec.progress_bar_name for spec in specs]
        if len(set(bar_names)) != len(bar_names):
            raise ValueError(f"Progress bar names must be unique, got {bar_names}")
        if sum(spec.is_output_phase for spec in specs) != 1:
            raise ValueError("Exactly one phase must be the output phase")

        self._op = op
        self._estimated_num_output_bundles: Optional[int] = None
        self._estimated_output_num_rows: Optional[int] = None

        self._aliased = len(specs) == 1
        self.phase_metrics: Dict[str, ShufflePhaseMetrics] = {
            spec.name: ShufflePhaseMetrics(
                spec,
                metrics=op_metrics if self._aliased else OpRuntimeMetrics(op),
                op_metrics=op_metrics,
                forward=not self._aliased and spec.is_output_phase,
            )
            for spec in specs
        }
        self._phase_metrics_by_bar_name = {
            pm.spec.progress_bar_name: pm for pm in self.phase_metrics.values()
        }

    @property
    def output_phase_metrics(self) -> ShufflePhaseMetrics:
        return next(pm for pm in self.phase_metrics.values() if pm.is_output_phase)

    def stats(self) -> StatsDict:
        # With several keys the stats builder prefixes each with the operator
        # name, so bare phase names render as ``<op>Shuffle`` / ``<op>Finalize``.
        return {pm.name: pm.block_stats for pm in self.phase_metrics.values()}

    def extra_metrics(self) -> Dict[str, Any]:
        if self._aliased:
            # The lone phase is the operator's own metrics; nesting it here
            # would put the operator's dict inside itself.
            (pm,) = self.phase_metrics.values()
            return dict(pm.custom)
        return {pm.name: pm.as_dict() for pm in self.phase_metrics.values()}

    def runtime_metrics_by_phase(self) -> Dict[str, OpRuntimeMetrics]:
        if self._aliased:
            return {}
        return {pm.name: pm.metrics for pm in self.phase_metrics.values()}

    def progress_bar_names(self) -> List[str]:
        return [pm.spec.progress_bar_name for pm in self.phase_metrics.values()]

    def set_progress_bar(self, name: str, bar: "BaseProgressBar") -> None:
        pm = self._phase_metrics_by_bar_name.get(name)
        if pm is not None:
            pm.set_progress_bar(bar)

    def update_output_estimates(
        self, phase_metrics: ShufflePhaseMetrics, num_tasks_submitted: int
    ) -> None:
        bundles, rows = phase_metrics.estimate_totals(
            num_tasks_submitted, self._op.upstream_op_num_outputs()
        )
        self._estimated_num_output_bundles = bundles
        self._estimated_output_num_rows = rows

    @property
    def estimated_num_output_bundles(self) -> Optional[int]:
        return self._estimated_num_output_bundles

    @property
    def estimated_output_num_rows(self) -> Optional[int]:
        return self._estimated_output_num_rows
