"""Per-phase metrics bookkeeping shared by all shuffle-family operators.

A shuffle-family operator (GPU shuffle and GPU aggregate, the shuffle v2
map/reduce/sampling operators) runs one or more internal *phases*, each with
its own task lifecycle, progress bar and block stats. ``ShuffleOperatorMetrics``
owns one ``ShufflePhaseMetrics`` per phase and derives every operator-facing
view from them: ``get_stats()``, ``_extra_metrics()``, ``get_phase_metrics()``,
the sub-progress bar wiring and the output estimates.

Operators record events on a phase at the same call sites where they would
otherwise touch an ``OpRuntimeMetrics`` directly. The phase records the event
on its own metrics and, where it belongs to the operator as a whole, forwards
it to the operator's base metrics so the operator-level series stays complete.
"""

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
        name: Key used in stats, extra metrics and Prometheus tags
            (e.g. ``"shuffle"``, ``"finalize"``).
        progress_bar_name: Display name of the phase's sub-progress bar.
        is_output_phase: Whether this phase's outputs are the operator's
            outputs. Exactly one phase per operator must be the output phase;
            only its output events are forwarded to the operator-level metrics.
        total_num_tasks: Total number of tasks the phase will run, if known
            upfront (e.g. the number of partitions). Used for output estimates.
    """

    name: str
    progress_bar_name: str
    is_output_phase: bool = False
    total_num_tasks: Optional[int] = None


class ShufflePhaseMetrics:
    """Metrics, block stats and progress bar for one phase of an operator.

    Task lifecycle events (``on_task_submitted`` / ``on_task_finished``) are
    always forwarded to the operator-level metrics. Phases number their tasks
    independently (shuffle task 0 and partition 0 both exist), so the
    forwarded key is ``local_index * num_phases + phase_ordinal``, which is
    unique across phases without any shared state. Output events are forwarded
    only from the output phase. When a phase is aliased to the operator's own
    metrics object (single-phase operators) nothing is forwarded.
    """

    def __init__(
        self,
        spec: PhaseSpec,
        metrics: OpRuntimeMetrics,
        op_metrics: OpRuntimeMetrics,
        phase_ordinal: int,
        num_phases: int,
    ):
        self.spec = spec
        self.metrics = metrics
        self.block_stats: List[BlockStats] = []
        # Shuffle-specific counters surfaced next to the OpRuntimeMetrics fields.
        self.custom: Dict[str, Union[int, float]] = {}
        self.total_num_tasks = spec.total_num_tasks

        self._op_metrics = op_metrics
        self._forward = metrics is not op_metrics
        self._phase_ordinal = phase_ordinal
        self._num_phases = num_phases
        self._bar: Optional["BaseProgressBar"] = None

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def is_output_phase(self) -> bool:
        return self.spec.is_output_phase

    def _op_task_index(self, task_index: int) -> int:
        return task_index * self._num_phases + self._phase_ordinal

    # ------------------------------------------------------------------
    # Task lifecycle
    # ------------------------------------------------------------------

    def on_task_submitted(
        self,
        task_index: int,
        inputs: RefBundle,
        task_id: Optional["ray.TaskID"] = None,
    ) -> None:
        self.metrics.on_task_submitted(task_index, inputs, task_id=task_id)
        if self._forward:
            self._op_metrics.on_task_submitted(
                self._op_task_index(task_index), inputs, task_id=task_id
            )

    def on_task_output(self, task_index: int, output: RefBundle) -> None:
        self.metrics.on_task_output_generated(task_index, output)
        if self._forward and self.is_output_phase:
            self._op_metrics.on_task_output_generated(
                self._op_task_index(task_index), output
            )

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
                self._op_task_index(task_index),
                exception,
                task_exec_stats=task_exec_stats,
                task_exec_driver_stats=task_exec_driver_stats,
            )

    # ------------------------------------------------------------------
    # Output queue
    # ------------------------------------------------------------------

    def on_output_queued(self, output: RefBundle) -> None:
        self.metrics.on_output_queued(output)
        if self._forward and self.is_output_phase:
            self._op_metrics.on_output_queued(output)

    def on_output_dequeued(self, output: RefBundle) -> None:
        self.metrics.on_output_dequeued(output)
        if self._forward and self.is_output_phase:
            self._op_metrics.on_output_dequeued(output)

    # ------------------------------------------------------------------
    # Stats, progress and estimates
    # ------------------------------------------------------------------

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
        """Estimate this phase's total output bundles and rows.

        When the phase knows its task count, the estimate is
        ``total tasks * average outputs per finished task``. This bypasses
        ``estimate_total_num_of_blocks``, whose guard needs a non-zero average
        number of *input* blocks per task; finalize tasks are submitted with an
        empty input bundle, so that average is 0 and the helper would return 0.
        """
        if self.total_num_tasks is not None:
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
        return {**self.metrics.as_dict(), **self.custom}


class ShuffleOperatorMetrics:
    """The per-operator collection of phases.

    Holds one ``ShufflePhaseMetrics`` per ``PhaseSpec`` and computes every
    operator-facing view from them on demand, so there is a single copy of each
    fact. Operators delegate their ``PhysicalOperator`` methods to this object:

    - ``get_stats()``            -> ``stats()``
    - ``_extra_metrics()``       -> ``extra_metrics()``
    - ``get_phase_metrics()``    -> ``phase_metrics()``
    - ``num_outputs_total()``    -> ``estimated_num_output_bundles``
    - ``num_output_rows_total()``-> ``estimated_output_num_rows``
    - ``get_sub_progress_bar_names()`` / ``set_sub_progress_bar()``
                                 -> ``progress_bar_names()`` / ``set_progress_bar()``

    A single-phase operator's phase is aliased to the operator's own metrics
    object, so its stats keep the plain operator name and nothing is exported a
    second time under a phase tag.
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
            op_metrics: The operator's own metrics object (``self._metrics``).
                Passed explicitly rather than read via ``op.metrics`` because
                that property calls ``op._extra_metrics()``, which delegates
                back here before construction has finished.
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
        self.phases: Dict[str, ShufflePhaseMetrics] = {}
        for ordinal, spec in enumerate(specs):
            self.phases[spec.name] = ShufflePhaseMetrics(
                spec,
                metrics=op_metrics if self._aliased else OpRuntimeMetrics(op),
                op_metrics=op_metrics,
                phase_ordinal=ordinal,
                num_phases=len(specs),
            )
        self._phases_by_bar_name = {
            phase.spec.progress_bar_name: phase for phase in self.phases.values()
        }

    @property
    def output_phase(self) -> ShufflePhaseMetrics:
        return next(p for p in self.phases.values() if p.is_output_phase)

    # ------------------------------------------------------------------
    # Operator-facing views
    # ------------------------------------------------------------------

    def stats(self) -> StatsDict:
        """Block stats keyed by phase name.

        ``DatasetStatsBuilder.build_multioperator`` prefixes each key with the
        operator name when there is more than one, and ignores the key when
        there is exactly one, so bare phase names render as
        ``<op name>Shuffle`` / ``<op name>Finalize``.
        """
        return {phase.name: phase.block_stats for phase in self.phases.values()}

    def extra_metrics(self) -> Dict[str, Any]:
        if self._aliased:
            # The phase *is* the operator's metrics; nesting it here would nest
            # the operator's own dict inside itself on every read.
            (phase,) = self.phases.values()
            return dict(phase.custom)
        return {phase.name: phase.as_dict() for phase in self.phases.values()}

    def phase_metrics(self) -> Dict[str, OpRuntimeMetrics]:
        if self._aliased:
            return {}
        return {phase.name: phase.metrics for phase in self.phases.values()}

    def progress_bar_names(self) -> List[str]:
        return [phase.spec.progress_bar_name for phase in self.phases.values()]

    def set_progress_bar(self, name: str, bar: "BaseProgressBar") -> None:
        phase = self._phases_by_bar_name.get(name)
        if phase is not None:
            phase.set_progress_bar(bar)

    # ------------------------------------------------------------------
    # Output estimates
    # ------------------------------------------------------------------

    def update_output_estimates(
        self, phase: ShufflePhaseMetrics, num_tasks_submitted: int
    ) -> None:
        """Re-estimate the operator's total output from ``phase``'s progress.

        Replaces the inline ``estimate_total_num_of_blocks`` block that each
        shuffle operator used to carry.
        """
        bundles, rows = phase.estimate_totals(
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
