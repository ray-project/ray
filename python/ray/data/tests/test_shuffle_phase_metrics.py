from typing import List
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    RefBundle,
)
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    OpRuntimeMetrics,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    PhysicalOperator,
    TaskExecDriverStats,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.shuffle_phase_metrics import (
    PhaseSpec,
    ShuffleOperatorMetrics,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.stats import DatasetStats, _StatsManager
from ray.data.block import BlockExecStats, BlockMetadata, TaskExecWorkerStats
from ray.data.context import DataContext


@pytest.fixture(autouse=True, scope="module")
def _ray(ray_start_regular_shared):
    # Task hooks read the local node id from the runtime context.
    yield


SHUFFLE = PhaseSpec(name="shuffle", progress_bar_name="Shuffle")
FINALIZE = PhaseSpec(
    name="finalize",
    progress_bar_name="Reduce",
    is_output_phase=True,
    total_num_tasks=4,
)


def _make_op(name: str = "Op") -> MagicMock:
    op = MagicMock()
    op.name = name
    op.input_dependencies = []
    op.data_context.enable_get_object_locations_for_metrics = False
    op.data_context.enable_per_node_metrics = False
    op.current_logical_usage.return_value = ExecutionResources.zero()
    op.upstream_op_num_outputs.return_value = 8
    return op


def _make_bundle(num_rows: int = 10, size_bytes: int = 100) -> RefBundle:
    metadata = BlockMetadata(
        num_rows=num_rows,
        size_bytes=size_bytes,
        input_files=None,
        exec_stats=BlockExecStats(wall_time_s=0, block_ser_time_s=0),
    )
    return RefBundle(
        (
            BlockEntry(
                ray.ObjectRef.from_random(),  # pyrefly: ignore[bad-argument-type]
                metadata,
            ),
        ),
        owns_blocks=False,
        schema=None,
    )


def _finish(phase, task_index: int, wall_time_s: float = 1.0) -> None:
    phase.on_task_finished(
        task_index,
        None,
        TaskExecWorkerStats(task_wall_time_s=wall_time_s),
        TaskExecDriverStats(task_output_backpressure_s=0),
    )


def _make_two_phase():
    op = _make_op()
    op_metrics = OpRuntimeMetrics(op)
    som = ShuffleOperatorMetrics(op, op_metrics, [SHUFFLE, FINALIZE])
    return op, op_metrics, som


@pytest.mark.parametrize(
    "specs",
    [
        [],
        [SHUFFLE, PhaseSpec("shuffle", "Other", is_output_phase=True)],
        [SHUFFLE, PhaseSpec("finalize", "Shuffle", is_output_phase=True)],
        [SHUFFLE, PhaseSpec("finalize", "Reduce")],
        [FINALIZE, PhaseSpec("other", "Other", is_output_phase=True)],
    ],
    ids=["empty", "dup_name", "dup_bar_name", "no_output_phase", "two_output"],
)
def test_phase_spec_validation(specs: List[PhaseSpec]):
    op = _make_op()
    with pytest.raises(ValueError):
        ShuffleOperatorMetrics(op, OpRuntimeMetrics(op), specs)


def test_only_output_phase_forwards_task_events():
    _, op_metrics, som = _make_two_phase()
    shuffle, finalize = som.phase_metrics["shuffle"], som.phase_metrics["finalize"]

    shuffle.on_task_submitted(0, _make_bundle())
    assert shuffle.metrics.num_tasks_running == 1
    assert op_metrics.num_tasks_running == 0
    # Same local index on the output phase lands on the operator unchanged.
    finalize.on_task_submitted(0, _make_bundle())
    assert finalize.metrics.num_tasks_running == 1
    assert list(op_metrics._running_tasks) == [0]

    _finish(shuffle, 0)
    _finish(finalize, 0)
    assert shuffle.metrics.num_tasks_finished == 1
    assert finalize.metrics.num_tasks_finished == 1
    assert op_metrics.num_tasks_submitted == op_metrics.num_tasks_finished == 1
    # The hanging-task threshold is built from output-phase durations only.
    assert op_metrics.op_task_duration_stats.num_samples == 1
    assert op_metrics._running_tasks == {}
    assert shuffle.metrics._running_tasks == {}
    assert finalize.metrics._running_tasks == {}


def test_output_events_forward_only_from_output_phase():
    _, op_metrics, som = _make_two_phase()
    shuffle, finalize = som.phase_metrics["shuffle"], som.phase_metrics["finalize"]
    shuffle.on_task_submitted(0, _make_bundle())
    finalize.on_task_submitted(0, _make_bundle())

    # The shuffle phase's "output" is its own input re-wrapped: phase-local only.
    shuffle.on_task_output(0, _make_bundle(size_bytes=100))
    shuffle.on_output_queued(_make_bundle(size_bytes=100))
    assert shuffle.metrics.bytes_task_outputs_generated == 100
    assert shuffle.metrics.obj_store_mem_internal_outqueue_blocks == 1
    assert op_metrics.bytes_task_outputs_generated == 0
    assert op_metrics.obj_store_mem_internal_outqueue_blocks == 0

    # The output phase's outputs are the operator's outputs.
    out = _make_bundle(size_bytes=300)
    finalize.on_task_output(0, out)
    finalize.on_output_queued(out)
    assert finalize.metrics.bytes_task_outputs_generated == 300
    assert op_metrics.bytes_task_outputs_generated == 300
    assert op_metrics.obj_store_mem_internal_outqueue_blocks == 1

    finalize.on_output_dequeued(out)
    assert finalize.metrics.obj_store_mem_internal_outqueue_blocks == 0
    assert op_metrics.obj_store_mem_internal_outqueue_blocks == 0


def test_progress_bar_routing():
    _, _, som = _make_two_phase()
    shuffle, finalize = som.phase_metrics["shuffle"], som.phase_metrics["finalize"]
    assert som.progress_bar_names() == ["Shuffle", "Reduce"]

    # No bar attached yet: updates are dropped, not errors.
    shuffle.update_progress(increment=5)

    shuffle_bar, reduce_bar = MagicMock(), MagicMock()
    som.set_progress_bar("Shuffle", shuffle_bar)
    som.set_progress_bar("Reduce", reduce_bar)
    som.set_progress_bar("Unknown", MagicMock())

    shuffle.update_progress(increment=3, total=30)
    finalize.update_progress(total=7)
    shuffle_bar.update.assert_called_once_with(increment=3, total=30)
    reduce_bar.update.assert_called_once_with(increment=0, total=7)


def test_output_estimates_with_known_task_count_and_empty_inputs():
    """Finalize tasks carry an empty input bundle (v1 and GPU do this), which
    makes the shared helper's input-blocks guard return 0. A phase that knows
    its task count must estimate from outputs alone."""
    op, _, som = _make_two_phase()
    finalize = som.phase_metrics["finalize"]
    assert som.estimated_num_output_bundles is None
    assert som.estimated_output_num_rows is None

    # Two of four finalize tasks done, each producing one 10-row bundle.
    empty = RefBundle((), owns_blocks=False, schema=None)
    for i in range(2):
        finalize.on_task_submitted(i, empty)
        finalize.on_task_output(i, _make_bundle(num_rows=10))
        _finish(finalize, i)

    # The shared helper gives up on empty inputs...
    assert estimate_total_num_of_blocks(
        2, op.upstream_op_num_outputs(), finalize.metrics, total_num_tasks=4
    ) == (0, 0, 0)
    # ...the phase does not.
    som.update_output_estimates(finalize, num_tasks_submitted=2)
    assert som.estimated_num_output_bundles == 4
    assert som.estimated_output_num_rows == 40


def test_output_estimates_without_known_task_count_use_helper():
    op, _, som = _make_two_phase()
    shuffle = som.phase_metrics["shuffle"]  # total_num_tasks is None
    for i in range(2):
        shuffle.on_task_submitted(i, _make_bundle())
        shuffle.on_task_output(i, _make_bundle(num_rows=10))
        _finish(shuffle, i)
    _, expected_bundles, expected_rows = estimate_total_num_of_blocks(
        2, op.upstream_op_num_outputs(), shuffle.metrics
    )
    assert shuffle.estimate_totals(2, op.upstream_op_num_outputs()) == (
        expected_bundles,
        expected_rows,
    )
    assert expected_bundles == 8  # 8 upstream outputs / 1 input per task


def test_stats_manager_exports_phases_and_sums_nodes_from_phases(
    restore_data_context,
):
    ray.data.DataContext.get_current().enable_per_node_metrics = True
    _, op_metrics, som = _make_two_phase()
    # The operator mirrors the output phase; the shuffle phase is phase-only.
    som.phase_metrics["shuffle"].metrics._per_node_metrics[
        "node-a"
    ].num_tasks_finished = 2
    som.phase_metrics["finalize"].metrics._per_node_metrics[
        "node-a"
    ].num_tasks_finished = 1
    op_metrics._per_node_metrics["node-a"].num_tasks_finished = 1

    with patch("ray.data._internal.stats.get_or_create_stats_actor") as get_actor:
        _StatsManager.update_execution_metrics(
            "ds",
            [op_metrics],
            ["Op_0"],
            {},
            phase_metrics={"Op_0": som.runtime_metrics_by_phase()},
        )
        (args, _) = get_actor.return_value.update_execution_metrics.remote.call_args

    per_node = args[-1]
    assert per_node["node-a"]["num_tasks_finished"] == 3
    # Resource usage is read from the owning operator, so phase dicts omit it.
    _, dicts, tags, _, _ = args
    assert tags == ["Op_0", "Op_0/shuffle", "Op_0/finalize"]
    assert all(k in dicts[0] for k in ("cpu_usage", "gpu_usage", "memory_usage"))
    for phase_dict in dicts[1:]:
        assert not any(
            k in phase_dict for k in ("cpu_usage", "gpu_usage", "memory_usage")
        )


def test_executor_builds_phase_tags():
    plain = _make_op("Read")
    plain.get_phase_metrics.return_value = {}
    _, _, som = _make_two_phase()
    shuffle_op = _make_op("Shuffle(k)")
    shuffle_op.get_phase_metrics.return_value = som.runtime_metrics_by_phase()

    executor = StreamingExecutor.__new__(StreamingExecutor)
    executor._topology = {plain: MagicMock(), shuffle_op: MagicMock()}
    assert executor._get_phase_metrics() == {
        "Shuffle(k)_1": som.runtime_metrics_by_phase()
    }


def test_existing_operators_export_no_phase_entries():
    with patch("ray.data._internal.stats.get_or_create_stats_actor") as get_actor:
        ray.data.range(10).map_batches(lambda b: b).materialize()
        calls = get_actor.return_value.update_execution_metrics.remote.call_args_list

    assert calls
    for args, _ in calls:
        _, metric_dicts, tags, _, _ = args
        assert len(metric_dicts) == len(tags)
        assert not any("/" in tag for tag in tags)


class _PhasedOperator(PhysicalOperator):
    """Minimal operator wired to ShuffleOperatorMetrics the way a migrated
    operator is: one-line delegations, an output queue, no transport."""

    def __init__(
        self, name: str, specs: List[PhaseSpec], num_upstream_outputs: int = 8
    ):
        ctx = DataContext.get_current()
        # Output estimates need a non-zero upstream output count.
        upstream = InputDataBuffer(
            ctx, input_data=[_make_bundle() for _ in range(num_upstream_outputs)]
        )
        super().__init__(name=name, input_dependencies=[upstream], data_context=ctx)
        self._phase_metrics = ShuffleOperatorMetrics(self, self._metrics, specs)
        self._queue: List[RefBundle] = []

    # --- delegations a migrated operator carries ---
    def _extra_metrics(self):
        return self._phase_metrics.extra_metrics()

    def get_phase_metrics(self):
        return self._phase_metrics.runtime_metrics_by_phase()

    def get_stats(self):
        return self._phase_metrics.stats()

    def num_outputs_total(self):
        return self._phase_metrics.estimated_num_output_bundles

    def num_output_rows_total(self):
        return self._phase_metrics.estimated_output_num_rows

    def get_sub_progress_bar_names(self):
        return self._phase_metrics.progress_bar_names()

    def set_sub_progress_bar(self, name, pg):
        self._phase_metrics.set_progress_bar(name, pg)

    # --- PhysicalOperator plumbing ---
    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        pass

    def has_next(self) -> bool:
        return bool(self._queue)

    def _get_next_inner(self) -> RefBundle:
        bundle = self._queue.pop(0)
        out = self._phase_metrics.output_phase_metrics
        out.on_output_dequeued(bundle)
        out.record_block_stats(*bundle.metadata)
        return bundle

    # --- what a GPU-style operator does in its task callbacks ---
    def run_two_phase_workload(self, num_inputs: int, num_ranks: int, rows: int):
        shuffle, finalize = (
            self.phase_metrics["shuffle"],
            self.phase_metrics["finalize"],
        )
        for i in range(num_inputs):
            inp = _make_bundle(num_rows=rows, size_bytes=10)
            shuffle.on_task_submitted(i, inp)
            shuffle.record_block_stats(*inp.metadata)
            shuffle.on_task_output(i, inp)
            _finish(shuffle, i)
            shuffle.update_progress(increment=rows)
        for r in range(num_ranks):
            finalize.on_task_submitted(r, RefBundle((), owns_blocks=False, schema=None))
            out = _make_bundle(num_rows=rows * num_inputs // num_ranks, size_bytes=100)
            self._queue.append(out)
            finalize.on_task_output(r, out)
            finalize.on_output_queued(out)
            self._phase_metrics.update_output_estimates(finalize, r + 1)
            finalize.update_progress(
                increment=out.num_rows() or 0, total=self.num_output_rows_total()
            )
            _finish(finalize, r)

    @property
    def phase_metrics(self):
        return self._phase_metrics.phase_metrics


def test_two_phase_operator_surfaces():
    """Success criteria for a migrated two-phase operator, all four surfaces."""
    op = _PhasedOperator("Shuffle(k)", [SHUFFLE, FINALIZE])
    shuffle_bar, reduce_bar = MagicMock(), MagicMock()
    assert op.get_sub_progress_bar_names() == ["Shuffle", "Reduce"]
    op.set_sub_progress_bar("Shuffle", shuffle_bar)
    op.set_sub_progress_bar("Reduce", reduce_bar)

    op.run_two_phase_workload(num_inputs=8, num_ranks=4, rows=10)
    while op.has_next():
        op.get_next()

    # Prometheus, operator tag: the output phase's tasks and outputs only.
    d = op.metrics.as_dict()
    assert d["num_tasks_submitted"] == d["num_tasks_finished"] == 4
    assert d["num_tasks_running"] == 0
    assert d["row_outputs_taken"] == 80
    assert d["bytes_task_outputs_generated"] == 4 * 100  # finalize only
    assert op.metrics._running_tasks == {}

    # Prometheus, phase tags.
    phases = op.get_phase_metrics()
    assert list(phases) == ["shuffle", "finalize"]
    assert phases["shuffle"].num_tasks_finished == 8
    assert phases["finalize"].num_tasks_finished == 4
    assert phases["shuffle"]._running_tasks == {}
    assert phases["finalize"]._running_tasks == {}
    # Takes are mirrored on the output phase only (ray_data_output_rows).
    assert phases["finalize"].row_outputs_taken == 80
    assert phases["shuffle"].row_outputs_taken == 0

    # Extra metrics line: one nested dict per phase, same counts as the tags,
    # plus the phase's custom counters.
    op.phase_metrics["finalize"].custom["num_partitions"] = 4
    d = op.metrics.as_dict()
    assert d["shuffle"]["num_tasks_finished"] == 8
    assert d["finalize"]["num_tasks_finished"] == 4
    assert d["finalize"]["num_partitions"] == 4
    assert "cpu_usage" in d and "cpu_usage" not in d["finalize"]
    assert op._phase_metrics.output_phase_metrics is op.phase_metrics["finalize"]

    # ds.stats() table: two rows named <op>Shuffle / <op>Finalize.
    stats = (
        DatasetStats(metadata={}, parent=None)
        .child_builder("Shuffle(k)")
        .build_multioperator(op.get_stats())
    )
    assert list(stats.metadata) == ["Shuffle(k)Shuffle", "Shuffle(k)Finalize"]
    assert len(stats.metadata["Shuffle(k)Shuffle"]) == 8
    assert len(stats.metadata["Shuffle(k)Finalize"]) == 4

    # Progress bars: shuffle increments sum to input rows; finalize total
    # tracks the output estimate.
    assert sum(c.kwargs["increment"] for c in shuffle_bar.update.call_args_list) == 80
    assert (
        reduce_bar.update.call_args_list[-1].kwargs["total"]
        == op.num_output_rows_total()
    )
    assert op.num_outputs_total() == 4
    assert op.num_output_rows_total() == 80

    # Export path: three dicts, three tags, phase dicts carry phase counts.
    with patch("ray.data._internal.stats.get_or_create_stats_actor") as get_actor:
        _StatsManager.update_execution_metrics(
            "ds",
            [op.metrics],
            ["Shuffle(k)_1"],
            {},
            phase_metrics={"Shuffle(k)_1": phases},
        )
        (args, _) = get_actor.return_value.update_execution_metrics.remote.call_args
    _, dicts, tags, _, _ = args
    assert tags == ["Shuffle(k)_1", "Shuffle(k)_1/shuffle", "Shuffle(k)_1/finalize"]
    assert [x["num_tasks_finished"] for x in dicts] == [4, 8, 4]


def test_single_phase_operator_surfaces_unchanged():
    """A v2-style operator keeps today's shapes: one stats row named after the
    operator, no phase tags, no nested copy of itself in extra metrics."""
    op = _PhasedOperator(
        "Reduce", [PhaseSpec("reduce", "Reduce", is_output_phase=True)]
    )
    (pm,) = op.phase_metrics.values()
    assert pm.metrics is op.metrics  # aliased, nothing is forwarded
    pm.custom["num_partitions"] = 4
    pm.on_task_submitted(0, _make_bundle())
    out = _make_bundle(num_rows=5)
    op._queue.append(out)
    pm.on_task_output(0, out)
    pm.on_output_queued(out)
    _finish(pm, 0)
    op.get_next()

    d = op.metrics.as_dict()
    assert d["num_tasks_finished"] == 1 and d["row_outputs_taken"] == 5
    assert "reduce" not in d and d["num_partitions"] == 4
    assert op.get_phase_metrics() == {}
    stats = (
        DatasetStats(metadata={}, parent=None)
        .child_builder("Reduce")
        .build_multioperator(op.get_stats())
    )
    assert list(stats.metadata) == ["Reduce"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
