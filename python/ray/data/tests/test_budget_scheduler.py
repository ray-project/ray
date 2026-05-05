"""Tests for the BudgetScheduler.

Tests are organized by lifecycle phase:
1. Initialization and registration
2. Operator selection logic
3. Task dispatch and budget accounting
4. Task output, peak tracking, and backpressure
5. Task completion and EMA updates
6. End-to-end pipeline simulations
"""

import importlib.util
import os
import sys
from unittest.mock import MagicMock

import pytest

# Import budget_scheduler directly by file path to avoid pulling in the
# full ray package (which requires a compiled _raylet extension).
_mod_path = os.environ.get(
    "BUDGET_SCHEDULER_PATH",
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.pardir,
        "_internal",
        "execution",
        "budget_scheduler.py",
    ),
)
_spec = importlib.util.spec_from_file_location(
    "budget_scheduler", os.path.normpath(_mod_path)
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

BudgetScheduler = _mod.BudgetScheduler
OpSchedulingState = _mod.OpSchedulingState
TaskState = _mod.TaskState


def make_mock_op(name="op", input_deps=None, cpu_per_task=1.0):
    """Create a mock PhysicalOperator."""
    op = MagicMock()
    op.name = name
    op.input_dependencies = input_deps or []
    resources = MagicMock()
    resources.cpu = cpu_per_task
    op.per_task_resource_allocation.return_value = resources
    return op


def make_linear_pipeline(n=3, names=None):
    """Create a linear pipeline of n mock operators: op0 -> op1 -> ... -> opN-1.

    Returns list of ops from source to sink.
    """
    names = names or [f"op{i}" for i in range(n)]
    ops = []
    for i in range(n):
        deps = [ops[i - 1]] if i > 0 else []
        ops.append(make_mock_op(name=names[i], input_deps=deps))
    return ops


def make_scheduler(max_bytes=1000, available_cpus=4, ema_alpha=0.3):
    """Create a BudgetScheduler with defaults suitable for testing."""
    return BudgetScheduler(
        max_bytes=max_bytes,
        available_cpus=available_cpus,
        ema_alpha=ema_alpha,
    )


# ---------------------------------------------------------------------------
# 1. Initialization and registration
# ---------------------------------------------------------------------------


class TestInitialization:
    def test_initial_state(self):
        s = make_scheduler(max_bytes=500, available_cpus=2)
        assert s.max_bytes == 500
        assert s.available_cpus == 2
        assert s.allocated_bytes == 0
        assert s.task_states == {}
        assert s.op_states == {}

    def test_register_operator_default_peak(self):
        s = make_scheduler()
        op = make_mock_op()
        s.register_operator(op)
        assert s.op_states[op].expected_peak_bytes == 0.0
        assert s.op_states[op].buffered_output_bytes == 0

    def test_register_operator_custom_peak(self):
        s = make_scheduler()
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=256.0)
        assert s.op_states[op].expected_peak_bytes == 256.0

    def test_register_multiple_operators(self):
        s = make_scheduler()
        ops = make_linear_pipeline(3)
        for op in ops:
            s.register_operator(op)
        assert len(s.op_states) == 3


# ---------------------------------------------------------------------------
# 2. Operator selection logic
# ---------------------------------------------------------------------------


class TestOperatorSelection:
    def test_select_from_single_op(self):
        s = make_scheduler(max_bytes=1000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        assert s.select_operator({op: 100}) is op

    def test_select_none_when_empty(self):
        s = make_scheduler()
        assert s.select_operator({}) is None

    def test_select_none_when_no_budget(self):
        """No op is dispatchable if adding expected peak exceeds budget."""
        s = make_scheduler(max_bytes=100, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=200.0)
        # 0 + 200 > 100 -> rejected
        assert s.select_operator({op: 100}) is None

    def test_select_none_when_allocated_full(self):
        """No op is dispatchable if already at budget."""
        s = make_scheduler(max_bytes=100, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=50.0)
        s.allocated_bytes = 60
        # 60 + 50 > 100 -> rejected
        assert s.select_operator({op: 100}) is None

    def test_select_allowed_when_peak_fits(self):
        """Op is dispatchable if expected peak fits within remaining budget."""
        s = make_scheduler(max_bytes=100, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=50.0)
        s.allocated_bytes = 40
        # 40 + 50 <= 100 -> OK
        assert s.select_operator({op: 100}) is op

    def test_select_none_when_no_cpus(self):
        s = make_scheduler(max_bytes=1000, available_cpus=0)
        op = make_mock_op()
        s.register_operator(op)
        assert s.select_operator({op: 100}) is None

    def test_select_prefers_fewer_buffered_bytes(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op_a = make_mock_op("a")
        op_b = make_mock_op("b")
        s.register_operator(op_a)
        s.register_operator(op_b)
        s.op_states[op_a].buffered_output_bytes = 500
        s.op_states[op_b].buffered_output_bytes = 100
        assert s.select_operator({op_a: 100, op_b: 100}) is op_b

    def test_select_tiebreak_favors_downstream(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        ops = make_linear_pipeline(3, names=["source", "map", "sink"])
        for op in ops:
            s.register_operator(op)
        # All have 0 buffered bytes (tied).
        assert s.select_operator({op: 100 for op in ops}) is ops[2]

    def test_select_skips_op_that_exceeds_budget(self):
        """Op whose expected peak exceeds remaining budget is skipped."""
        s = make_scheduler(max_bytes=500, available_cpus=4)
        op_cheap = make_mock_op("cheap")
        op_expensive = make_mock_op("expensive")
        s.register_operator(op_cheap, initial_peak_bytes=100.0)
        s.register_operator(op_expensive, initial_peak_bytes=600.0)
        s.allocated_bytes = 0
        # cheap: 0 + 100 <= 500 -> OK
        # expensive: 0 + 600 > 500 -> rejected
        assert s.select_operator({op_cheap: 100, op_expensive: 100}) is op_cheap

    def test_select_skips_unregistered_op(self):
        s = make_scheduler(max_bytes=1000, available_cpus=4)
        op = make_mock_op()
        assert s.select_operator({op: 100}) is None


# ---------------------------------------------------------------------------
# 3. Task dispatch and budget accounting
# ---------------------------------------------------------------------------


class TestTaskDispatch:
    def test_dispatch_basic(self):
        s = make_scheduler(max_bytes=1000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=150.0)

        tid = s.dispatch_task(op, input_bytes=100, num_cpus=1.0)

        assert tid == 0
        task = s.task_states[tid]
        assert task.op is op
        assert task.input_bytes == 100
        assert task.expected_peak_bytes == 150
        assert task.produced_bytes == 0
        assert task.consumed_bytes == 0
        assert task.peak_concurrent_bytes == 0
        assert s.allocated_bytes == 150
        assert s.available_cpus == 3.0

    def test_dispatch_zero_peak(self):
        """Op with 0 expected peak allocates nothing."""
        s = make_scheduler(max_bytes=1000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=0.0)

        s.dispatch_task(op, input_bytes=100)
        assert s.allocated_bytes == 0

    def test_dispatch_multiple_tasks_increments_ids(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)

        tid1 = s.dispatch_task(op, input_bytes=100)
        tid2 = s.dispatch_task(op, input_bytes=100)
        tid3 = s.dispatch_task(op, input_bytes=100)

        assert (tid1, tid2, tid3) == (0, 1, 2)
        assert len(s.task_states) == 3
        assert s.allocated_bytes == 300  # 3 * 100

    def test_dispatch_decrements_cpus(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op(cpu_per_task=2.0)
        s.register_operator(op)

        s.dispatch_task(op, input_bytes=100, num_cpus=2.0)
        assert s.available_cpus == 2.0
        s.dispatch_task(op, input_bytes=100, num_cpus=2.0)
        assert s.available_cpus == 0.0

    def test_dispatch_across_operators(self):
        s = make_scheduler(max_bytes=1000, available_cpus=4)
        op_a = make_mock_op("a")
        op_b = make_mock_op("b")
        s.register_operator(op_a, initial_peak_bytes=200.0)
        s.register_operator(op_b, initial_peak_bytes=50.0)

        s.dispatch_task(op_a, input_bytes=100)
        s.dispatch_task(op_b, input_bytes=100)

        assert s.allocated_bytes == 250  # 200 + 50
        assert s.available_cpus == 2.0

    def test_dispatch_consumes_input_op_buffered_output(self):
        """Dispatching a task consumes buffered output from input operators."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=100.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        # Source produces output.
        tid0 = s.dispatch_task(source, input_bytes=0)
        s.on_task_output(tid0, output_bytes=200)
        assert s.op_states[source].buffered_output_bytes == 200

        # Dispatching mapper consumes from source.
        s.dispatch_task(mapper, input_bytes=200)
        assert s.op_states[source].buffered_output_bytes == 0

    def test_dispatch_consumes_does_not_go_negative(self):
        """Consuming more than buffered clamps to 0."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=100.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        # Source has 50 buffered but mapper claims 100.
        s.op_states[source].buffered_output_bytes = 50
        s.dispatch_task(mapper, input_bytes=100)
        assert s.op_states[source].buffered_output_bytes == 0

    def test_dispatch_updates_producing_task_consumed_bytes(self):
        """Dispatching with input_task_ids updates consumed_bytes on producers."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=200.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        tid0 = s.dispatch_task(source, input_bytes=0)
        s.on_task_output(tid0, output_bytes=200)

        # Dispatch mapper, indicating it consumes output from tid0.
        s.dispatch_task(mapper, input_bytes=200, input_task_ids=[tid0])
        assert s.task_states[tid0].consumed_bytes == 200
        assert s.task_states[tid0].concurrent_bytes == 0  # 200 - 200

    def test_dispatch_updates_multiple_producing_tasks(self):
        """Input bytes are split across multiple producing tasks."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=100.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        tid0 = s.dispatch_task(source, input_bytes=0)
        tid1 = s.dispatch_task(source, input_bytes=0)
        s.on_task_output(tid0, output_bytes=100)
        s.on_task_output(tid1, output_bytes=100)

        # Dispatch mapper consuming 200 bytes from both tasks.
        s.dispatch_task(mapper, input_bytes=200, input_task_ids=[tid0, tid1])
        assert s.task_states[tid0].consumed_bytes == 100
        assert s.task_states[tid1].consumed_bytes == 100

    def test_dispatch_skips_finished_producing_tasks(self):
        """If a producing task has already finished, it's not in task_states."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=100.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        tid0 = s.dispatch_task(source, input_bytes=0)
        s.on_task_output(tid0, output_bytes=100)
        s.on_task_finished(tid0)

        # Dispatching mapper with a finished task_id is a no-op for that task.
        s.dispatch_task(mapper, input_bytes=100, input_task_ids=[tid0])
        # No error, source buffered output still consumed.
        assert s.op_states[source].buffered_output_bytes == 0

    def test_dispatch_no_input_deps_no_consumption(self):
        """Source operator with no input_dependencies doesn't consume anything."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op("source", input_deps=[])
        s.register_operator(op, initial_peak_bytes=100.0)

        # Even with input_bytes > 0, no crash if no input_dependencies.
        s.dispatch_task(op, input_bytes=100)
        # No error


# ---------------------------------------------------------------------------
# 4. Task output, peak tracking, and backpressure
# ---------------------------------------------------------------------------


class TestTaskOutput:
    def test_output_updates_produced_and_peak(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=200.0)
        tid = s.dispatch_task(op, input_bytes=100)

        s.on_task_output(tid, output_bytes=80)
        task = s.task_states[tid]
        assert task.produced_bytes == 80
        assert task.peak_concurrent_bytes == 80

        s.on_task_output(tid, output_bytes=50)
        assert task.produced_bytes == 130
        assert task.peak_concurrent_bytes == 130  # new peak

    def test_output_within_expected_peak(self):
        """No extra charge when peak stays within expected."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=200.0)
        tid = s.dispatch_task(op, input_bytes=100)
        assert s.allocated_bytes == 200

        s.on_task_output(tid, output_bytes=150)
        assert s.allocated_bytes == 200  # No extra charge

    def test_output_exceeds_expected_peak(self):
        """Excess beyond expected peak is charged to allocated_bytes."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)
        assert s.allocated_bytes == 100

        # Produce 150 bytes with nothing consumed -> peak = 150.
        s.on_task_output(tid, output_bytes=150)
        # Excess = 150 - 100 = 50
        assert s.allocated_bytes == 150

    def test_output_excess_incremental(self):
        """Excess is charged incrementally as peak grows."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)

        s.on_task_output(tid, output_bytes=120)  # peak=120, excess=20
        assert s.allocated_bytes == 120

        s.on_task_output(tid, output_bytes=80)  # peak=200, excess=100
        assert s.allocated_bytes == 200

    def test_consumption_at_dispatch_prevents_peak_growth(self):
        """If downstream dispatches (consuming output) between outputs, peak doesn't grow."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=200.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        tid0 = s.dispatch_task(source, input_bytes=0)
        s.on_task_output(tid0, output_bytes=100)
        assert s.task_states[tid0].peak_concurrent_bytes == 100

        # Downstream dispatch consumes 80 bytes from tid0.
        s.dispatch_task(mapper, input_bytes=80, input_task_ids=[tid0])
        # concurrent = 100 - 80 = 20

        s.on_task_output(tid0, output_bytes=50)
        # concurrent = 100 + 50 - 80 = 70, peak stays at 100
        assert s.task_states[tid0].peak_concurrent_bytes == 100

    def test_output_updates_buffered_output(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        tid = s.dispatch_task(op, input_bytes=100)

        s.on_task_output(tid, output_bytes=30)
        s.on_task_output(tid, output_bytes=50)
        assert s.op_states[op].buffered_output_bytes == 80

    def test_backpressure_triggered(self):
        """Backpressure when budget exceeded and peak >= expected."""
        s = make_scheduler(max_bytes=200, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)
        # allocated = 100

        # Force budget to be full.
        s.allocated_bytes = 200
        # Produce output so peak (100) >= expected (100).
        s.on_task_output(tid, output_bytes=100)
        bp = s.on_task_output(tid, output_bytes=50)
        assert bp is True
        assert s.task_states[tid].backpressured is True

    def test_no_backpressure_under_budget(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)

        s.on_task_output(tid, output_bytes=500)
        bp = s.on_task_output(tid, output_bytes=500)
        assert bp is False

    def test_no_backpressure_within_expected_peak(self):
        """No backpressure if peak < expected, even when at budget."""
        s = make_scheduler(max_bytes=200, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=200.0)
        tid = s.dispatch_task(op, input_bytes=50)
        # allocated = 200 (at budget)

        bp = s.on_task_output(tid, output_bytes=100)
        # peak=100 < expected=200 -> no backpressure
        assert bp is False

    def test_output_for_unknown_task(self):
        s = make_scheduler()
        assert s.on_task_output(999, output_bytes=100) is False


# ---------------------------------------------------------------------------
# 5. Task completion and EMA updates
# ---------------------------------------------------------------------------


class TestTaskCompletion:
    def test_completion_updates_ema(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4, ema_alpha=0.5)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)

        # Produce 300 bytes, nothing consumed -> peak = 300.
        s.on_task_output(tid, output_bytes=300)
        s.on_task_finished(tid)

        # EMA: 0.5 * 300 + 0.5 * 100 = 200
        assert s.op_states[op].expected_peak_bytes == pytest.approx(200.0)

    def test_completion_ema_with_consumption(self):
        """Peak reflects consumption that happened via dispatch."""
        s = make_scheduler(max_bytes=10000, available_cpus=4, ema_alpha=0.5)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=100.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        tid0 = s.dispatch_task(source, input_bytes=0)
        # Produce 200, then downstream dispatch consumes 150, then produce 100.
        s.on_task_output(tid0, output_bytes=200)  # peak=200
        s.dispatch_task(mapper, input_bytes=150, input_task_ids=[tid0])  # consumed=150
        s.on_task_output(
            tid0, output_bytes=100
        )  # concurrent=200+100-150=150, peak still 200
        s.on_task_finished(tid0)

        # EMA: 0.5 * 200 + 0.5 * 100 = 150
        assert s.op_states[source].expected_peak_bytes == pytest.approx(150.0)

    def test_completion_removes_allocation(self):
        """Task completion removes the charged amount from allocated_bytes."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)
        assert s.allocated_bytes == 100

        # Peak stays within expected.
        s.on_task_output(tid, output_bytes=80)
        assert s.allocated_bytes == 100  # No excess
        s.on_task_finished(tid)
        # Charged = max(expected=100, actual_peak=80) = 100
        assert s.allocated_bytes == 0

    def test_completion_removes_excess_allocation(self):
        """When peak exceeded expected, the full charged amount is removed."""
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)
        tid = s.dispatch_task(op, input_bytes=50)
        assert s.allocated_bytes == 100

        s.on_task_output(tid, output_bytes=250)  # peak=250, excess=150
        assert s.allocated_bytes == 250  # 100 + 150
        s.on_task_finished(tid)
        # Charged = max(100, 250) = 250
        assert s.allocated_bytes == 0

    def test_completion_releases_cpus(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        tid = s.dispatch_task(op, input_bytes=100, num_cpus=2.0)
        assert s.available_cpus == 2.0
        s.on_task_finished(tid)
        assert s.available_cpus == 4.0

    def test_completion_removes_task_state(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        tid = s.dispatch_task(op, input_bytes=100)
        assert tid in s.task_states
        s.on_task_finished(tid)
        assert tid not in s.task_states

    def test_completion_unknown_task_is_noop(self):
        s = make_scheduler()
        s.on_task_finished(999)

    def test_ema_converges_over_multiple_tasks(self):
        """EMA should converge toward the actual peak over many tasks."""
        s = make_scheduler(max_bytes=100000, available_cpus=100, ema_alpha=0.3)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=50.0)

        # 20 tasks each with actual peak = 300.
        for _ in range(20):
            tid = s.dispatch_task(op, input_bytes=100)
            s.on_task_output(tid, output_bytes=300)
            s.on_task_finished(tid)

        assert s.op_states[op].expected_peak_bytes == pytest.approx(300.0, abs=1.0)

    def test_ema_converges_with_consumption(self):
        """EMA converges when tasks have concurrent consumption via dispatch."""
        s = make_scheduler(max_bytes=100000, available_cpus=100, ema_alpha=0.3)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=500.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        # 20 tasks: produce 200, downstream dispatches consuming 150, produce 100 -> peak=200.
        for _ in range(20):
            tid = s.dispatch_task(source, input_bytes=0)
            s.on_task_output(tid, output_bytes=200)
            s.dispatch_task(mapper, input_bytes=150, input_task_ids=[tid])
            s.on_task_output(
                tid, output_bytes=100
            )  # concurrent=200+100-150=150, peak=200
            s.on_task_finished(tid)

        assert s.op_states[source].expected_peak_bytes == pytest.approx(200.0, abs=1.0)


# ---------------------------------------------------------------------------
# 6. Backpressure queries
# ---------------------------------------------------------------------------


class TestBackpressure:
    def test_is_backpressured_false_when_no_tasks(self):
        s = make_scheduler()
        op = make_mock_op()
        s.register_operator(op)
        assert s.is_backpressured(op) is False

    def test_is_backpressured_false_when_some_tasks_ok(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        tid1 = s.dispatch_task(op, input_bytes=100)
        _ = s.dispatch_task(op, input_bytes=100)
        s.task_states[tid1].backpressured = True
        assert s.is_backpressured(op) is False

    def test_is_backpressured_true_when_all_tasks_bp(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        tid1 = s.dispatch_task(op, input_bytes=100)
        tid2 = s.dispatch_task(op, input_bytes=100)
        s.task_states[tid1].backpressured = True
        s.task_states[tid2].backpressured = True
        assert s.is_backpressured(op) is True

    def test_get_backpressured_tasks(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op)
        tid1 = s.dispatch_task(op, input_bytes=100)
        _ = s.dispatch_task(op, input_bytes=100)
        tid3 = s.dispatch_task(op, input_bytes=100)
        s.task_states[tid1].backpressured = True
        s.task_states[tid3].backpressured = True
        assert set(s.get_backpressured_tasks(op)) == {tid1, tid3}


# ---------------------------------------------------------------------------
# 7. End-to-end pipeline simulations
# ---------------------------------------------------------------------------


class TestPipelineSimulation:
    def test_simple_passthrough_pipeline(self):
        """source -> map -> sink, peak-based budget returns to 0."""
        s = make_scheduler(max_bytes=5000, available_cpus=2)
        source, mapper, sink = make_linear_pipeline(3, ["source", "map", "sink"])
        for op in [source, mapper, sink]:
            s.register_operator(op, initial_peak_bytes=200.0)

        # Source produces data.
        tid0 = s.dispatch_task(source, input_bytes=0, num_cpus=1.0)
        s.on_task_output(tid0, output_bytes=100)
        s.on_task_finished(tid0)

        # Map consumes source output (dispatch consumes from source).
        tid1 = s.dispatch_task(
            mapper, input_bytes=100, num_cpus=1.0, input_task_ids=[tid0]
        )
        assert s.op_states[source].buffered_output_bytes == 0
        s.on_task_output(tid1, output_bytes=100)
        s.on_task_finished(tid1)

        # Sink consumes map output (dispatch consumes from mapper).
        tid2 = s.dispatch_task(
            sink, input_bytes=100, num_cpus=1.0, input_task_ids=[tid1]
        )
        assert s.op_states[mapper].buffered_output_bytes == 0
        s.on_task_output(tid2, output_bytes=100)
        s.on_task_finished(tid2)

        assert s.allocated_bytes == 0
        assert s.available_cpus == 2.0
        assert len(s.task_states) == 0

    def test_pipelined_consumption_reduces_peak(self):
        """Downstream dispatching during execution keeps peak low."""
        s = make_scheduler(max_bytes=10000, available_cpus=10, ema_alpha=1.0)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=500.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        tid = s.dispatch_task(source, input_bytes=0)

        # Produce 5 blocks of 100, with downstream dispatching between each.
        # Without consumption: peak would be 500.
        # With consumption: peak stays at 100.
        for i in range(5):
            s.on_task_output(tid, output_bytes=100)
            if i < 4:
                s.dispatch_task(mapper, input_bytes=100, input_task_ids=[tid])

        assert s.task_states[tid].peak_concurrent_bytes == 100
        s.on_task_finished(tid)
        # EMA alpha=1.0 -> expected_peak = actual peak = 100
        assert s.op_states[source].expected_peak_bytes == pytest.approx(100.0)

    def test_fan_out_triggers_backpressure(self):
        """High expansion with no consumption triggers backpressure."""
        s = make_scheduler(max_bytes=300, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)

        tid = s.dispatch_task(op, input_bytes=50)  # allocated = 100

        s.on_task_output(tid, output_bytes=100)  # peak=100, at expected
        s.on_task_output(tid, output_bytes=100)  # peak=200, excess=100, allocated=200
        s.on_task_output(tid, output_bytes=100)  # peak=300, excess=200, allocated=300
        bp = s.on_task_output(tid, output_bytes=50)  # peak=350, allocated=350 >= 300
        assert bp is True

    def test_ema_adapts_and_tightens_budget(self):
        """After observing low peaks (due to consumption), budget gets tighter."""
        s = make_scheduler(max_bytes=10000, available_cpus=10, ema_alpha=0.5)
        source, mapper = make_linear_pipeline(2, ["source", "mapper"])
        s.register_operator(source, initial_peak_bytes=500.0)
        s.register_operator(mapper, initial_peak_bytes=100.0)

        # First task: pipelined consumption keeps peak at 100.
        tid = s.dispatch_task(source, input_bytes=0)
        for i in range(5):
            s.on_task_output(tid, output_bytes=100)
            s.dispatch_task(mapper, input_bytes=100, input_task_ids=[tid])
        s.on_task_finished(tid)
        # EMA: 0.5 * 100 + 0.5 * 500 = 300
        assert s.op_states[source].expected_peak_bytes == pytest.approx(300.0)

        # Second task: same behavior.
        tid = s.dispatch_task(source, input_bytes=0)
        for i in range(5):
            s.on_task_output(tid, output_bytes=100)
            s.dispatch_task(mapper, input_bytes=100, input_task_ids=[tid])
        s.on_task_finished(tid)
        # EMA: 0.5 * 100 + 0.5 * 300 = 200
        assert s.op_states[source].expected_peak_bytes == pytest.approx(200.0)

    def test_selection_drains_downstream_first(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        source, mapper, sink = make_linear_pipeline(3)
        for op in [source, mapper, sink]:
            s.register_operator(op)

        inputs = {source: 100, mapper: 100, sink: 100}

        # All have 0 buffered bytes (tied) -> pick downstream (sink).
        assert s.select_operator(inputs) is sink

        s.op_states[mapper].buffered_output_bytes = 500
        s.op_states[sink].buffered_output_bytes = 0
        assert s.select_operator(inputs) is sink

        s.op_states[source].buffered_output_bytes = 0
        s.op_states[sink].buffered_output_bytes = 100
        assert s.select_operator(inputs) is source

    def test_budget_invariant_after_full_lifecycle(self):
        """After all tasks complete, budget returns to 0."""
        s = make_scheduler(max_bytes=50000, available_cpus=4, ema_alpha=0.3)
        reader, processor = make_linear_pipeline(2, ["reader", "processor"])
        s.register_operator(reader, initial_peak_bytes=200.0)
        s.register_operator(processor, initial_peak_bytes=200.0)

        for i in range(5):
            rt = s.dispatch_task(reader, input_bytes=0, num_cpus=1.0)
            s.on_task_output(rt, output_bytes=200)
            s.on_task_finished(rt)

            pt = s.dispatch_task(
                processor, input_bytes=200, num_cpus=1.0, input_task_ids=[rt]
            )
            s.on_task_output(pt, output_bytes=180)
            s.on_task_finished(pt)

        assert s.allocated_bytes == 0
        assert s.available_cpus == 4.0
        assert len(s.task_states) == 0

    def test_concurrent_tasks_share_budget(self):
        s = make_scheduler(max_bytes=10000, available_cpus=4)
        op = make_mock_op()
        s.register_operator(op, initial_peak_bytes=100.0)

        tids = [s.dispatch_task(op, input_bytes=100, num_cpus=1.0) for _ in range(4)]
        assert s.available_cpus == 0.0
        assert s.allocated_bytes == 400  # 4 * 100

        for tid in tids:
            s.on_task_output(tid, output_bytes=80)
            s.on_task_finished(tid)

        assert s.available_cpus == 4.0
        assert s.allocated_bytes == 0
        assert len(s.task_states) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
