import sys
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.lineage_tracker import LineageTracker
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
    format_op_state_summary,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import noop_counter


def test_recovery_status_counts_queued_running_and_finished():
    """Queued counts reconstruction tasks not yet submitted, and running counts only
    tasks serving a plan, once each however many plans they serve."""
    # Only the attributes the method reads; cast so it type-checks as ``self``.
    state = SimpleNamespace(
        _lineage_tracker=LineageTracker(),
        _batched_plans={"op:3": set(), "op:4": {"plan_d"}},
        _data_tasks={
            0: SimpleNamespace(reconstruction_plan_ids=frozenset()),
            1: SimpleNamespace(reconstruction_plan_ids=frozenset({"plan_a"})),
            2: SimpleNamespace(reconstruction_plan_ids=frozenset({"plan_b", "plan_c"})),
        },
        _num_reconstruction_tasks_finished=3,
    )
    op = cast(MapOperator, state)
    assert MapOperator.lineage_recovery_progress_str(op) == (
        "Recovery: 2 queued, 2 running, 3 finished"
    )

    state._lineage_tracker = None
    assert MapOperator.lineage_recovery_progress_str(op) == ""


def test_summary_str_lineage_recovery():
    """The recovery status shows only when seed-input lineage recovery is on and
    the operator has re-run tasks."""
    data_context = DataContext.get_current()
    o1 = InputDataBuffer(data_context, [])
    o2 = MapOperator.create(
        MapTransformer([BlockMapTransformFn(lambda blocks, ctx: blocks)]),
        o1,
        data_context,
        name="O2",
    )
    topo = build_streaming_topology(o2, ExecutionOptions(), noop_counter())
    resource_manager = MagicMock()

    # Recovery disabled: nothing shown, even with re-run tasks recorded.
    o2._num_reconstruction_tasks_finished = 2
    assert "Recovery" not in format_op_state_summary(topo[o2], resource_manager)

    # Recovery enabled but nothing re-run: nothing shown.
    o2._lineage_tracker = LineageTracker()
    o2._num_reconstruction_tasks_finished = 0
    assert "Recovery" not in format_op_state_summary(topo[o2], resource_manager)

    # The cumulative count stays on the line after recovery has finished.
    o2._num_reconstruction_tasks_finished = 2
    summary = format_op_state_summary(topo[o2], resource_manager)
    assert summary.endswith("; Recovery: 0 queued, 0 running, 2 finished")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
