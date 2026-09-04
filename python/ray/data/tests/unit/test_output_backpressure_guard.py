from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.interfaces.physical_operator import MetadataOpTask
from ray.data._internal.execution.streaming_executor_state import (
    OutputBackpressureGuard,
)
from ray.data.tests.util import FakeDataOpTask, make_backpressured_op


def _guard() -> OutputBackpressureGuard:
    """A guard for exercising its stateless task-ordering / bypass-lane policy.

    ``order_ready_tasks`` and ``reconstruction_bypass_lane`` take the queued
    generator set as an argument, so no per-round setup is needed.
    """
    return OutputBackpressureGuard({}, MagicMock())


def test_order_ready_tasks_puts_queued_generators_first():
    gen = FakeDataOpTask(2, "gen", bytes_read=10)
    normal = FakeDataOpTask(0, "normal", bytes_read=10)
    meta = MagicMock(spec=MetadataOpTask)
    meta.task_index.return_value = 1

    # The queued-for-resubmit generator claims the operator's output budget
    # first, even though its task index is highest. Non-data tasks (no task ID)
    # are ordered without being probed for eligibility.
    assert _guard().order_ready_tasks([normal, meta, gen], {"gen"}) == [
        gen,
        normal,
        meta,
    ]

    # With nothing queued, ordering falls back to plain task-index order.
    assert _guard().order_ready_tasks([gen, meta, normal], set()) == [
        normal,
        meta,
        gen,
    ]


@pytest.mark.parametrize("op_output_budget", [0, 1])
def test_reconstruction_bypass_lane_granted_when_fully_backpressured(op_output_budget):
    # Budget 0 is full backpressure; budget 1 is what ``should_unblock`` bumps it
    # to, which is still too little for an ordinary task to make progress.
    gen = FakeDataOpTask(0, "gen", bytes_read=10)
    lane = _guard().reconstruction_bypass_lane(
        make_backpressured_op(3), op_output_budget, {"gen"}
    )
    assert lane.try_drain(gen, metadata_fetcher=None)
    # Drained one block at a time so the bypass stays bounded.
    assert gen.calls == [("gen", 1)] * 3


@pytest.mark.parametrize("op_output_budget", [None, 2, 1024])
def test_no_reconstruction_bypass_lane_when_output_budget_allows_progress(
    op_output_budget,
):
    gen = FakeDataOpTask(0, "gen", bytes_read=10)
    lane = _guard().reconstruction_bypass_lane(
        make_backpressured_op(3), op_output_budget, {"gen"}
    )
    assert not lane.try_drain(gen, metadata_fetcher=None)
    assert gen.calls == []


def test_reconstruction_bypass_lane_disabled_by_zero_bypass_blocks():
    gen = FakeDataOpTask(0, "gen", bytes_read=10)
    lane = _guard().reconstruction_bypass_lane(make_backpressured_op(0), 0, {"gen"})
    assert not lane.try_drain(gen, metadata_fetcher=None)
    assert gen.calls == []


def test_reconstruction_bypass_lane_ignores_ordinary_tasks():
    normal = FakeDataOpTask(0, "normal", bytes_read=10)
    lane = _guard().reconstruction_bypass_lane(make_backpressured_op(3), 0, {"gen"})
    assert not lane.try_drain(normal, metadata_fetcher=None)
    assert normal.calls == []


def test_reconstruction_bypass_lane_stops_when_generator_has_no_ready_output():
    # The lane pulls one block at a time up to the configured cap, but stops as
    # soon as ``on_data_ready`` reports 0 bytes read (generator drained / pending
    # emits / nothing buffered), so it never spins the full cap for nothing.
    gen = FakeDataOpTask(0, "gen", bytes_read=0)
    lane = _guard().reconstruction_bypass_lane(make_backpressured_op(100), 0, {"gen"})
    assert lane.try_drain(gen, metadata_fetcher=None)
    assert gen.calls == [("gen", 1)]


def test_reconstruction_bypass_lane_budget_is_shared_per_operator():
    # ``lineage_reconstruction_backpressure_bypass_blocks`` is a per-operator
    # budget shared across that operator's queued-for-resubmit generators, NOT
    # granted N-per-task: with two queued generators and a cap of 3, the total
    # is 3 bypass reads, not 6.
    gen_a = FakeDataOpTask(0, "gen_a", bytes_read=10)
    gen_b = FakeDataOpTask(1, "gen_b", bytes_read=10)
    lane = _guard().reconstruction_bypass_lane(
        make_backpressured_op(3), 0, {"gen_a", "gen_b"}
    )

    assert lane.try_drain(gen_a, metadata_fetcher=None)
    assert gen_a.calls == [("gen_a", 1)] * 3
    # gen_b finds the shared allowance exhausted and falls back to the
    # operator's (zeroed) byte budget.
    assert not lane.try_drain(gen_b, metadata_fetcher=None)
    assert gen_b.calls == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
