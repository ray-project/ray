"""Tests for integration of lineage reconstruction into the Ray Data streaming executor.

The end-to-end tests inject an ``ObjectLostError`` into a task's output read and
check that the rows a consumer receives match a run without loss. Tracker semantics are
covered in ``tests/unit/test_lineage_tracker_*.py``; the remaining tests here are
unit tests of the abort protocol between ``DataOpTask``, the metadata fetcher and
the bundler.
"""

from typing import Dict
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.lineage_tracker import LineageTracker
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.util import create_map_transformer_from_block_fn
from ray.exceptions import ObjectLostError
from ray.job_config import JobConfig
from ray.tests.conftest import *  # noqa: F401, F403

recovery_enabled = pytest.mark.parametrize(
    "ray_start_regular_shared",
    [{"job_config": JobConfig(_disable_job_level_lineage_reconstruction=True)}],
    indirect=True,
    ids=["core_lineage_off"],
)


@pytest.fixture
def trackers(monkeypatch):
    """Every ``LineageTracker`` the executor builds during the test."""
    created = []
    original_init = LineageTracker.__init__

    def spy(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created.append(self)

    monkeypatch.setattr(LineageTracker, "__init__", spy)
    return created


@pytest.fixture
def lose_output(monkeypatch):
    """Make ``victim``'s tasks raise ``ObjectLostError`` from their output read.

    ``lose_output(victim, task_indices, reads_before_loss)`` picks the operator by a
    name substring and the task indices to fail. Each fails once, after
    ``reads_before_loss`` successful reads. Returns the list of indices that failed.
    """

    def install(victim, task_indices=(0,), reads_before_loss=0):
        pending = set(task_indices)
        reads_seen: Dict[int, int] = {}
        fired = []
        original_on_data_ready = DataOpTask.on_data_ready

        def flaky_on_data_ready(self, max_bytes_to_read, metadata_fetcher):
            index = self.task_index()
            if victim in self.operator_name and index in pending:
                if reads_seen.get(index, 0) >= reads_before_loss:
                    pending.discard(index)
                    fired.append(index)
                    raise ObjectLostError(
                        ray.ObjectRef.nil().hex(), None, "injected by test"
                    )
                reads_seen[index] = reads_seen.get(index, 0) + 1
            return original_on_data_ready(self, max_bytes_to_read, metadata_fetcher)

        monkeypatch.setattr(DataOpTask, "on_data_ready", flaky_on_data_ready)
        return fired

    return install


def _nodes(tracker):
    return tracker._data_task_id_to_task_node


def _assert_every_row_once(ids, expected):
    actual = sorted(ids)
    duplicates = len(actual) - len(set(actual))
    assert actual == expected, (
        f"expected {len(expected)} rows, got {len(actual)} "
        f"({len(actual) - len(expected):+d}), duplicates={duplicates}"
    )


def test_flag_off_builds_no_tracker(
    ray_start_regular_shared, trackers, lose_output  # noqa: F405
):
    """By default the job leaves reconstruction to Ray Core: execution must not touch
    the lineage machinery, and a lost output surfaces as the error it always was.
    """
    assert not DataContext.get_current().enable_ray_data_reconstruction
    fired = lose_output("ReadRange")

    with pytest.raises(ObjectLostError):
        ray.data.range(50, override_num_blocks=4).map(lambda row: row).take_all()

    assert fired == [0]
    assert trackers == []


@pytest.mark.parametrize("reads_before_loss", [0, 1])
@recovery_enabled
def test_iter_batches_recovery_matches_baseline(
    ray_start_regular_shared, lose_output, reads_before_loss  # noqa: F405
):
    """A seed task loses its output and is re-run; the caller sees every row once.

    ``iter_batches`` hands batches to user code as they arrive, so a duplicate here
    means the same row was handed over twice. ``reads_before_loss=1`` loses the
    output after an earlier read already put a block in flight, which must not be
    re-emitted on top of the reconstructed one.
    """
    fired = lose_output("ReadRange", reads_before_loss=reads_before_loss)
    dataset = ray.data.range(100, override_num_blocks=4).map(
        lambda row: {"id": row["id"] * 2}
    )

    seen = []
    for batch in dataset.iter_batches(batch_size=10):
        seen.extend(int(value) for value in batch["id"])

    assert fired == [0]
    _assert_every_row_once(seen, [i * 2 for i in range(100)])


@recovery_enabled
def test_two_losses_under_one_seed_match_baseline(
    ray_start_regular_shared, restore_data_context, lose_output  # noqa: F405
):
    """Two children of one seed lose their output; the seed re-runs, every row once.

    Tiny blocks make the single read task fan out to several tasks. The
    re-produced outputs a plan does not need must be pruned, not re-emitted.
    """
    restore_data_context.target_max_block_size = 1
    fired = lose_output("MapBatches", task_indices=(0, 1))

    rows = (
        ray.data.range(20, override_num_blocks=1)
        .map(lambda row: {"id": row["id"] + 1})
        .map_batches(lambda batch: batch, batch_size=5, concurrency=1)
        .take_all()
    )

    assert sorted(fired) == [0, 1]
    _assert_every_row_once((row["id"] for row in rows), [i + 1 for i in range(20)])


@pytest.mark.parametrize(
    ("num_rows", "num_blocks", "batch_size", "one_row_blocks"),
    [
        pytest.param(20, 2, 20, False, id="fan_in_2_to_1"),
        pytest.param(200, 20, 200, False, id="fan_in_20_to_1"),
        pytest.param(60, 6, 30, False, id="fan_in_6_to_2"),
        pytest.param(20, 1, 1, True, id="fan_out_1_to_20"),
    ],
)
@recovery_enabled
def test_lost_child_recovers_across_graph_shapes(
    ray_start_regular_shared,  # noqa: F405
    restore_data_context,  # noqa: F405
    trackers,
    lose_output,
    num_rows,
    num_blocks,
    batch_size,
    one_row_blocks,
):
    """A child loses its output; every parent re-runs and the child re-executes once.

    ``num_blocks`` seeds feed ``num_rows / batch_size`` children. Fan-in: a child's
    re-produced inputs are held until the whole set is in hand. Fan-out: the seed's
    outputs owed to other children are pruned rather than re-emitted.
    """
    if one_row_blocks:
        restore_data_context.target_max_block_size = 1
    num_children = num_rows // batch_size
    # Lose the last child submitted: by then every sibling is registered, so the
    # seed's other re-produced outputs are pruned. An earlier child would re-emit
    # siblings still queued -- right for a dead node, duplicates under an injection.
    fired = lose_output("MapBatches", task_indices=(num_children - 1,))

    rows = (
        ray.data.range(num_rows, override_num_blocks=num_blocks)
        .map(lambda row: {"id": row["id"] + 1})
        .map_batches(lambda batch: batch, batch_size=batch_size, concurrency=1)
        .take_all()
    )

    assert fired == [num_children - 1]
    _assert_every_row_once(
        (row["id"] for row in rows), [i + 1 for i in range(num_rows)]
    )
    # Seeds plus children. A child re-registered under a fresh id would add one.
    (tracker,) = trackers
    assert len(_nodes(tracker)) == num_blocks + num_children


def test_reconstruction_input_bypasses_the_bundler(
    ray_start_regular_shared,
):  # noqa: F405
    """A reconstruction child's input set must reach submission exactly as assembled.

    ``RebundleQueue`` would hold it back, merge it with other pending input, or slice
    it to hit the row target; any of those runs the child against the wrong blocks.
    """
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.util import make_ref_bundles

    ctx = DataContext.get_current()
    inputs = make_ref_bundles([[1]])[0]
    op = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=InputDataBuffer(ctx, [inputs]),
        data_context=ctx,
        name="Consumer",
        min_rows_per_bundle=1000,
    )
    scheduled = []
    op._try_schedule_task = lambda refs, strict: scheduled.append((refs, strict))

    # Ordinary input waits in the bundler for the row target.
    op._add_input_inner(inputs, 0)
    assert scheduled == []
    assert op._block_ref_bundler.num_blocks() == 1

    # A stamped reconstruction input goes straight to submission.
    stamped = make_ref_bundles([[2]])[0]
    op._pending_child_ids[stamped.block_refs[0].hex()] = ("child:0", "plan_a")
    op._add_input_inner(stamped, 0)
    assert scheduled == [(stamped, True)]
    assert op._block_ref_bundler.num_blocks() == 1
    # Read, not consumed: `_lineage_for_submission` still needs it to name the task.
    assert op._pending_child_ids == {stamped.block_refs[0].hex(): ("child:0", "plan_a")}


def _abortable_task(task_done_callback):
    """A ``DataOpTask`` in its freshly-submitted (ACTIVE) state.

    ``mark_aborted`` reads neither the generator nor the ref counter, so both are
    stubs; this keeps the test on the state machine and off a live cluster.
    """
    return DataOpTask(
        0,
        MagicMock(),  # streaming_gen
        MagicMock(),  # block_ref_counter
        "test_op",
        task_done_callback=task_done_callback,
        data_task_id="seed:0",
    )


def test_mark_aborted_fires_the_done_callback_exactly_once():
    """Abort from ACTIVE runs the done-callback with the error and finishes the task.

    A repeat abort, or an abort after normal completion, must not run it again: that
    would double-release the task's resource reservations.
    """
    calls = []
    task = _abortable_task(lambda exc, worker_stats, driver_stats: calls.append(exc))
    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")

    task.mark_aborted(error)
    task.mark_aborted(error)
    assert calls == [error]
    assert task.has_finished

    calls.clear()
    task = _abortable_task(lambda exc, worker_stats, driver_stats: calls.append(exc))
    task.mark_done()
    task.mark_aborted(error)
    assert calls == [None]


def test_a_drained_task_aborted_before_its_done_callback_fires_it_once():
    """``_fire_done_callbacks`` must skip a drained task that was since aborted."""
    from ray.data._internal.execution.metadata_fetcher import ThreadedMetadataFetcher

    calls = []
    task = _abortable_task(lambda exc, worker_stats, driver_stats: calls.append(exc))
    fetcher = ThreadedMetadataFetcher()
    fetcher._drained_tasks.add(task)
    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
    task.mark_aborted(error)

    fetcher._fire_done_callbacks()

    assert calls == [error]
    assert task not in fetcher._drained_tasks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
