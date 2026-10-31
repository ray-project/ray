"""Tests for integration of lineage reconstruction into the Ray Data streaming executor.

The end-to-end tests inject an ``ObjectLostError`` into a task's output read and
check that the rows a consumer receives match a run without loss. Tracker semantics are
covered in ``tests/unit/test_lineage_tracker_*.py``; the remaining tests here are
unit tests of the abort protocol between ``DataOpTask``, the metadata fetcher and
the bundler.
"""

import pickle
from dataclasses import replace
from typing import TYPE_CHECKING, Dict, List
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces.physical_operator import DataOpTask
from ray.data._internal.execution.interfaces.ref_bundle import ReconstructionStamp
from ray.data._internal.execution.lineage_tracker import LineageTracker
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.streaming_executor_state import (
    _reconstruct_lost_object,
)
from ray.data.context import DataContext
from ray.data.exceptions import LineageReconstructionError
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.util import create_map_transformer_from_block_fn
from ray.exceptions import ObjectLostError
from ray.job_config import JobConfig
from ray.tests.conftest import *  # noqa: F401, F403

if TYPE_CHECKING:
    from ray.data._internal.execution.interfaces import RefBundle

reconstruction_enabled = pytest.mark.parametrize(
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
    return tracker._lineage_task_id_to_task_node


def _assert_every_row_once(ids, expected):
    actual = sorted(ids)
    duplicates = len(actual) - len(set(actual))
    assert actual == expected, (
        f"expected {len(expected)} rows, got {len(actual)} "
        f"({len(actual) - len(expected):+d}), duplicates={duplicates}"
    )


def test_flag_off_disables_data_lineage_reconstruction(
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
@reconstruction_enabled
def test_iter_batches_reconstruction_matches_baseline(
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


@reconstruction_enabled
def test_two_losses_under_one_seed_match_baseline(
    ray_start_regular_shared, restore_data_context, trackers, lose_output  # noqa: F405
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
    # One reconstruction attempt per lost object.
    assert [t.num_object_reconstruction_attempts for t in trackers] == [2]


def _produced_bundles(simple_data):
    """``make_ref_bundles`` plus the per-block exec stats a real task output carries.

    ``OpRuntimeMetrics.on_task_output_generated`` asserts on them, so bundles fed
    straight into a task's output callback need them where plain inputs do not.
    """
    import pandas as pd
    import pyarrow as pa

    from ray.data._internal.execution.interfaces import BlockEntry, RefBundle
    from ray.data.block import BlockAccessor, BlockExecStats

    bundles = []
    for rows in simple_data:
        block = pd.DataFrame({"id": rows})
        exec_stats = BlockExecStats.builder().build(block_ser_time_s=0.0)
        bundles.append(
            RefBundle(
                (
                    BlockEntry(
                        ray.put(block),  # pyrefly: ignore[bad-argument-type]
                        BlockAccessor.for_block(block).get_metadata(
                            block_exec_stats=exec_stats
                        ),
                    ),
                ),
                owns_blocks=True,
                schema=pa.Schema.from_pandas(block, preserve_index=False),
            )
        )
    return bundles


def _seed_and_consumer_ops():
    """A started seed operator reading from an ``InputDataBuffer`` and a started
    consumer of its output, sharing one ``LineageTracker``.

    Returns:
        The seed operator, the consumer operator, the seed input and the tracker.
    """
    from ray.data._internal.execution.interfaces import ExecutionOptions
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.util import make_ref_bundles

    ctx = DataContext.get_current()
    seed_input = make_ref_bundles([[0]])[0]
    producer = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=InputDataBuffer(ctx, [seed_input]),
        data_context=ctx,
        name="Seed",
    )
    consumer = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=producer,
        data_context=ctx,
        name="Consumer",
    )
    tracker = LineageTracker()
    producer.start(ExecutionOptions(), noop_counter(), tracker)  # noqa: F405
    consumer.start(ExecutionOptions(), noop_counter(), tracker)  # noqa: F405
    return producer, consumer, seed_input, tracker


def _drain(op):
    """Every bundle in ``op``'s output queue, in order."""
    bundles = []
    while op.has_next():
        bundles.append(op.get_next())
    return bundles


def _lose(topology: Dict, tracker: LineageTracker, lost_task_id: str) -> None:
    """Report that ``lost_task_id``'s output was lost, the way the executor does.

    Args:
        topology: The operator topology to reconstruct in.
        tracker: The lineage tracker the operators share.
        lost_task_id: The lineage task ID of the task whose output was lost.
    """
    task = DataOpTask(
        0, MagicMock(), MagicMock(), "lost_op", lineage_task_id=lost_task_id
    )
    _reconstruct_lost_object(
        topology,
        tracker,
        MagicMock(),
        task,
        ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test"),
    )


def test_losses_before_the_seed_is_submitted_share_one_reconstruction_task(
    ray_start_regular_shared,  # noqa: F405
):
    """Two children of one seed lose their output before the seed's re-execution is
    submitted.

    The executor re-injects the seed once and batches the second plan into it. The
    one re-run then serves both plans. It withholds each re-produced output under the
    plan whose child needs it, and releases each child once under its own plan.
    """
    from ray.data._internal.execution.interfaces.physical_operator import (
        TaskExecDriverStats,
    )
    from ray.data._internal.execution.lineage_tracker import ParentBlockOutput
    from ray.data._internal.execution.util import make_ref_bundles

    producer, consumer, seed_input, tracker = _seed_and_consumer_ops()

    # First run: the seed fans two blocks out to two children, then completes.
    seed_task_id = producer._lineage_task_id_for(0)
    child_task_ids = [consumer._lineage_task_id_for(index) for index in range(2)]
    first_run = make_ref_bundles([[1], [2]])
    tracker.register_task_submission(seed_task_id, [])
    tracker.register_seed_input(seed_task_id, seed_input)
    for output_index, (child_task_id, bundle) in enumerate(
        zip(child_task_ids, first_run)
    ):
        block_id = bundle.block_refs[0].hex()  # pyrefly: ignore[missing-attribute]
        tracker.register_block_output(seed_task_id, block_id, output_index)
        tracker.register_block_queued(seed_task_id, output_index)
        tracker.register_task_submission(
            child_task_id, tracker.resolve_dependencies([block_id])
        )
    tracker.register_task_complete(seed_task_id)

    # Both children are lost before the seed's re-execution is submitted. Each plan
    # is keyed by its lost child. The first loss re-injects the seed, and the second
    # is batched into that queued re-execution instead of re-injecting it again.
    plan_ids = child_task_ids
    source_op = producer.input_dependencies[0]
    topology = {source_op: MagicMock(), producer: MagicMock(), consumer: MagicMock()}
    for child_task_id in child_task_ids:
        _lose(topology, tracker, child_task_id)
    (call,) = topology[source_op].add_output.call_args_list
    reinjected = call.args[0]
    assert reinjected.reconstruction_stamp == ReconstructionStamp(
        seed_task_id, frozenset({plan_ids[0]})
    )
    assert producer._batched_plans == {seed_task_id: {plan_ids[1]}}

    # The one re-execution is submitted under both plans.
    producer._submit_data_task(MagicMock(), reinjected)
    (task,) = producer._data_tasks.values()
    assert task.lineage_task_id == seed_task_id
    assert task.reconstruction_plan_ids == frozenset(plan_ids)
    assert producer._batched_plans == {}

    # Each re-produced output is withheld under exactly the plan whose child needs
    # it. Nothing reaches the output queue before the last withheld output.
    second_run = _produced_bundles([[1], [2]])
    task._output_ready_callback(second_run[0])
    assert not producer.has_next()
    assert producer._reconstruction_outputs == {
        plan_ids[0]: {ParentBlockOutput(seed_task_id, 0): second_run[0]},
    }

    # The last withheld output releases each child once, under its own plan, before
    # the task completes.
    task._output_ready_callback(second_run[1])
    released = _drain(producer)
    assert [bundle.block_refs for bundle in released] == [
        second_run[0].block_refs,
        second_run[1].block_refs,
    ]
    assert [bundle.reconstruction_stamp for bundle in released] == [
        ReconstructionStamp(child_task_ids[0], frozenset({plan_ids[0]})),
        ReconstructionStamp(child_task_ids[1], frozenset({plan_ids[1]})),
    ]
    assert producer._reconstruction_outputs == {}

    # Completing releases nothing more.
    task._task_done_callback(
        None, None, TaskExecDriverStats(task_output_backpressure_s=0.0), False
    )
    assert not producer.has_next()


def test_joined_plans_sharing_a_child_release_it_once(
    ray_start_regular_shared,  # noqa: F405
):
    """Two leaves under one child of a seed are lost while the seed's re-execution
    is still queued.

    Both plans need that child, and they share one seed re-run. The re-run
    withholds the child's input under both plans, then releases it once for both,
    and the child is submitted once under both plans.
    """
    from ray.data._internal.execution.interfaces.physical_operator import (
        TaskExecDriverStats,
    )
    from ray.data._internal.execution.util import make_ref_bundles

    producer, consumer, seed_input, tracker = _seed_and_consumer_ops()

    # First run: the seed feeds one child, which fans two blocks out to two leaves.
    seed_task_id = producer._lineage_task_id_for(0)
    child_task_id = consumer._lineage_task_id_for(0)
    leaf_task_ids = ["leaf:0", "leaf:1"]
    seed_block_ref = make_ref_bundles([[1]])[0].block_refs[0]
    seed_block_id = seed_block_ref.hex()  # pyrefly: ignore[missing-attribute]
    tracker.register_task_submission(seed_task_id, [])
    tracker.register_block_output(seed_task_id, seed_block_id, 0)
    tracker.register_block_queued(seed_task_id, 0)
    tracker.register_task_submission(
        child_task_id, tracker.resolve_dependencies([seed_block_id])
    )
    for output_index, leaf_task_id in enumerate(leaf_task_ids):
        child_block_id = f"{child_task_id}/{output_index}"
        tracker.register_block_output(child_task_id, child_block_id, output_index)
        tracker.register_block_queued(child_task_id, output_index)
        tracker.register_task_submission(
            leaf_task_id, tracker.resolve_dependencies([child_block_id])
        )

    # Both leaves are lost. The second plan joins the first plan's queued seed
    # re-execution even though both plans need the same child.
    plan_ids = [
        tracker.register_task_failed(leaf_task_id)[1] for leaf_task_id in leaf_task_ids
    ]
    stamped_seed_input = producer.stamp_seed_reconstruction_task(
        seed_task_id, {plan_ids[0]}, seed_input
    )
    assert producer.join_queued_reconstruction_task(seed_task_id, {plan_ids[1]})

    producer._submit_data_task(MagicMock(), stamped_seed_input)
    (task,) = producer._data_tasks.values()
    assert task.reconstruction_plan_ids == frozenset(plan_ids)

    # The child's input is the last withheld output under both plans, so producing it
    # releases the child once, for both plans, before the task completes.
    (second_run,) = _produced_bundles([[1]])
    task._output_ready_callback(second_run)
    (released,) = _drain(producer)
    assert released.block_refs == second_run.block_refs
    assert released.reconstruction_stamp == ReconstructionStamp(
        child_task_id, frozenset(plan_ids)
    )
    assert producer._reconstruction_outputs == {}

    # Completing releases nothing more.
    task._task_done_callback(
        None, None, TaskExecDriverStats(task_output_backpressure_s=0.0), False
    )
    assert not producer.has_next()

    # The child is submitted once, as a re-execution of both plans.
    consumer._submit_data_task(MagicMock(), released)
    (child_task,) = consumer._data_tasks.values()
    assert child_task.lineage_task_id == child_task_id
    assert child_task.reconstruction_plan_ids == frozenset(plan_ids)


def _run_to_completion(
    op: MapOperator, inputs: "RefBundle", outputs: List["RefBundle"]
) -> DataOpTask:
    """Submit ``inputs`` on ``op``, emit ``outputs`` from the task and complete it.

    Args:
        op: The operator to submit the task on.
        inputs: The task's input bundle.
        outputs: The bundles the task emits, in order.

    Returns:
        The submitted task.
    """
    from ray.data._internal.execution.interfaces.physical_operator import (
        TaskExecDriverStats,
    )

    submitted = set(op._data_tasks)
    op._submit_data_task(MagicMock(), inputs)
    (task_index,) = set(op._data_tasks) - submitted
    task = op._data_tasks[task_index]
    for bundle in outputs:
        task._output_ready_callback(bundle)
    task._task_done_callback(
        None, None, TaskExecDriverStats(task_output_backpressure_s=0.0), False
    )
    return task


@pytest.mark.parametrize("child_submitted_first", [False, True])
def test_shared_child_is_batched_until_it_is_submitted(
    ray_start_regular_shared, child_submitted_first  # noqa: F405
):
    """A child reads from two seeds, and two leaves under it are lost.

    The second plan joins the first plan's queued reconstruction task of one seed,
    but the other seed re-runs once per plan. So the child's input is complete for
    the first plan before the second, and the child is released for the first plan
    alone.

    If that reconstruction task of the child is still queued when the second plan's
    input completes, the second plan is batched into it and the child runs once for
    both plans. If it was already submitted, the second plan releases the child again
    on its own.
    """
    from ray.data._internal.execution.util import make_ref_bundles

    producer, consumer, seed_input, tracker = _seed_and_consumer_ops()

    # First run: two seeds feed one child, which fans two blocks out to two leaves.
    seed_task_ids = [producer._lineage_task_id_for(index) for index in range(2)]
    child_task_id = consumer._lineage_task_id_for(0)
    leaf_task_ids = ["leaf:0", "leaf:1"]
    seed_block_ids = []
    for seed_task_id in seed_task_ids:
        block_ref = make_ref_bundles([[1]])[0].block_refs[0]
        block_id = block_ref.hex()  # pyrefly: ignore[missing-attribute]
        tracker.register_task_submission(seed_task_id, [])
        tracker.register_block_output(seed_task_id, block_id, 0)
        tracker.register_block_queued(seed_task_id, 0)
        seed_block_ids.append(block_id)
    tracker.register_task_submission(
        child_task_id, tracker.resolve_dependencies(seed_block_ids)
    )
    for output_index, leaf_task_id in enumerate(leaf_task_ids):
        child_block_id = f"{child_task_id}/{output_index}"
        tracker.register_block_output(child_task_id, child_block_id, output_index)
        tracker.register_block_queued(child_task_id, output_index)
        tracker.register_task_submission(
            leaf_task_id, tracker.resolve_dependencies([child_block_id])
        )
    plan_a, plan_b = (
        tracker.register_task_failed(leaf_task_id)[1] for leaf_task_id in leaf_task_ids
    )

    # The second seed re-runs for the first plan alone. The child still waits on the
    # first seed.
    (second_seed_a,) = _produced_bundles([[2]])
    _run_to_completion(
        producer,
        producer.stamp_seed_reconstruction_task(seed_task_ids[1], {plan_a}, seed_input),
        [second_seed_a],
    )
    assert not producer.has_next()

    # Both plans share the first seed's re-run. Its completion completes the child's
    # input for the first plan only.
    stamped = producer.stamp_seed_reconstruction_task(
        seed_task_ids[0], {plan_a}, seed_input
    )
    assert producer.join_queued_reconstruction_task(seed_task_ids[0], {plan_b})
    (first_seed,) = _produced_bundles([[1]])
    _run_to_completion(producer, stamped, [first_seed])
    (released_a,) = _drain(producer)
    assert released_a.block_refs == first_seed.block_refs + second_seed_a.block_refs
    assert released_a.reconstruction_stamp == ReconstructionStamp(
        child_task_id, frozenset({plan_a})
    )
    if child_submitted_first:
        consumer._submit_data_task(MagicMock(), released_a)

    # The second seed's re-run for the second plan completes the child's input for
    # that plan.
    (second_seed_b,) = _produced_bundles([[2]])
    _run_to_completion(
        producer,
        producer.stamp_seed_reconstruction_task(seed_task_ids[1], {plan_b}, seed_input),
        [second_seed_b],
    )
    assert producer._reconstruction_outputs == {}
    if child_submitted_first:
        # Too late to batch, so the child is released again for the second plan.
        (released_b,) = _drain(producer)
        assert released_b.block_refs == first_seed.block_refs + second_seed_b.block_refs
        assert released_b.reconstruction_stamp == ReconstructionStamp(
            child_task_id, frozenset({plan_b})
        )
        consumer._submit_data_task(MagicMock(), released_b)
        expected_plan_ids = [[plan_a], [plan_b]]
    else:
        # The child is still queued, so the second plan is batched into it.
        assert not producer.has_next()
        assert consumer._batched_plans == {child_task_id: {plan_b}}
        consumer._submit_data_task(MagicMock(), released_a)
        expected_plan_ids = [sorted([plan_a, plan_b])]
    assert consumer._batched_plans == {}
    assert (
        sorted(
            sorted(task.reconstruction_plan_ids)
            for task in consumer._data_tasks.values()
        )
        == expected_plan_ids
    )


def test_seed_targeted_by_one_plan_and_reused_by_another(
    ray_start_regular_shared,  # noqa: F405
):
    """A seed loses an output it had not queued yet, and its child loses its own.

    The seed is the target of the first plan and an intermediate task of the
    second, and one re-run serves both. The output the child consumed is held for
    the second plan only. The output never queued is NEW for the first plan, and
    goes to the output queue unstamped.
    """
    producer, consumer, seed_input, tracker = _seed_and_consumer_ops()

    # First run: the seed queues its first output to the child, then loses the
    # second before queueing it.
    seed_task_id = producer._lineage_task_id_for(0)
    child_task_id = consumer._lineage_task_id_for(0)
    seed_block_id = "seed/0"
    tracker.register_task_submission(seed_task_id, [])
    tracker.register_block_output(seed_task_id, seed_block_id, 0)
    tracker.register_block_queued(seed_task_id, 0)
    tracker.register_task_submission(
        child_task_id, tracker.resolve_dependencies([seed_block_id])
    )
    seed_plan = tracker.register_task_failed(seed_task_id)[1]
    child_plan = tracker.register_task_failed(child_task_id)[1]

    stamped = producer.stamp_seed_reconstruction_task(
        seed_task_id, {seed_plan}, seed_input
    )
    assert producer.join_queued_reconstruction_task(seed_task_id, {child_plan})
    consumed_output, unqueued_output = _produced_bundles([[1], [2]])
    task = _run_to_completion(producer, stamped, [consumed_output, unqueued_output])
    assert task.reconstruction_plan_ids == frozenset({seed_plan, child_plan})

    # The child's input is the last withheld output, so the child is released for its
    # plan as soon as it is produced. The NEW output after it is queued as is.
    released, new_output = _drain(producer)
    assert new_output.block_refs == unqueued_output.block_refs
    assert new_output.reconstruction_stamp is None
    assert released.block_refs == consumed_output.block_refs
    assert released.reconstruction_stamp == ReconstructionStamp(
        child_task_id, frozenset({child_plan})
    )
    assert producer._reconstruction_outputs == {}
    assert _nodes(tracker)[seed_task_id].num_queued_outputs == 2


def test_loss_after_the_seed_reconstruction_task_is_submitted_re_injects_the_seed(
    ray_start_regular_shared,  # noqa: F405
):
    """A plan that traces back to a seed whose re-execution was already submitted
    cannot join it, and re-injects the seed for itself.
    """
    producer, consumer, seed_input, tracker = _seed_and_consumer_ops()

    seed_task_id = producer._lineage_task_id_for(0)
    tracker.register_task_submission(seed_task_id, [])
    tracker.register_seed_input(seed_task_id, seed_input)
    lost_task_ids = [consumer._lineage_task_id_for(index) for index in range(2)]
    for output_index, child_task_id in enumerate(lost_task_ids):
        block_id = f"{seed_task_id}/{output_index}"
        tracker.register_block_output(seed_task_id, block_id, output_index)
        tracker.register_block_queued(seed_task_id, output_index)
        tracker.register_task_submission(
            child_task_id, tracker.resolve_dependencies([block_id])
        )

    source_op = producer.input_dependencies[0]
    topology = {source_op: MagicMock(), producer: MagicMock(), consumer: MagicMock()}

    # The first loss re-injects the seed, and the re-execution is submitted.
    _lose(topology, tracker, lost_task_ids[0])
    (first_call,) = topology[source_op].add_output.call_args_list
    producer._submit_data_task(MagicMock(), first_call.args[0])
    assert producer._batched_plans == {}

    # The second loss finds nothing queued to join, so it re-injects the seed again.
    _lose(topology, tracker, lost_task_ids[1])
    first_call, second_call = topology[source_op].add_output.call_args_list
    assert first_call.args[0].reconstruction_stamp == ReconstructionStamp(
        seed_task_id, frozenset({lost_task_ids[0]})
    )
    assert second_call.args[0].reconstruction_stamp == ReconstructionStamp(
        seed_task_id, frozenset({lost_task_ids[1]})
    )
    # The second plan is on the new stamp, and nothing is batched into it yet.
    assert producer._batched_plans == {seed_task_id: set()}


@pytest.mark.parametrize("reads_before_loss", [5, 10])
@reconstruction_enabled
def test_seed_loss_does_not_re_emit_queued_outputs(
    ray_start_regular_shared,
    restore_data_context,
    lose_output,
    reads_before_loss,  # noqa: F405
):
    """The seed loses its output after queueing some blocks; every row arrives once.

    The downstream bundler waits for all 20 rows, so the blocks the seed queued
    before the loss are still waiting when it re-runs. They are already
    downstream, so the re-run must not queue them again.
    """
    restore_data_context.target_max_block_size = 1
    fired = lose_output("ReadRange", reads_before_loss=reads_before_loss)

    rows = (
        ray.data.range(20, override_num_blocks=1)
        .map(lambda row: {"id": row["id"] + 1})
        .map_batches(lambda batch: batch, batch_size=20, concurrency=1)
        .take_all()
    )

    assert fired == [0]
    _assert_every_row_once((row["id"] for row in rows), [i + 1 for i in range(20)])


@pytest.mark.parametrize(
    ("run_plan", "expected"),
    [
        pytest.param(
            lambda: len(ray.data.range(2).map(lambda row: row).limit(1).take_all()),
            1,
            id="limit",
        ),
        pytest.param(
            lambda: len(ray.data.range(1).union(ray.data.range(1)).take_all()),
            2,
            id="union",
        ),
        pytest.param(
            lambda: ray.data.range(10).filter(lambda row: row["id"] < 5).count(),
            5,
            id="count",
        ),
        pytest.param(
            lambda: len(
                ray.data.range(2)
                .repartition(target_num_rows_per_block=1, strict=True)
                .take_all()
            ),
            2,
            id="strict_repartition",
        ),
    ],
)
@reconstruction_enabled
def test_unsupported_plan_disables_reconstruction(
    ray_start_regular_shared, trackers, run_plan, expected  # noqa: F405
):
    """A plan with a non-map operator, or a map operator whose bundler slices blocks
    (strict streaming repartition), runs with lineage reconstruction disabled."""
    assert run_plan() == expected
    assert trackers == []


@reconstruction_enabled
def test_preserve_order_disables_reconstruction(
    ray_start_regular_shared, restore_data_context, trackers  # noqa: F405
):
    """A plan of map operators runs with lineage reconstruction disabled under
    ``preserve_order``: a re-execution gets a fresh task index, so the ordered
    output queue would emit the re-produced rows out of order."""
    restore_data_context.execution_options.preserve_order = True

    rows = ray.data.range(3).map(lambda row: row).take_all()

    assert [row["id"] for row in rows] == list(range(3))
    assert trackers == []


@pytest.mark.parametrize(
    ("num_rows", "num_blocks", "batch_size", "one_row_blocks"),
    [
        pytest.param(20, 2, 20, False, id="fan_in_2_to_1"),
        pytest.param(200, 20, 200, False, id="fan_in_20_to_1"),
        pytest.param(60, 6, 30, False, id="fan_in_6_to_2"),
        pytest.param(20, 1, 1, True, id="fan_out_1_to_20"),
    ],
)
@reconstruction_enabled
def test_lost_child_reconstructs_across_graph_shapes(
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
    # seed's other re-produced outputs are pruned.
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


def test_reconstruction_stamp_survives_schema_divergence():
    """``OpState.add_output`` rebuilds a bundle whose schema diverges from the
    operator's, field by field. The stamp must survive the rebuild, or the
    re-execution is bundled and submitted as a fresh task.
    """
    import pyarrow as pa

    from ray.data._internal.execution.interfaces import RefBundle
    from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry
    from ray.data._internal.execution.streaming_executor_state import (
        dedupe_schemas_with_validation,
    )
    from ray.data.block import BlockMetadata

    stamp = ReconstructionStamp(
        lineage_task_id="child:0",
        reconstruction_plan_ids=frozenset({"plan_a"}),
    )
    meta = BlockMetadata(num_rows=2, size_bytes=16, exec_stats=None, input_files=None)
    # The same column comes back all nulls on the re-run, so it is inferred as null.
    bundle = RefBundle(
        blocks=(
            BlockEntry(
                ray.ObjectRef(b"1" * 28),  # pyrefly: ignore[bad-argument-type]
                meta,
            ),
        ),
        schema=pa.schema([("x", pa.null())]),
        owns_blocks=True,
        reconstruction_stamp=stamp,
    )

    rebuilt, diverged = dedupe_schemas_with_validation(
        pa.schema([("x", pa.int64())]), bundle, enforce_schemas=False
    )

    assert diverged and rebuilt is not bundle
    assert rebuilt.reconstruction_stamp == stamp


def test_reconstruction_input_passes_through_the_bundler(
    ray_start_regular_shared,
):  # noqa: F405
    """A reconstruction input must reach submission exactly as assembled.

    This holds for a child's input set and for a re-injected seed's input, which both
    carry a ``reconstruction_stamp``. ``RebundleQueue`` must emit it as its own ready
    bundle instead of holding it back, merging it with other pending input, or slicing
    it to hit the row target.
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
    bundler = op._block_ref_bundler
    assert bundler is not None

    # Ordinary input waits in the bundler for the row target.
    op._add_input_inner(inputs, 0)
    assert scheduled == []
    assert bundler.num_blocks() == 1

    # A stamped reconstruction input goes straight to submission.
    stamped = replace(
        make_ref_bundles([[2]])[0],
        reconstruction_stamp=ReconstructionStamp(
            lineage_task_id="child:0",
            reconstruction_plan_ids=frozenset({"plan_a"}),
        ),
    )
    op._add_input_inner(stamped, 0)
    assert scheduled == [(stamped, True)]
    assert bundler.num_blocks() == 1


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
        lineage_task_id="seed:0",
    )


def test_mark_aborted_fires_the_done_callback_exactly_once():
    """Abort from ACTIVE runs the done-callback with the error and finishes the task.

    A repeat abort, or an abort after normal completion, must not run it again: that
    would double-release the task's resource reservations.
    """
    calls = []
    task = _abortable_task(
        lambda exc, worker_stats, driver_stats, aborted: calls.append(exc)
    )
    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")

    task.mark_aborted(error)
    task.mark_aborted(error)
    assert calls == [error]
    assert task.has_finished

    calls.clear()
    task = _abortable_task(
        lambda exc, worker_stats, driver_stats, aborted: calls.append(exc)
    )
    task.mark_done()
    task.mark_aborted(error)
    assert calls == [None]


def test_a_drained_task_aborted_before_its_done_callback_fires_it_once():
    """``_fire_done_callbacks`` must skip a drained task that was since aborted."""
    from ray.data._internal.execution.metadata_fetcher import ThreadedMetadataFetcher

    calls = []
    task = _abortable_task(
        lambda exc, worker_stats, driver_stats, aborted: calls.append(exc)
    )
    fetcher = ThreadedMetadataFetcher()
    fetcher._drained_tasks.add(task)
    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
    task.mark_aborted(error)

    fetcher._fire_done_callbacks()

    assert calls == [error]
    assert task not in fetcher._drained_tasks


@pytest.mark.parametrize(
    "lineage_task_id", [None, "seed:0"], ids=["untracked", "tracked"]
)
def test_object_lost_in_task_output_raises_only_when_lineage_tracked(
    monkeypatch, lineage_task_id
):
    """A task's stream failing with ``ObjectLostError`` drains the task like any other
    task error when it is not lineage tracked, so pairs already in flight in the metadata
    fetcher still emit. Only a lineage tracked task re-raises it for reconstruction.
    """
    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
    block_ref = MagicMock()
    block_ref.is_nil.return_value = False
    streaming_gen = MagicMock()
    # The block ref arrives, then the stream stops where its metadata should be.
    streaming_gen._next_sync.side_effect = [block_ref, StopIteration()]

    def raise_lost(ref):
        raise error

    monkeypatch.setattr(ray, "get", raise_lost)
    calls = []
    task = DataOpTask(
        0,
        streaming_gen,
        MagicMock(),  # block_ref_counter
        "test_op",
        task_done_callback=lambda exc, worker_stats, driver_stats, aborted: calls.append(
            exc
        ),
        lineage_task_id=lineage_task_id,
    )
    assert task.is_lineage_tracked is (lineage_task_id is not None)
    metadata_fetcher = MagicMock()

    if task.is_lineage_tracked:
        with pytest.raises(ObjectLostError):
            task.on_data_ready(None, metadata_fetcher)
        metadata_fetcher.in_data_ready_done.assert_not_called()
    else:
        task.on_data_ready(None, metadata_fetcher)
        assert task.task_error is error
        assert task.is_drained()
        metadata_fetcher.in_data_ready_done.assert_called_once_with(task)

    # Not finished either way. The fetcher's `has_finished` skip must not drop
    # in-flight pairs, and the done-callback has not fired.
    assert not task.has_finished
    assert calls == []


@reconstruction_enabled
def test_failed_reconstruction_aborts_the_lost_task_once(
    ray_start_regular_shared, restore_data_context, monkeypatch  # noqa: F405
):
    """A loss whose reconstruction fails must abort its task only once.

    With a tracker, ``on_data_ready`` leaves the lost task ACTIVE for the executor
    to abort. A failure path that skips the abort leaves the task polled every
    tick, so the same loss is counted again until ``max_errored_blocks`` runs out.
    """
    # Bounds the run if the task is never aborted: the same loss would be counted
    # again every tick and, with an unlimited budget, spin forever.
    DataContext.get_current().max_errored_blocks = 1
    reconstruction_attempts = []

    def fail_reconstruction(topology, lineage_tracker, state, task, lost_error):
        reconstruction_attempts.append(task.task_index())
        raise LineageReconstructionError(lost_error, "failed by test")

    def lost_output(self, max_bytes_to_read, metadata_fetcher):
        # A lost output stays lost: every read raises until the executor stops
        # polling the task.
        raise ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")

    monkeypatch.setattr(
        "ray.data._internal.execution.streaming_executor_state."
        "_reconstruct_lost_object",
        fail_reconstruction,
    )
    monkeypatch.setattr(DataOpTask, "on_data_ready", lost_output)

    abort_errors = []
    original_mark_aborted = DataOpTask.mark_aborted

    def recording_mark_aborted(self, exception, aborted_for_reconstruction=False):
        abort_errors.append(exception)
        original_mark_aborted(self, exception, aborted_for_reconstruction)

    monkeypatch.setattr(DataOpTask, "mark_aborted", recording_mark_aborted)

    ray.data.range(1).take_all()

    # The executor aborted the lost task once, with the reconstruction error.
    assert [type(error) for error in abort_errors] == [LineageReconstructionError]
    # The task was re-attempted once.
    assert reconstruction_attempts == [0]


def test_started_reconstruction_aborts_for_reconstruction():
    """A task whose lost output is being reconstructed is aborted for
    reconstruction, keeping the lost-object error."""
    calls = []
    task = _abortable_task(lambda *args: calls.append(args))
    tracker = MagicMock()
    tracker.register_task_failed.return_value = (set(), "plan_a")
    lost_error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")

    _reconstruct_lost_object({}, tracker, MagicMock(), task, lost_error)

    assert task.has_finished
    assert calls == [(lost_error, None, None, True)]


def test_reconstruction_error_wraps_object_lost_error():
    """An ``ObjectLostError`` triggers a lineage reconstruction attempt that fails.

    Test that the LineageReconstructionError carries both the original ObjectLostError
    and the reason for the reconstruction failure.
    """
    object_lost_error = ObjectLostError(
        ray.ObjectRef.nil().hex(), b"owner", "injected by test"
    )
    lost_output_task = _abortable_task(
        lambda exc, worker_stats, driver_stats, aborted: None
    )
    op_state = MagicMock()
    op_state.op.name = "test_op"

    # A fresh tracker has never seen the task, so reconstruction cannot start.
    with pytest.raises(LineageReconstructionError) as raised:
        _reconstruct_lost_object(
            {}, LineageTracker(), op_state, lost_output_task, object_lost_error
        )
    reconstruction_error = raised.value

    assert isinstance(reconstruction_error, ObjectLostError)
    assert reconstruction_error.lost_error is object_lost_error
    assert reconstruction_error.__cause__ is object_lost_error
    for field in ("object_ref_hex", "owner_address", "call_site"):
        assert getattr(reconstruction_error, field) == getattr(object_lost_error, field)
    assert str(object_lost_error) in str(reconstruction_error)
    assert "not registered with the lineage graph" in str(reconstruction_error)

    unpickled_error = pickle.loads(pickle.dumps(reconstruction_error))
    assert type(unpickled_error.lost_error) is type(object_lost_error)
    assert str(unpickled_error) == str(reconstruction_error)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
