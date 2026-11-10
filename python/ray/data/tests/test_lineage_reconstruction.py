"""Integration tests of lineage reconstruction with the Ray Data streaming executor.
"""

import collections
from typing import TYPE_CHECKING, Callable, Dict, List, Sequence
from unittest.mock import MagicMock

import pytest

import ray
from ray.data import ActorPoolStrategy, Dataset
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

# Four CPUs, so seed and child tasks run at the same time and the end-to-end tests see
# more than one ordering.
reconstruction_enabled = pytest.mark.parametrize(
    "ray_start_regular_shared",
    [
        {
            "num_cpus": 4,
            "job_config": JobConfig(_disable_job_level_lineage_reconstruction=True),
        }
    ],
    indirect=True,
    ids=["data_reconstruction_on"],
)


# ---------------------------------------------------------------------------
# Operator tests
# ---------------------------------------------------------------------------


def _nodes(tracker):
    return tracker._lineage_task_id_to_task_node


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


def test_shared_child_is_batched_until_it_is_submitted(
    ray_start_regular_shared,  # noqa: F405
):
    """A child reads from two seeds, and two leaves under it are lost.

    The second plan joins the first plan's queued reconstruction task of one seed,
    but the other seed re-runs once per plan. So the child's input is complete for
    the first plan before the second, and the child is released for the first plan
    alone.

    That reconstruction task of the child is still queued when the second plan's
    input completes, so the second plan is batched into it and the child runs once
    for both plans.
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

    # The second seed's re-run for the second plan completes the child's input for
    # that plan.
    (second_seed_b,) = _produced_bundles([[2]])
    _run_to_completion(
        producer,
        producer.stamp_seed_reconstruction_task(seed_task_ids[1], {plan_b}, seed_input),
        [second_seed_b],
    )
    assert producer._reconstruction_outputs == {}
    # The child is still queued, so the second plan is batched into it.
    assert not producer.has_next()
    assert consumer._batched_plans == {child_task_id: {plan_b}}
    consumer._submit_data_task(MagicMock(), released_a)
    assert consumer._batched_plans == {}
    (child_task,) = consumer._data_tasks.values()
    assert child_task.reconstruction_plan_ids == frozenset({plan_a, plan_b})


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


def test_lost_reconstruction_task_is_re_traced_under_its_plan(
    ray_start_regular_shared,  # noqa: F405
):
    """A consumer's reconstruction task loses its input too.

    The second loss is traced under the plan the task already serves, so the seed is
    re-injected for that plan rather than for a new plan keyed by the consumer.
    """
    producer, consumer, seed_input, tracker = _seed_and_consumer_ops()

    # First run: the seed feeds the consumer, which feeds a leaf.
    seed_task_id = producer._lineage_task_id_for(0)
    consumer_task_id = consumer._lineage_task_id_for(0)
    leaf_task_id = "leaf:0"
    tracker.register_task_submission(seed_task_id, [])
    tracker.register_seed_input(seed_task_id, seed_input)
    tracker.register_block_output(seed_task_id, "seed/0", 0)
    tracker.register_block_queued(seed_task_id, 0)
    tracker.register_task_submission(
        consumer_task_id, tracker.resolve_dependencies(["seed/0"])
    )
    tracker.register_block_output(consumer_task_id, "consumer/0", 0)
    tracker.register_block_queued(consumer_task_id, 0)
    tracker.register_task_submission(
        leaf_task_id, tracker.resolve_dependencies(["consumer/0"])
    )

    # The leaf is lost. The seed re-runs and releases the consumer's input, and the
    # consumer's reconstruction task is submitted under the leaf's plan.
    source_op = producer.input_dependencies[0]
    topology = {source_op: MagicMock(), producer: MagicMock(), consumer: MagicMock()}
    _lose(topology, tracker, leaf_task_id)
    (first_call,) = topology[source_op].add_output.call_args_list
    _run_to_completion(producer, first_call.args[0], _produced_bundles([[1]]))
    (released,) = _drain(producer)
    consumer._submit_data_task(MagicMock(), released)
    (consumer_task,) = consumer._data_tasks.values()
    assert consumer_task.reconstruction_plan_ids == frozenset({leaf_task_id})

    # The consumer's reconstruction task loses its input as well.
    _reconstruct_lost_object(
        topology,
        tracker,
        MagicMock(),
        consumer_task,
        ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test"),
    )

    # The seed is re-injected again, under the leaf's plan.
    _, second_call = topology[source_op].add_output.call_args_list
    assert second_call.args[0].reconstruction_stamp == ReconstructionStamp(
        seed_task_id, frozenset({leaf_task_id})
    )


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


# ---------------------------------------------------------------------------
# End-to-end tests
# ---------------------------------------------------------------------------

NUM_ROWS = 100


@pytest.fixture
def raise_original_map_exception(restore_data_context):
    """Let the ``ObjectLostError`` a UDF raises reach the executor as is.

    By default Ray Data wraps a UDF error in ``UserCodeException``, which the executor
    treats as a user error rather than a lost block.
    """
    restore_data_context.raise_original_map_exception = True


@ray.remote(num_cpus=0)
class LossInjector:
    """Fails the first batch that contains each row in ``rows``.

    A row listed twice fails twice, once in its first run and once in its re-run. A
    batch fails for at most one row. If two listed rows share a batch, the second one
    fails the re-run of that batch, so ``fired`` still lists both.
    """

    def __init__(self, rows: Sequence[int]):
        self._losses_left = collections.Counter(rows)
        self._fired: List[int] = []

    def should_lose(self, ids: List[int]) -> bool:
        for row in ids:
            if self._losses_left[row] > 0:
                self._losses_left[row] -= 1
                self._fired.append(row)
                return True
        return False

    def fired(self) -> List[int]:
        return sorted(self._fired)


def lose_on(injector) -> Callable:
    """A ``map_batches`` UDF that fails with ``ObjectLostError`` when ``injector`` says to."""

    def lose(batch):
        if ray.get(injector.should_lose.remote(batch["id"].tolist())):
            raise ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
        return batch

    return lose


def assert_every_row_once(ids: List[int]):
    counts = collections.Counter(ids)
    duplicated = sorted(row for row, count in counts.items() if count > 1)
    missing = sorted(set(range(NUM_ROWS)) - set(counts))
    assert sorted(ids) == list(
        range(NUM_ROWS)
    ), f"duplicated={duplicated}, missing={missing}"


# Each lossy map asks for a different `num_cpus` than the operator before it, so the two
# stay separate operators instead of fusing into one.


def _linear(injector) -> Dataset:
    """Ten read tasks, each feeding one lossy task."""
    return ray.data.range(NUM_ROWS, override_num_blocks=10).map_batches(
        lose_on(injector), batch_size=10, num_cpus=0.5
    )


def _fan_in(injector) -> Dataset:
    """Ten read tasks, five of them feeding each lossy task."""
    return ray.data.range(NUM_ROWS, override_num_blocks=10).map_batches(
        lose_on(injector), batch_size=50, num_cpus=0.5
    )


def _fan_out(injector) -> Dataset:
    """One read task of one-row blocks, feeding ten lossy tasks.

    Sets ``target_max_block_size=1`` to get one-row blocks.
    """
    DataContext.get_current().target_max_block_size = 1
    return ray.data.range(NUM_ROWS, override_num_blocks=1).map_batches(
        lose_on(injector), batch_size=10, num_cpus=0.5
    )


def _three_operators(injector) -> Dataset:
    """Read tasks feed a middle map, which feeds the lossy map."""
    return (
        ray.data.range(NUM_ROWS, override_num_blocks=10)
        .map_batches(lambda batch: batch, batch_size=20, num_cpus=0.5)
        .map_batches(lose_on(injector), batch_size=50, num_cpus=0.25)
    )


def _one_row_outputs(injector) -> Dataset:
    """Lossy tasks that each read one 10-row input block and yield one-row blocks.

    So a task fails after it already yielded the rows before the lost one. Sets
    ``target_max_block_size=1`` to get one-row output blocks. The input blocks come
    from ``from_items``, which keeps them at 10 rows.
    """
    DataContext.get_current().target_max_block_size = 1
    items = [{"id": row} for row in range(NUM_ROWS)]
    return ray.data.from_items(items, override_num_blocks=10).map_batches(
        lose_on(injector), batch_size=1
    )


def _actor_pool(injector) -> Dataset:
    """The linear shape with the lossy UDF on a pool of two actors."""
    lose = lose_on(injector)

    class Lose:
        def __call__(self, batch):
            return lose(batch)

    return ray.data.range(NUM_ROWS, override_num_blocks=10).map_batches(
        Lose, batch_size=10, compute=ActorPoolStrategy(size=2), num_cpus=0.5
    )


@pytest.mark.parametrize(
    ("pipeline", "lost_rows"),
    [
        pytest.param(_linear, [45], id="linear"),
        pytest.param(_fan_in, [45], id="fan_in"),
        pytest.param(_fan_out, [45], id="fan_out"),
        pytest.param(_fan_out, [15, 75], id="two_losses_under_one_seed"),
        pytest.param(_linear, [45, 45], id="lost_again_while_reconstructing"),
        pytest.param(_three_operators, [45], id="three_operators"),
        pytest.param(_one_row_outputs, [45], id="lost_after_yielding_outputs"),
        pytest.param(_actor_pool, [45], id="actor_pool"),
    ],
)
@reconstruction_enabled
def test_lineage_reconstruction_delivers_every_row_once(
    ray_start_regular_shared, raise_original_map_exception, pipeline, lost_rows
):
    injector = LossInjector.remote(lost_rows)  # pyrefly: ignore[missing-attribute]

    rows = pipeline(injector).take_all()

    assert ray.get(injector.fired.remote()) == sorted(lost_rows)
    assert_every_row_once([row["id"] for row in rows])


@reconstruction_enabled
def test_write_after_a_lost_block_writes_every_row_once(
    ray_start_regular_shared, raise_original_map_exception, tmp_path
):
    """A write operator runs after the reconstructed one, so its files hold the rows."""
    injector = LossInjector.remote([45])  # pyrefly: ignore[missing-attribute]

    _linear(injector).write_parquet(str(tmp_path))

    assert ray.get(injector.fired.remote()) == [45]
    rows = ray.data.read_parquet(str(tmp_path)).take_all()
    assert_every_row_once([row["id"] for row in rows])


# One plan per branch of the gate that decides whether lineage reconstruction runs.


def _strict_repartition(injector):
    """A strict repartition, whose bundler slices blocks across tasks."""
    _linear(injector).repartition(target_num_rows_per_block=7, strict=True).take_all()


def _preserve_order(injector):
    """A plan of map operators that must keep its output order."""
    DataContext.get_current().execution_options.preserve_order = True
    _linear(injector).take_all()


def _streaming_split(injector):
    """A ``streaming_split``, the plan Ray Train ingest runs.

    Its ``OutputSplitter`` is not a map operator, so the gate refuses the plan whatever
    the split count. A union, limit or sort is refused the same way.
    """
    (split,) = _linear(injector).streaming_split(1)
    list(split.iter_rows())


@pytest.mark.parametrize(
    "run_plan", [_strict_repartition, _preserve_order, _streaming_split]
)
@reconstruction_enabled
def test_unsupported_plan_fails_on_lost_block(
    ray_start_regular_shared, raise_original_map_exception, run_plan
):
    """A plan lineage reconstruction doesn't support runs without it, so a lost block
    fails the dataset."""
    injector = LossInjector.remote([45])  # pyrefly: ignore[missing-attribute]

    with pytest.raises(ObjectLostError) as raised:
        run_plan(injector)

    # A `LineageReconstructionError` is an `ObjectLostError` too. It would mean the plan
    # was admitted and its reconstruction failed.
    assert not isinstance(raised.value, LineageReconstructionError)
    assert ray.get(injector.fired.remote()) == [45]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
