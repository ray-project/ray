"""Integration tests for experimental object-loss lineage recovery.

These cover the *wiring* between the streaming executor and
:class:`~ray.data._internal.execution.lineage_tracker.LineageTracker`. The tracker
itself is unit-tested in ``test_lineage_tracker_{linear,fan_in,fan_out}.py``; what is
tested here is that real execution actually calls it, and calls it with arguments that
build a usable graph.

That distinction matters: the tracker's units passed for the entire period during which
the executor called a set of methods that no longer existed, because nothing exercised
the two together.
"""

from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionOptions
from ray.data._internal.execution.lineage_tracker import (
    LineageTracker,
    ParentBlockOutput,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.data.tests.util import create_map_transformer_from_block_fn
from ray.exceptions import ObjectLostError
from ray.tests.conftest import *  # noqa: F401, F403


@pytest.fixture
def trackers(monkeypatch):
    """Capture every ``LineageTracker`` the executor builds during a test.

    The executor owns its tracker privately, so this is the seam for asserting on
    what execution actually recorded.
    """
    created = []
    original_init = LineageTracker.__init__

    def spy(self):
        original_init(self)
        created.append(self)

    monkeypatch.setattr(LineageTracker, "__init__", spy)
    return created


@pytest.fixture
def recovery_enabled(restore_data_context):  # noqa: F405
    ctx = ray.data.DataContext.get_current()
    ctx.enable_seed_input_lineage_recovery = True
    return ctx


def _nodes(tracker):
    return tracker._data_task_id_to_task_node


def _parents_of(tracker, data_task_id):
    return [p.data_task_id for p in _nodes(tracker)[data_task_id].parent_tasks]


@pytest.fixture
def loss_in_operator(monkeypatch):
    """Fail task 0 of a *named* operator with ``ObjectLostError``.

    ``object_loss_injector`` fires on whichever operator reaches task 0 first, which
    is fine when there is only one candidate but ambiguous in a longer chain. This
    picks the victim explicitly so a multi-operator test is deterministic.
    """
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask

    state: Dict[str, Any] = {
        "fired": False,
        "reads_seen": 0,
        "victim": "",
        "reads_before_loss": 0,
    }
    original_on_data_ready = DataOpTask.on_data_ready

    def flaky_on_data_ready(self, max_bytes_to_read, metadata_fetcher):
        name = getattr(self, "_operator_name", "")
        if not state["fired"] and self.task_index() == 0 and state["victim"] in name:
            if state["reads_seen"] >= state["reads_before_loss"]:
                state["fired"] = True
                raise ObjectLostError(
                    ray.ObjectRef.nil().hex(), None, "injected by test"
                )
            state["reads_seen"] += 1
        return original_on_data_ready(self, max_bytes_to_read, metadata_fetcher)

    monkeypatch.setattr(DataOpTask, "on_data_ready", flaky_on_data_ready)

    def arm(victim, reads_before_loss=0):
        state["victim"] = victim
        state["reads_before_loss"] = reads_before_loss
        return state

    state["arm"] = arm
    return state


@pytest.fixture
def losses_in_operator(monkeypatch):
    """Fail several tasks of a *named* operator with ``ObjectLostError``, each once.

    ``loss_in_operator`` fires on task 0 only. This arms a set of task indices, so
    several plans open under one execution and trace back to the same seed.
    """
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask

    state: Dict[str, Any] = {"victim": None, "pending": set(), "fired": []}
    original_on_data_ready = DataOpTask.on_data_ready

    def flaky_on_data_ready(self, max_bytes_to_read, metadata_fetcher):
        name = getattr(self, "_operator_name", "")
        if (
            state["victim"] is not None
            and state["victim"] in name
            and self.task_index() in state["pending"]
        ):
            state["pending"].discard(self.task_index())
            state["fired"].append(self.task_index())
            raise ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
        return original_on_data_ready(self, max_bytes_to_read, metadata_fetcher)

    monkeypatch.setattr(DataOpTask, "on_data_ready", flaky_on_data_ready)

    def arm(victim, task_indices):
        state["victim"] = victim
        state["pending"] = set(task_indices)
        return state

    state["arm"] = arm
    return state


def test_flag_off_builds_no_tracker(ray_start_regular_shared, trackers):  # noqa: F405
    """With the feature off, execution must not touch the lineage machinery."""
    result = ray.data.range(50, override_num_blocks=4).map(
        lambda row: {"id": row["id"]}
    )
    assert result.count() == 50
    assert trackers == []


def test_execution_registers_a_connected_graph(
    ray_start_regular_shared,
    recovery_enabled,
    trackers,  # noqa: F405
):
    """The regression test for the wiring.

    Every data task must register, and downstream tasks must end up with real
    parents. If ``dependencies`` were ever empty, every task would look like a chain
    root and ``register_task_failed`` would return the failed task as its own seed --
    silently unrecoverable.
    """
    rows = (
        ray.data.range(100, override_num_blocks=4)
        .map(lambda row: {"id": row["id"] * 2})
        .take_all()
    )
    assert sorted(r["id"] for r in rows) == [i * 2 for i in range(100)]

    assert len(trackers) == 1, "expected exactly one tracker per execution"
    nodes = _nodes(trackers[0])
    assert nodes, "no tasks were registered with the lineage graph"

    # Read and map fuse into one operator here, so every task is a seed. What matters
    # is that ids are well formed and the graph is internally consistent.
    for data_task_id, node in nodes.items():
        assert ":" in data_task_id, f"malformed logical id {data_task_id!r}"
        for child in node.child_tasks:
            assert child.data_task_id in node.child_task_block_dependencies


def test_unfused_chain_records_parent_edges(
    ray_start_regular_shared,
    recovery_enabled,
    trackers,  # noqa: F405
):
    """A task consuming another task's output must record that dependency."""
    # An actor-backed stage does not fuse with the preceding task-backed one, so the
    # second operator's tasks genuinely consume blocks the first operator produced.
    # It also exercises `ActorPoolMapOperator`'s submission path, where the bundle is
    # queued and submitted later.
    rows = (
        ray.data.range(60, override_num_blocks=3)
        .map(lambda row: {"id": row["id"] + 1})
        .map_batches(lambda batch: batch, concurrency=1)
        .take_all()
    )
    assert sorted(r["id"] for r in rows) == [i + 1 for i in range(60)]

    tracker = trackers[0]
    with_parents = [
        task_id for task_id in _nodes(tracker) if _parents_of(tracker, task_id)
    ]
    assert with_parents, (
        "no task recorded a parent; block attribution is not reaching "
        "register_task_submission"
    )
    for task_id in with_parents:
        for parent_id in _parents_of(tracker, task_id):
            # The edge must be navigable in both directions.
            assert task_id in _nodes(tracker)[parent_id].child_task_block_dependencies


def test_reconstruction_does_not_grow_a_duplicate_node(
    ray_start_regular_shared,  # noqa: F405
    recovery_enabled,
    trackers,
    loss_in_operator,
):
    """The re-executed child reuses its original id, through real execution.

    The tracker-level version of this passes even when production is broken. What it
    cannot see is that the parent completes first, discharging the plan. If the child
    then falls back to a lookup it finds nothing and registers as a new task, leaving
    the parent listing two consumers for one output -- so the next failure computes
    pruning against a doubled map, and the original node is never cleaned up.

    Asserted as a graph shape rather than a specific id: two nodes standing for the
    same logical task are exactly two nodes with the same parents.
    """
    loss_in_operator["arm"]("MapBatches")

    rows = (
        ray.data.range(60, override_num_blocks=6)
        .map(lambda row: {"id": row["id"] + 1})
        # An actor stage keeps this from fusing into the read, so the lost output
        # belongs to a task with a real registered parent.
        .map_batches(lambda batch: batch, concurrency=1)
        .take_all()
    )

    assert loss_in_operator["fired"], "the injected loss never triggered"
    assert sorted(r["id"] for r in rows) == sorted(i + 1 for i in range(60))

    (tracker,) = [t for t in trackers if _nodes(t)]
    signatures = [
        (data_task_id.rsplit(":", 1)[0], tuple(_parents_of(tracker, data_task_id)))
        for data_task_id in _nodes(tracker)
    ]
    duplicates = [
        signature
        for signature in signatures
        # A seed has no parents, so an empty signature says nothing about identity.
        if signature[1] and signatures.count(signature) > 1
    ]
    assert not duplicates, f"reconstruction grew a duplicate node: {duplicates}"


def test_only_the_minting_operator_owns_a_data_task_id(
    ray_start_regular_shared,  # noqa: F405
):
    """The id format is private to ``MapOperator``; everyone else asks the owner.

    Recovery resolves a seed id or a child id back to an operator by asking each
    operator, never by parsing the id, so a format change cannot silently break
    the lookups.
    """
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.util import make_ref_bundles

    ctx = DataContext.get_current()
    seed_input = make_ref_bundles([[1]])[0]
    source = InputDataBuffer(ctx, [seed_input])
    first = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=source,
        data_context=ctx,
        name="First",
    )
    second = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=first,
        data_context=ctx,
        name="Second",
    )

    task_id = first._data_task_id_for(7)
    assert first.owns_data_task(task_id)
    assert not second.owns_data_task(task_id)
    assert not source.owns_data_task(task_id)

    assert first.retained_seed_input(task_id) is None
    first._seed_task_inputs[task_id] = seed_input
    assert first.retained_seed_input(task_id) is seed_input
    assert second.retained_seed_input(task_id) is None
    assert source.retained_seed_input(task_id) is None


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
                [
                    BlockEntry(
                        ray.put(block),
                        BlockAccessor.for_block(block).get_metadata(
                            block_exec_stats=exec_stats
                        ),
                    )
                ],
                owns_blocks=True,
                schema=pa.lib.Schema.from_pandas(block, preserve_index=False),
            )
        )
    return bundles


def test_merged_bundle_still_resolves():
    """A bundler merge must not hide the block of interest.

    ``RebundleQueue`` parks zero-row bundles and prepends them on the next merge, so
    a task's inputs can begin with an unattributed block. Anything that looked only
    at ``block_refs[0]`` would miss, treat the task as fresh, and silently skip
    classification -- re-emitting rows the consumer already received.
    """
    tracker = LineageTracker()
    tracker.register_task_submission("seed:0", [])
    tracker.register_output("seed:0", "real_block", 0)

    dependencies = tracker.resolve_dependencies(["empty_block", "real_block"])
    assert dependencies == [
        ParentBlockOutput(parent_data_task_id="seed:0", output_index=0)
    ]


def test_resolved_dependencies_are_consumed_once():
    """Entries are popped, so the map tracks in-flight blocks, not the whole run."""
    tracker = LineageTracker()
    tracker.register_task_submission("seed:0", [])
    tracker.register_output("seed:0", "block_a", 0)

    assert tracker.resolve_dependencies(["block_a"]) == [
        ParentBlockOutput(parent_data_task_id="seed:0", output_index=0)
    ]
    assert tracker.resolve_dependencies(["block_a"]) == []


@pytest.fixture
def object_loss_injector(monkeypatch):
    """Fail one task output read with ``ObjectLostError``.

    This is the trigger a dead node would pull. Everything downstream of it -- the
    recovery branch in ``process_completed_tasks``, seed re-injection, re-execution
    under the original logical id, and per-output classification -- runs for real.

    ``arm(reads_before_loss=N)`` lets the task emit N outputs first, so the loss can
    be placed either before it has produced anything or after some of its outputs
    have already been consumed downstream.
    """
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask

    state: Dict[str, Any] = {"fired": False, "reads_before_loss": 0, "reads_seen": 0}
    original_on_data_ready = DataOpTask.on_data_ready

    def flaky_on_data_ready(self, max_bytes_to_read, metadata_fetcher):
        if not state["fired"] and self.task_index() == 0:
            if state["reads_seen"] >= state["reads_before_loss"]:
                state["fired"] = True
                raise ObjectLostError(
                    ray.ObjectRef.nil().hex(), None, "injected by test"
                )
            state["reads_seen"] += 1
        return original_on_data_ready(self, max_bytes_to_read, metadata_fetcher)

    monkeypatch.setattr(DataOpTask, "on_data_ready", flaky_on_data_ready)

    def arm(reads_before_loss=0):
        state["reads_before_loss"] = reads_before_loss
        return state

    state["arm"] = arm
    return state


def _assert_matches_baseline(rows, expected):
    actual = sorted(row["id"] for row in rows)
    duplicates = len(actual) - len(set(actual))
    assert actual == expected, (
        f"expected {len(expected)} rows, got {len(actual)} "
        f"({len(actual) - len(expected):+d}), duplicates={duplicates}"
    )


# Pruning asks "does a copy of these rows outlive the node that produced them?",
# and only one of the two ways that can be true is recorded:
#
#   read by a downstream task -> fetched to that task's node. Recorded, via that
#       task's submission, in `child_task_block_dependencies`.
#   fetched by a consumer     -> `ray.get` copies the rows into the driver, out of
#       the object store. NOT recorded -- the consumer is not a task, so it never
#       registers a submission. This is the gap below.
#
# And one case that is easy to mistake for those two but is not durable at all: an
# output sitting in a downstream operator's queue. That queue holds an `ObjectRef`,
# so the only copy is still on the producing node. It dies with it, and re-emitting
# is required rather than duplicative.
# That gap is not what ``reads_before_loss=1`` exercises, despite once being marked
# xfail for it: at the point the loss fires, the earlier pair has been pulled but is
# still in flight in the ``ThreadedMetadataFetcher``, not yet delivered to anyone. Its
# duplicate came from that in-flight emit landing *after* the task was aborted, which
# `_emit_ready` now drops. A row genuinely consumed by the driver before the loss would
# still be re-emitted; that case has no coverage here.


@pytest.mark.parametrize("reads_before_loss", [0, 1])
def test_recovery_output_matches_a_no_loss_baseline(
    ray_start_regular_shared,  # noqa: F405
    recovery_enabled,
    object_loss_injector,
    reads_before_loss,
):
    """The correctness bar: recovery reproduces the baseline exactly.

    Not a subset and not a superset. Missing rows mean the reconstruction failed;
    duplicate rows mean an output that was never lost was re-emitted, which is the
    specific failure ``OBJECT_PRUNED`` exists to prevent.

    ``reads_before_loss=0`` loses the output before the task has produced anything.
    ``reads_before_loss=1`` loses it after an earlier read already delivered outputs
    downstream, which is where pruning has to do its job. A task's generator is
    drained over exactly two ``on_data_ready`` calls here, so those are the two
    distinct positions available.
    """
    object_loss_injector["arm"](reads_before_loss)
    expected = sorted(i * 2 for i in range(100))

    rows = (
        ray.data.range(100, override_num_blocks=4)
        .map(lambda row: {"id": row["id"] * 2})
        .take_all()
    )

    assert object_loss_injector["fired"], "the injected loss never triggered"
    _assert_matches_baseline(rows, expected)


def test_recovery_across_two_operators_matches_baseline(
    ray_start_regular_shared,  # noqa: F405
    recovery_enabled,
    object_loss_injector,
):
    """Recovery through a real multi-operator chain.

    With an actor-backed second stage the operators do not fuse, so the first
    operator's outputs are consumed by *registered* downstream tasks. That is what
    makes ``OBJECT_PRUNED`` reachable: already-consumed outputs must not be
    re-delivered when the producer re-runs.
    """
    object_loss_injector["arm"](1)
    expected = sorted(i + 1 for i in range(60))

    rows = (
        ray.data.range(60, override_num_blocks=3)
        .map(lambda row: {"id": row["id"] + 1})
        .map_batches(lambda batch: batch, concurrency=1)
        .take_all()
    )

    assert object_loss_injector["fired"], "the injected loss never triggered"
    _assert_matches_baseline(rows, expected)


def test_two_losses_under_one_seed_match_baseline(
    ray_start_regular_shared,  # noqa: F405
    recovery_enabled,
    losses_in_operator,
):
    """Two lost children of one seed, through real execution, reproduce the baseline.

    Tiny output blocks make the single fused read task fan out to several actor-stage
    tasks, and two of those lose their output. Whether the second plan joins the
    first's queued re-injection or arrives after it dispatched depends on timing, so
    the shared re-run itself is pinned by the operator-level tests above; this holds
    the correctness bar for either outcome: every row exactly once.
    """
    recovery_enabled.target_max_block_size = 1
    losses_in_operator["arm"]("MapBatches", {0, 1})
    expected = sorted(i + 1 for i in range(20))

    rows = (
        ray.data.range(20, override_num_blocks=1)
        .map(lambda row: {"id": row["id"] + 1})
        .map_batches(lambda batch: batch, batch_size=5, concurrency=1)
        .take_all()
    )

    assert sorted(losses_in_operator["fired"]) == [
        0,
        1,
    ], f"expected both injected losses to fire, got {losses_in_operator['fired']}"
    _assert_matches_baseline(rows, expected)


def _task_counts_per_op(tracker):
    """How many data tasks each operator registered, as a sorted multiset.

    Operator ids are fresh uuid4s per execution, so two runs of the same pipeline
    cannot be compared by id -- only by shape.
    """
    counts: Dict[str, int] = {}
    for data_task_id in _nodes(tracker):
        op_id = data_task_id.rsplit(":", 1)[0]
        counts[op_id] = counts.get(op_id, 0) + 1
    return sorted(counts.values())


# 6 blocks of 10 rows each. 30 divides evenly, so the bundler merges three whole
# blocks per child; 25 does not, so it slices the third and carries the remainder into
# the next child -- and a slice shares its block ref with the remainder, which
# `resolve_dependencies` pops on first use, so the graph records the whole output index
# for the first consumer and nothing at all for the second.
@pytest.mark.parametrize("batch_size", [30, 25])
def test_fan_in_child_recovers_against_its_whole_input_set(
    ray_start_regular_shared,  # noqa: F405
    recovery_enabled,
    trackers,
    loss_in_operator,
    batch_size,
):
    """End-to-end coverage of a fan-in child being reconstructed.

    ``map_batches(batch_size=...)`` makes the second operator's tasks fan in: its
    ``RebundleQueue`` merges several upstream outputs into one task input. So the lost
    task has multiple registered parents, every one of them is traced and re-run, and
    their re-produced blocks become available one completing parent at a time -- which
    is the case the readiness check in ``_release_reconstruction_children`` exists
    to serialize.

    Note on what this does and does not prove. It is *not* a witness for the bug the
    holding mechanism fixes: it passes against the previous implementation too, because
    the consumer's bundler happens to regroup the re-produced blocks into the same sets
    it formed the first time, so releasing them one at a time coincidentally worked
    here. The cases where that coincidence breaks down -- reconstruction blocks
    interleaving with fresh ones, or with a second plan's -- are not deterministically
    reachable from this level, so the regression weight sits on
    ``test_reconstruction_input_bypasses_the_bundler`` and
    ``test_release_hands_over_the_whole_input_set_once``. What this test does is hold the
    end-to-end line: rows equal to a no-loss baseline, and a task graph that has not
    grown, through the real executor.
    """
    expected = sorted(i + 1 for i in range(60))

    def run():
        return (
            ray.data.range(60, override_num_blocks=6)
            .map(lambda row: {"id": row["id"] + 1})
            .map_batches(lambda batch: batch, batch_size=batch_size, concurrency=1)
            .take_all()
        )

    # Baseline first. The injector matches on a substring of the operator name, and an
    # empty victim would match everything, so aim it somewhere that cannot exist.
    loss_in_operator["arm"]("__no_such_operator__")
    _assert_matches_baseline(run(), expected)
    assert not loss_in_operator["fired"], "the baseline run was supposed to be clean"

    loss_in_operator["arm"]("MapBatches")
    rows = run()

    assert loss_in_operator["fired"], "the injected loss never triggered"
    _assert_matches_baseline(rows, expected)

    baseline_tracker, recovered_tracker = [t for t in trackers if _nodes(t)]
    # The reconstruction re-runs tasks under their original ids, so recovery must not
    # add nodes. A child submitted per-parent instead of once would.
    assert _task_counts_per_op(recovered_tracker) == _task_counts_per_op(
        baseline_tracker
    ), "recovery submitted tasks the no-loss run did not"
    assert any(
        len(_parents_of(recovered_tracker, task_id)) > 1
        for task_id in _nodes(recovered_tracker)
    ), "no task fanned in, so this ran without exercising a multi-parent child"


def _fan_in_op_under_reconstruction(ctx, num_parents=2):
    """A producer operator mid-plan, with a fan-in child owed one block per parent.

    Returns the operator, its downstream consumer, the tracker, the plan id, the
    child's data task id and the parents' re-produced bundles (in dependency order).
    """
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.util import make_ref_bundles

    bundles = make_ref_bundles([[index] for index in range(num_parents)])
    producer = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=InputDataBuffer(ctx, list(bundles)),
        data_context=ctx,
        name="Producer",
    )
    consumer = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=producer,
        data_context=ctx,
        name="Consumer",
    )

    tracker = LineageTracker()
    producer._lineage_tracker = tracker
    consumer._lineage_tracker = tracker

    parent_ids = [f"{producer.id}:{index}" for index in range(num_parents)]
    for parent_id, bundle in zip(parent_ids, bundles):
        tracker.register_task_submission(parent_id, [])
        tracker.register_output(parent_id, bundle.block_refs[0].hex(), 0)

    # The child fans in over one output block from each parent.
    child_task_id = f"{consumer.id}:0"
    tracker.register_task_submission(
        child_task_id,
        tracker.resolve_dependencies(
            [bundle.block_refs[0].hex() for bundle in bundles]
        ),
    )

    seeds, plan_id = tracker.register_task_failed(child_task_id)
    assert seeds == sorted(parent_ids)
    for parent_id in parent_ids:
        tracker.register_task_submission(parent_id, [])

    return producer, consumer, tracker, plan_id, child_task_id, parent_ids, bundles


def test_release_hands_over_the_whole_input_set_once(
    ray_start_regular_shared,  # noqa: F405
):
    """The core of scheduling a reconstruction child: one bundle, in order, once.

    Each parent's re-produced output is withheld as it is generated. The parent that
    completes last is the one holding the child's whole input set, and it
    hands the assembled set downstream as a single bundle -- in the order the child's
    first attempt consumed it, stamped so the re-execution is registered under the
    child's original id rather than minting a new node.
    """
    ctx = DataContext.get_current()
    (
        producer,
        consumer,
        tracker,
        plan_id,
        child_task_id,
        parent_ids,
        bundles,
    ) = _fan_in_op_under_reconstruction(ctx)
    producer.start(ExecutionOptions(), noop_counter())  # noqa: F405

    # Only the first parent has re-produced its block so far, so the child's input
    # set is incomplete and nothing may be released.
    producer._reconstruction_outputs[plan_id] = {(parent_ids[0], 0): bundles[0]}
    producer._release_reconstruction_children(parent_ids[0], plan_id, task_index=0)
    assert not producer.has_next(), "a child was released against a partial input set"
    tracker.register_task_complete(parent_ids[0], plan_id)

    # The second parent re-produces its block, completing the set. Inserted after the
    # first so that iterating the held blocks instead of the child's requirement map
    # would hand the child its inputs backwards.
    producer._reconstruction_outputs[plan_id][(parent_ids[1], 0)] = bundles[1]
    producer._release_reconstruction_children(parent_ids[1], plan_id, task_index=1)
    assert producer.has_next()

    handed_over = producer.get_next()
    assert handed_over.block_refs == [
        bundles[0].block_refs[0],
        bundles[1].block_refs[0],
    ], "the child's inputs were not assembled in its original dependency order"
    assert not producer.has_next(), "the child was handed over more than once"
    assert producer._reconstruction_outputs == {}, "held blocks were not consumed"

    # Stamped on the consumer that owns the child, so its re-execution resolves back to
    # the original node instead of registering a fresh one.
    assert consumer._pending_child_ids == {
        block_ref.hex(): (child_task_id, plan_id)
        for block_ref in handed_over.block_refs
    }
    assert consumer._lineage_for_submission(0, handed_over)[:2] == (
        child_task_id,
        plan_id,
    )


def test_release_waits_when_a_re_produced_block_is_missing(
    ray_start_regular_shared,  # noqa: F405
):
    """A child with a slot still empty must wait, not be submitted short.

    Every parent serving a plan runs the release check when it completes, and a
    parent's outputs are withheld before its own done-callback, so whichever parent
    completes last holds the whole set. Until then the child stays put: submitting it
    against a partial input set would silently emit a subset of its rows.
    """
    ctx = DataContext.get_current()
    (
        producer,
        _,
        tracker,
        plan_id,
        child_task_id,
        parent_ids,
        bundles,
    ) = _fan_in_op_under_reconstruction(ctx)
    producer.start(ExecutionOptions(), noop_counter())  # noqa: F405

    # The first parent re-produced its block; the second completes without one.
    producer._reconstruction_outputs[plan_id] = {(parent_ids[0], 0): bundles[0]}
    tracker.register_task_complete(parent_ids[0], plan_id)
    producer._release_reconstruction_children(parent_ids[1], plan_id, task_index=1)

    assert not producer.has_next(), "a child was submitted against a partial input set"
    # The block that did arrive stays held, so a later re-production can complete
    # the set rather than finding half of it already consumed.
    assert producer._reconstruction_outputs[plan_id] == {(parent_ids[0], 0): bundles[0]}


def test_reconstruction_input_bypasses_the_bundler(
    ray_start_regular_shared,  # noqa: F405
):
    """An assembled input set must reach submission exactly as it was assembled.

    ``RebundleQueue`` exists to hit a row target, and it will hold a bundle back, merge
    it with whatever else is pending, or ``slice()`` it to get there. Any of those
    applied to a reconstruction child breaks it: merging two children's sets runs them
    as one task and strands one of them, and slicing re-emits part of a block. With a
    1000-row target and a 1-row bundle, the unbypassed bundler would simply swallow it.
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

    # Ordinary input: the bundler holds it, waiting for the row target.
    op._add_input_inner(inputs, 0)
    assert scheduled == []
    assert op._block_ref_bundler.num_blocks() == 1

    # A stamped reconstruction input goes straight to submission instead.
    stamped = make_ref_bundles([[2]])[0]
    op._pending_child_ids[stamped.block_refs[0].hex()] = ("child:0", "plan_a")
    op._add_input_inner(stamped, 0)

    assert scheduled == [(stamped, True)]
    assert op._block_ref_bundler.num_blocks() == 1, "the stamped input entered bundler"
    # Read, not consumed: `_lineage_for_submission` still needs it to name the task.
    assert op._pending_child_ids == {stamped.block_refs[0].hex(): ("child:0", "plan_a")}


def test_held_reconstruction_outputs_keep_the_operator_from_completing(
    ray_start_regular_shared,  # noqa: F405
):
    """A producer holding a child's inputs must not report itself complete.

    Withheld blocks live outside ``_output_queue``, so nothing in the completion
    predicate would see them on its own. ``has_completed()`` is what gates the
    downstream operator's ``all_inputs_done``, so a producer that reported complete
    while still holding a child's input set would strand that child -- its rows would
    go missing with no error anywhere.
    """
    from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
    from ray.data._internal.execution.util import make_ref_bundles

    ctx = DataContext.get_current()
    held = make_ref_bundles([[1, 2, 3]])[0]
    op = MapOperator.create(
        create_map_transformer_from_block_fn(lambda block, _: block),
        input_op=InputDataBuffer(ctx, [held]),
        data_context=ctx,
        name="Producer",
    )
    op.start(ExecutionOptions(), noop_counter())  # noqa: F405

    # Nothing in flight: without the held blocks this operator is finished and complete.
    op._inputs_complete = True
    assert op.has_execution_finished()
    assert op.has_completed()

    op._reconstruction_outputs["plan_a"] = {("seed:0", 0): held}
    assert op.internal_output_queue_num_blocks() == 1
    assert op.internal_output_queue_num_bytes() == held.size_bytes()
    assert not op.has_completed(), "reported complete while owing a child its inputs"

    op.clear_internal_output_queue()
    assert op._reconstruction_outputs == {}
    assert op.has_completed()


@pytest.mark.parametrize("reads_before_loss", [0, 1])
def test_iter_batches_recovery_matches_baseline(
    ray_start_regular_shared,  # noqa: F405
    recovery_enabled,
    object_loss_injector,
    reads_before_loss,
):
    """The terminal gap, stated without inference.

    ``take_all`` buffers, so "these rows reached the driver" has to be argued.
    ``iter_batches`` hands each batch to user code as it arrives, so a duplicate here
    means the caller was given the same row twice, having already been given it once.
    That is the failure in its plainest form.

    ``reads_before_loss=0`` loses the output before anything was yielded, so there is
    nothing to duplicate and recovery is clean.
    """
    object_loss_injector["arm"](reads_before_loss)

    dataset = ray.data.range(100, override_num_blocks=4).map(
        lambda row: {"id": row["id"] * 2}
    )

    seen = []
    for batch in dataset.iter_batches(batch_size=10):
        seen.extend(int(value) for value in batch["id"])

    assert object_loss_injector["fired"], "the injected loss never triggered"
    expected = sorted(i * 2 for i in range(100))
    duplicates = len(seen) - len(set(seen))
    assert sorted(seen) == expected, (
        f"expected {len(expected)} rows, got {len(seen)} "
        f"({len(seen) - len(expected):+d}), duplicates={duplicates}"
    )


def _abortable_task(task_done_callback):
    """A ``DataOpTask`` in its freshly-submitted (ACTIVE) state.

    ``mark_aborted`` reads neither the generator nor the ref counter, so both are
    stubs -- this keeps the test on the state machine and off a live cluster.
    """
    from ray.data._internal.execution.interfaces.physical_operator import DataOpTask

    return DataOpTask(
        0,
        MagicMock(),  # streaming_gen
        MagicMock(),  # block_ref_counter
        "test_op",
        task_done_callback=task_done_callback,
        data_task_id="seed:0",
    )


def test_abort_tears_down_a_task_that_never_drained():
    """``_recover_lost_object`` aborts tasks straight out of ACTIVE.

    Regression test: the re-entrancy guard used to read an attribute that no
    ``__init__`` ever set, so this raised ``AttributeError`` before the
    done-callback ran and the operator leaked the task's resource reservations.
    """
    calls = []
    task = _abortable_task(
        lambda exc, worker_stats, driver_stats: calls.append((exc, worker_stats))
    )
    assert not task.has_finished

    lost_error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
    task.mark_aborted(lost_error)

    assert calls == [(lost_error, None)]
    assert task.has_finished, "an aborted task must not stay pollable"


def test_abort_is_idempotent():
    """A second abort must not double-release the task's reservations."""
    calls = []
    task = _abortable_task(lambda exc, worker_stats, driver_stats: calls.append(exc))

    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected by test")
    task.mark_aborted(error)
    task.mark_aborted(error)

    assert len(calls) == 1


def test_aborted_tasks_in_flight_emits_are_dropped():
    """A pair still in the fetcher when its task is aborted must not be emitted.

    Regression test: the abort fires the done-callback and the operator drops the
    task, so a later ``produce_block`` raised ``KeyError`` on the freed per-task
    metrics -- and where it didn't, it duplicated the reconstructed output.
    """
    from ray.data._internal.execution.interfaces.physical_operator import DeferredEmit
    from ray.data._internal.execution.metadata_fetcher import ThreadedMetadataFetcher

    emitted = []
    task = _abortable_task(lambda exc, worker_stats, driver_stats: None)
    task.produce_block = lambda block_ref, meta: emitted.append(block_ref)

    fetcher = ThreadedMetadataFetcher()
    meta_ref = ray.ObjectRef.nil()
    fetcher._pending_deferred = [DeferredEmit(task, MagicMock(), meta_ref)]
    fetcher.submit("op", [])
    assert task.has_pending_emits()

    # Metadata arrives, but the task is aborted before the fetcher drains it.
    fetcher._results[meta_ref] = b"unused-because-the-task-is-gone"
    task.mark_aborted(ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected"))

    assert fetcher._emit_ready() == []
    assert emitted == [], "an aborted task's in-flight pair was still emitted"
    assert not task.has_pending_emits(), "the dropped pair must still be accounted"


def test_a_drained_task_aborted_before_its_done_callback_fires_it_once():
    """``_fire_done_callbacks`` must skip a drained task that was since aborted."""
    from ray.data._internal.execution.metadata_fetcher import ThreadedMetadataFetcher

    calls = []
    task = _abortable_task(lambda exc, worker_stats, driver_stats: calls.append(exc))

    fetcher = ThreadedMetadataFetcher()
    fetcher._drained_tasks.add(task)
    error = ObjectLostError(ray.ObjectRef.nil().hex(), None, "injected")
    task.mark_aborted(error)

    fetcher._fire_done_callbacks()

    assert calls == [error], "the drained-task sweep re-fired an aborted task"
    assert task not in fetcher._drained_tasks


def test_abort_after_normal_completion_is_a_noop():
    """``mark_done`` already fired the callback; aborting must not fire it again."""
    calls = []
    task = _abortable_task(lambda exc, worker_stats, driver_stats: calls.append(exc))

    task.mark_done()
    assert task.has_finished
    task.mark_aborted(ObjectLostError(ray.ObjectRef.nil().hex(), None, "late loss"))

    assert calls == [None], "the abort resurrected a completed task's callback"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
