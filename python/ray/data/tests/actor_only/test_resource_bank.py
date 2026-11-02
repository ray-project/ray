from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Tuple, cast
from unittest.mock import MagicMock

import pytest

from ray.data._internal.compute import ActorPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.resource_bank import (
    ActorPoolAccounting,
    ActorPoolOperator,
    LiveObjStore,
    ResourceBank,
    num_prebuffered_objects,
)
from ray.data.block import Block, BlockExecStats, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

NODE = "node-A"
NODE_B = "node-B"
ACTOR = "actor-1"
# Tests use plain ints as task ids; ResourceBank only needs them as map keys.
TASK: Any = 0


def _tid(n: int) -> Any:
    """Wrap a test task id so pyrefly accepts it as ``ray.TaskID``."""
    return n


@dataclass(frozen=True)
class _FakeRef:
    key: str

    def hex(self) -> str:
        return self.key


@dataclass(frozen=True)
class _FakeActorState:
    status: ActorStatus


class _FakeActorPool:
    """Stand-in for an operator's actor pool."""

    def __init__(
        self,
        *,
        max_concurrency: int = 1,
        active_actors_by_node: Dict[str, int] | None = None,
    ):
        self._max_concurrency = max_concurrency
        # Shaped like the real pool's index, since the fake op's
        # ``actor_counts_by_node`` derives its per-node status breakdown from
        # it -- the (op, node) scope's prebuffer estimate reads that.
        self._node_to_actor_states: Dict[str, Dict[str, _FakeActorState]] = {
            node_id: {
                f"{node_id}-active-{i}": _FakeActorState(ActorStatus.ACTIVE)
                for i in range(count)
            }
            for node_id, count in (active_actors_by_node or {}).items()
        }

    def max_actor_concurrency(self) -> int:
        return self._max_concurrency

    def num_active_actors(self) -> int:
        return sum(len(states) for states in self._node_to_actor_states.values())

    def average_bytes_per_output(self) -> float | None:
        """The pool's sizing hint, from configured per-actor output budgets.

        Optional on the accounting protocol -- the fake returns None so the
        tests exercise the 128 MiB default, and
        ``test_pool_sizing_hint_is_used_before_any_block_is_observed`` covers
        the case where a pool does publish one.
        """


class _FakeActorOp:
    """Structural stand-in for an ``ActorPoolOperator``.

    Subclasses nothing on purpose, so these tests stay independent of any
    particular actor-pool operator. Not a ``PhysicalOperator``, so it has no
    ``.metrics`` -- fine, because ``_ray_remote_args`` always carries a
    non-negative cap, short-circuiting ``estimate_npo_per_actor``.
    """

    def __init__(
        self,
        *,
        prebuffered_outputs: int = 0,
        max_concurrency: int = 1,
        active_actors_by_node: Dict[str, int] | None = None,
    ):
        self._ray_remote_args = {
            "_actor_generator_backpressure_num_objects": prebuffered_outputs * 2,
        }
        self._ray_actor_task_remote_args = {}
        self._actor_pool = _FakeActorPool(
            max_concurrency=max_concurrency,
            active_actors_by_node=active_actors_by_node,
        )

    @property
    def actor_pool(self) -> _FakeActorPool:
        return self._actor_pool

    def actor_counts_by_node(self) -> Dict[str, "Counter[ActorStatus]"]:
        """Per-node actor status breakdown, mirroring a real actor pool's.

        The fake never has pending actors, so unlike a real pool this only
        folds in the running-actor index.
        """
        return {
            node_id: Counter(state.status for state in states.values())
            for node_id, states in self._actor_pool._node_to_actor_states.items()
        }


def _make_op(
    prebuffered_outputs: int = 0,
    *,
    num_active_actors: int = 0,
    active_actors_by_node: Dict[str, int] | None = None,
) -> PhysicalOperator:
    """Build a fake actor operator, typed as ``PhysicalOperator``.

    Args:
        prebuffered_outputs: Generator backpressure cap, in outputs per actor.
        num_active_actors: Actors the pool reports as active, all on ``NODE``.
        active_actors_by_node: Per-node counts; overrides ``num_active_actors``.

    Returns:
        The fake operator.
    """
    if active_actors_by_node is None:
        active_actors_by_node = {NODE: num_active_actors} if num_active_actors else {}
    return cast(
        PhysicalOperator,
        _FakeActorOp(
            prebuffered_outputs=prebuffered_outputs,
            active_actors_by_node=active_actors_by_node,
        ),
    )


def _bm(
    task_idx: int = 0,
    size_bytes: int = 100,
    num_rows: int = 10,
    node: str = NODE,
) -> BlockMetadata:
    return BlockMetadata(
        num_rows=num_rows,
        size_bytes=size_bytes,
        exec_stats=BlockExecStats(task_idx=task_idx),
        input_files=None,
        node_id=node,
    )


def _new_output(
    bank: ResourceBank,
    op: PhysicalOperator,
    ref_key: str,
    *,
    task_id: Any = None,
    node: str = NODE,
    size_bytes: int = 100,
) -> _FakeRef:
    ref = _FakeRef(ref_key)
    bank.on_new_output(
        op=op,
        ref=cast(ObjectRef[Block], ref),
        bm=_bm(task_idx=task_id or 0, node=node, size_bytes=size_bytes),
        task_id=task_id,
    )
    return ref


def _consume(bank: ResourceBank, *refs: _FakeRef) -> None:
    for ref in refs:
        bank.on_block_consumed(input_ref=cast(ObjectRef[Block], ref))
    bank.drain_consumed_blocks()


def _total_blocks_bytes(usage: LiveObjStore) -> Tuple[int, int]:
    blocks = (
        usage.num_pulled_output_blocks
        + usage.num_prebuffered_output_blocks
        + usage.num_input_blocks
    )
    bytes_ = (
        usage.num_pulled_output_bytes
        + usage.num_prebuffered_output_bytes
        + usage.num_input_bytes
    )
    return blocks, bytes_


def _assert_no_outputs(usage: LiveObjStore) -> None:
    assert usage.num_pulled_output_blocks == 0
    assert usage.num_pulled_output_bytes == 0
    assert usage.num_pulled_output_rows == 0
    assert usage.num_prebuffered_output_blocks == 0
    assert usage.num_prebuffered_output_bytes == 0
    assert usage.num_prebuffered_output_rows == 0


def _assert_empty(usage: LiveObjStore) -> None:
    _assert_no_outputs(usage)
    assert usage.num_input_blocks == 0
    assert usage.num_input_bytes == 0


def test_fake_actor_op_satisfies_the_actor_pool_protocol():
    """ResourceBank narrows operators structurally, so pin the fake's
    conformance directly.

    If the fake stopped matching ``ActorPoolOperator``, the actor-path tests
    below would silently take the no-actor-pool branches and assert nothing
    meaningful, rather than failing.
    """
    assert isinstance(_make_op(), ActorPoolOperator)
    assert isinstance(
        _make_op(prebuffered_outputs=2, num_active_actors=1), ActorPoolOperator
    )


def test_prebuffer_estimate_treats_absent_backpressure_keys_as_disabled():
    """Operators opted out of generator backpressure emit NO backpressure keys
    (``generator_backpressure_disabled=True``); the prebuffer estimate must
    read an absent key exactly like an explicit -1: disabled (-1)."""
    op_absent = cast(Any, _make_op())
    op_absent._ray_remote_args = {}
    op_absent._ray_actor_task_remote_args = {}
    assert num_prebuffered_objects(op=op_absent) == -1

    op_disabled = cast(Any, _make_op())
    op_disabled._ray_remote_args = {"_actor_generator_backpressure_num_objects": -1}
    op_disabled._ray_actor_task_remote_args = {
        "_generator_backpressure_num_objects": -1
    }
    assert num_prebuffered_objects(op=op_disabled) == -1

    # And a real cap still converts (object count -> blocks, grouped yields).
    op_capped = _make_op(prebuffered_outputs=3)
    assert num_prebuffered_objects(op=op_capped) == 3


def test_source_output_created_consumed_and_duplicate_consumption_is_idempotent():
    bank = ResourceBank()
    source_op = _make_op()

    ref = _new_output(bank, source_op, "source-0")

    assert _total_blocks_bytes(bank.live_object_store(op=source_op)) == (1, 100)
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (1, 100)
    assert _total_blocks_bytes(bank.live_object_store()) == (1, 100)

    _consume(bank, ref)

    _assert_empty(bank.live_object_store(op=source_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store())

    # A second consumer report for the same ref should be ignored, not underflow.
    _consume(bank, ref)
    _assert_empty(bank.live_object_store())


@pytest.mark.parametrize("num_outputs", [2, 4, 8])
def test_actor_multi_output_task_drains_after_completion(num_outputs: int):
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=99, size_bytes=50)],
    )
    refs = [
        _new_output(bank, actor_op, f"actor-output-{i}", task_id=TASK)
        for i in range(num_outputs)
    ]

    actor_usage = bank.live_object_store(actor_id=ACTOR)
    assert actor_usage.num_input_blocks == 1
    assert actor_usage.num_input_bytes == 50
    assert actor_usage.num_pulled_output_blocks == num_outputs
    assert actor_usage.num_pulled_output_bytes == num_outputs * 100

    bank.on_task_completed(op=actor_op, task_id=TASK)
    assert bank.live_object_store(actor_id=ACTOR).num_input_blocks == 0

    for remaining, ref in reversed(list(enumerate(refs))):
        _consume(bank, ref)
        assert (
            bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == remaining
        )

    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store())


def test_cross_node_input_counts_secondary_copy_until_task_completion():
    bank = ResourceBank()
    source_op = _make_op()
    actor_op = _make_op()

    source_ref = _new_output(
        bank, source_op, "source-remote", node=NODE, size_bytes=100
    )
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=0, node=NODE, size_bytes=100)],
    )

    assert _total_blocks_bytes(bank.live_object_store(op=source_op)) == (1, 100)
    assert bank.live_object_store(op=actor_op).num_input_blocks == 1
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (1, 100)
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE_B)) == (1, 100)
    assert _total_blocks_bytes(bank.live_object_store()) == (2, 200)

    cumulative = bank.cumulative_object_store(op=actor_op)
    assert cumulative.input_bytes_remote.num_samples == 1
    assert cumulative.input_bytes_remote.mean == 100
    assert cumulative.input_bytes_local.num_samples == 0

    bank.on_task_completed(op=actor_op, task_id=TASK)
    assert _total_blocks_bytes(bank.live_object_store()) == (1, 100)
    _consume(bank, source_ref)
    _assert_empty(bank.live_object_store())


def test_local_input_transfers_primary_copy_without_double_count_after_drain():
    bank = ResourceBank()
    source_op = _make_op()
    actor_op = _make_op()

    source_ref = _new_output(bank, source_op, "source-local", node=NODE)
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)

    # This mirrors _ExperimentalActorPool.on_task_submitted: local input refs
    # are withdrawn from the producer when ownership moves to the actor.
    bank.on_block_consumed(input_ref=cast(ObjectRef[Block], source_ref))
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=0, node=NODE)],
    )
    bank.drain_consumed_blocks()

    assert _total_blocks_bytes(bank.live_object_store(op=source_op)) == (0, 0)
    assert bank.live_object_store(op=actor_op).num_input_blocks == 1
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (1, 100)
    assert _total_blocks_bytes(bank.live_object_store()) == (1, 100)

    cumulative = bank.cumulative_object_store(op=actor_op)
    assert cumulative.input_bytes_local.num_samples == 1
    assert cumulative.input_bytes_local.mean == 100
    assert cumulative.input_bytes_remote.num_samples == 0

    bank.on_task_completed(op=actor_op, task_id=TASK)
    _assert_empty(bank.live_object_store())


def test_prebuffered_pulled_and_actorless_usage_are_correct_by_scope():
    bank = ResourceBank()
    # One actor stays busy (a single in-flight task), so its pool reports one
    # active actor for the op-scope prebuffer estimate.
    actor_op = _make_op(prebuffered_outputs=2, num_active_actors=1)
    source_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    _new_output(bank, actor_op, "actor-0", task_id=TASK, node=NODE)
    _new_output(bank, actor_op, "actor-1", task_id=TASK, node=NODE)
    _new_output(bank, source_op, "source-a", node=NODE, size_bytes=50)
    _new_output(bank, source_op, "source-b", node=NODE_B, size_bytes=70)

    actor_usage = bank.live_object_store(actor_id=ACTOR)
    assert actor_usage.num_pulled_output_blocks == 2
    assert actor_usage.num_pulled_output_bytes == 200
    assert actor_usage.num_prebuffered_output_blocks == 2
    assert actor_usage.num_prebuffered_output_bytes == 200

    assert _total_blocks_bytes(bank.live_object_store(op=actor_op)) == (4, 400)
    assert _total_blocks_bytes(bank.live_object_store(op=source_op)) == (2, 120)
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (5, 450)
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE_B)) == (1, 70)
    assert _total_blocks_bytes(bank.live_object_store(op=actor_op, node_id=NODE)) == (
        4,
        400,
    )
    assert _total_blocks_bytes(bank.live_object_store(op=source_op, node_id=NODE)) == (
        1,
        50,
    )
    assert _total_blocks_bytes(
        bank.live_object_store(op=source_op, node_id=NODE_B)
    ) == (1, 70)
    assert _total_blocks_bytes(bank.live_object_store()) == (6, 520)


def test_downscaling_an_actor_stops_counting_its_outputs():
    """Scaling an actor down drops its output accounting on purpose.

    Its blocks are still in the object store until they are consumed, but a
    downscaled actor with unconsumed outputs is rare enough not to be worth
    tracking. The operator and node scopes still count them, so the totals
    those drive stay correct.
    """
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    refs = [
        _new_output(bank, actor_op, f"downscaled-{i}", task_id=TASK) for i in range(2)
    ]
    bank.on_task_completed(op=actor_op, task_id=TASK)

    bank.deregister_actor(actor_id=ACTOR)

    # The actor stops being tracked at all, since nothing is left charged to it.
    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    assert ACTOR not in bank.stats.actor_to_operator
    # The blocks are still live, and the scopes that budget on them know it.
    assert _total_blocks_bytes(bank.live_object_store(op=actor_op)) == (2, 200)
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (2, 200)

    _consume(bank, *refs)

    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))


def test_actor_can_downscale_after_outputs_are_consumed():
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    ref = _new_output(bank, actor_op, "completed-before-downscale", task_id=TASK)
    bank.on_task_completed(op=actor_op, task_id=TASK)
    _consume(bank, ref)

    bank.deregister_actor(actor_id=ACTOR)

    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))


def test_finalize_dataset_clears_all_outputs_inputs_and_refs():
    bank = ResourceBank()
    op1 = _make_op()
    op2 = _make_op()

    bank.maybe_register_actor(op=op1, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=0, node=NODE, size_bytes=50)],
    )
    _new_output(bank, op1, "op1-output", task_id=TASK, node=NODE, size_bytes=100)
    op2_ref = _new_output(bank, op2, "op2-output", node=NODE, size_bytes=70)

    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (3, 220)

    bank.finalize_dataset(ops={op1})

    _assert_empty(bank.live_object_store(op=op1))
    _assert_empty(bank.live_object_store(op=op2))
    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store())

    # Late duplicate consumption reports after finalization should be ignored.
    _consume(bank, op2_ref)
    _assert_empty(bank.live_object_store())


def test_resource_bank_instances_are_isolated():
    bank1 = ResourceBank()
    bank2 = ResourceBank()
    op1 = _make_op()
    op2 = _make_op()

    _new_output(bank1, op1, "bank1-output", node=NODE, size_bytes=100)
    _new_output(bank2, op2, "bank2-output", node=NODE_B, size_bytes=70)

    assert _total_blocks_bytes(bank1.live_object_store()) == (1, 100)
    assert _total_blocks_bytes(bank2.live_object_store()) == (1, 70)

    bank1.finalize_dataset(ops={op1})

    _assert_empty(bank1.live_object_store())
    assert _total_blocks_bytes(bank2.live_object_store()) == (1, 70)


def test_global_cumulative_object_store_merges_operator_trackers():
    bank = ResourceBank()
    op1 = _make_op()
    op2 = _make_op()

    _new_output(bank, op1, "op1-output", size_bytes=100)
    _new_output(bank, op2, "op2-output", size_bytes=300)

    cumulative = bank.cumulative_object_store()
    assert cumulative.output_bytes.num_samples == 2
    assert cumulative.output_bytes.mean == 200
    assert cumulative.output_bytes.min == 100
    assert cumulative.output_bytes.max == 300


def test_late_task_completion_and_actor_lookup_after_finalize_are_noops():
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    bank.finalize_dataset(ops={actor_op})

    assert bank.get_actor_id_from_task_id(op=actor_op, task_id=TASK) is None
    bank.on_task_completed(op=actor_op, task_id=TASK)

    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store())


def test_actor_relocation_migrates_node_bookkeeping():
    """When an actor restarts on another node (only possible on node death),
    its accounting follows it: the in-flight tasks are retried there, so their
    inputs move with the actor. Blocks it produced on the dead node may linger
    in that node's object store, but nothing can reach them any more, so they
    are dropped rather than left charged to a node that is gone."""
    bank = ResourceBank()
    actor_op = _make_op()

    # 1) First sighting on NODE, with an in-flight task and a live output.
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=0, node=NODE, size_bytes=50)],
    )
    pre_reloc = _new_output(bank, actor_op, "pre-reloc-output", task_id=TASK, node=NODE)
    assert bank.live_object_store(actor_id=ACTOR).num_input_blocks == 1

    # 2) Node died -> the actor is re-sighted on NODE_B. The in-flight task is
    # retried there, so the actor keeps its input; the output left on the dead
    # node is released from every scope.
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    actor_usage = bank.live_object_store(actor_id=ACTOR)
    assert actor_usage.num_input_blocks == 1
    assert actor_usage.num_input_bytes == 50
    _assert_no_outputs(actor_usage)
    _assert_empty(bank.live_object_store(op=actor_op, node_id=NODE))
    _assert_empty(bank.live_object_store(node_id=NODE))
    # The retried task's input now occupies NODE_B, not the dead node.
    assert bank.live_object_store(node_id=NODE_B).num_input_bytes == 50

    # 3) Regression: a task submitted on the relocated actor must not raise, and
    # its usage must land on NODE_B (the new node's registry now knows it).
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK + 1,
        input_bms=[_bm(task_idx=0, node=NODE_B, size_bytes=100)],
    )
    post_reloc = _new_output(
        bank, actor_op, "post-reloc-output", task_id=TASK + 1, node=NODE_B
    )

    # Preserved pre-reloc input (50) plus the new task's input (100), all on
    # NODE_B now, plus the block the actor just produced there.
    assert bank.live_object_store(actor_id=ACTOR).num_input_blocks == 2
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE_B)) == (3, 250)
    _assert_empty(bank.live_object_store(node_id=NODE))

    # 4) Once every task is done and every block is consumed, nothing remains.
    bank.on_task_completed(op=actor_op, task_id=TASK)
    bank.on_task_completed(op=actor_op, task_id=TASK + 1)
    # `pre_reloc` was already released at relocation, so reporting it now is a
    # no-op rather than an underflow.
    _consume(bank, pre_reloc, post_reloc)
    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store(node_id=NODE_B))
    _assert_empty(bank.live_object_store())
    _assert_registries_drained(bank)


def test_straggler_task_completion_after_relocation_is_a_noop():
    """A task submitted before relocation can complete after the actor was
    re-sighted on a new node. Its completion must resolve to the node the task
    was re-homed onto (the old node's registry no longer knows it), must not
    drive the in-flight count negative, and must leave the task bound to its
    actor throughout (task_pull_request budgets retried outputs through it)."""
    bank = ResourceBank()
    actor_op = _make_op()

    # 1) First sighting on NODE, with an in-flight task.
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=0, node=NODE, size_bytes=50)],
    )

    # 2) NODE died -> actor re-sighted on NODE_B. The task stays bound to the
    # actor: core retries it on the restarted actor and its outputs must keep
    # resolving to a live per-actor budget.
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    assert bank.get_actor_id_from_task_id(op=actor_op, task_id=TASK) == ACTOR
    actor_usage = bank.live_object_store(actor_id=ACTOR)
    assert actor_usage.num_input_blocks == 1
    _assert_no_outputs(actor_usage)

    # 3) The pre-relocation task's done-callback fires. Must not raise, and
    # must not drive the relocated actor's in-flight count negative.
    bank.on_task_completed(op=actor_op, task_id=TASK)

    op_stats = bank.stats.per_operator[actor_op]
    actor_info = op_stats.actors.get_actor_info(actor_id=ACTOR)
    assert actor_info is not None
    assert actor_info.num_tasks_in_flight == 0
    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store(node_id=NODE_B))

    # 4) The relocated actor keeps working normally.
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK + 1,
        input_bms=[_bm(task_idx=0, node=NODE_B, size_bytes=100)],
    )
    bank.on_task_completed(op=actor_op, task_id=TASK + 1)
    assert actor_info.num_tasks_in_flight == 0
    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store(node_id=NODE_B))
    _assert_empty(bank.live_object_store())


def test_retried_task_output_before_actor_reregisters_on_new_node():
    """A restarted actor re-runs its in-flight task on a NEW node and can
    produce outputs there before any fresh submission re-registers it with the
    bank (relocation fires on submit). The output lands on the new node's
    registry, which doesn't know the actor yet -- that must not assert."""
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR,
        task_id=TASK,
        input_bms=[_bm(task_idx=0, node=NODE, size_bytes=50)],
    )

    # NODE died; the retried task produces on NODE_B while the bank still has
    # the actor registered on NODE.
    ref = _new_output(bank, actor_op, "retried-output", task_id=TASK, node=NODE_B)

    # Node-level accounting still charges NODE_B for the block.
    assert bank.live_object_store(node_id=NODE_B).num_pulled_output_blocks == 1

    bank.on_task_completed(op=actor_op, task_id=TASK)
    _consume(bank, ref)
    _assert_empty(bank.live_object_store(node_id=NODE_B))


def test_op_node_scope_is_joined_on_both_operator_and_node():
    bank = ResourceBank()
    op_a, op_b = _make_op(), _make_op()

    # op_a runs on both nodes; op_b only on NODE. Inputs are all produced on
    # NODE, so only the actors that live on NODE score a locality hit.
    bank.maybe_register_actor(op=op_a, actor_id="a-on-A", new_node_id=NODE)
    bank.maybe_register_actor(op=op_a, actor_id="a-on-B", new_node_id=NODE_B)
    bank.maybe_register_actor(op=op_b, actor_id="b-on-A", new_node_id=NODE)
    bank.on_task_submitted(
        actor_id="a-on-A",
        task_id=_tid(1),
        input_bms=[_bm(node=NODE, size_bytes=100)],
    )
    bank.on_task_submitted(
        actor_id="a-on-B",
        task_id=_tid(2),
        input_bms=[_bm(node=NODE, size_bytes=200)],
    )
    bank.on_task_submitted(
        actor_id="b-on-A",
        task_id=_tid(3),
        input_bms=[_bm(node=NODE, size_bytes=300)],
    )

    assert bank.op_node_ids() == {(op_a, NODE), (op_a, NODE_B), (op_b, NODE)}

    # Same operator, different node: one hit, one miss.
    a_on_node = bank.cumulative_object_store(op=op_a, node_id=NODE)
    assert (
        a_on_node.input_bytes_local.sum,
        a_on_node.input_bytes_local.num_samples,
    ) == (
        100,
        1,
    )
    assert a_on_node.input_bytes_remote.num_samples == 0

    a_on_node_b = bank.cumulative_object_store(op=op_a, node_id=NODE_B)
    assert (
        a_on_node_b.input_bytes_remote.sum,
        a_on_node_b.input_bytes_remote.num_samples,
    ) == (200, 1)
    assert a_on_node_b.input_bytes_local.num_samples == 0

    # Same node, different operator: not folded into op_a's scope.
    assert bank.cumulative_object_store(
        op=op_b, node_id=NODE
    ).input_bytes_local.sum == (300)

    # A pair that never existed reads as empty rather than raising.
    assert bank.cumulative_object_store(op=op_b, node_id=NODE_B).input_bytes.sum == 0


@pytest.mark.parametrize(
    "input_node", [NODE, NODE_B], ids=["inputs_on_node_a", "inputs_on_node_b"]
)
def test_op_node_scope_sums_to_the_operator_and_node_scopes(input_node):
    """Grafana derives the per-operator and per-node views by summing the
    (op, node) series over the other dimension, so the joined scope has to agree
    with both scopes it is derived from."""
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id="a-on-A", new_node_id=NODE)
    bank.maybe_register_actor(op=actor_op, actor_id="a-on-B", new_node_id=NODE_B)
    bank.on_task_submitted(
        actor_id="a-on-A",
        task_id=_tid(1),
        input_bms=[_bm(node=input_node, size_bytes=100)],
    )
    bank.on_task_submitted(
        actor_id="a-on-B",
        task_id=_tid(2),
        input_bms=[_bm(node=input_node, size_bytes=100)],
    )
    _new_output(bank, actor_op, "out-A", task_id=_tid(1), node=NODE, size_bytes=70)
    _new_output(bank, actor_op, "out-B", task_id=_tid(2), node=NODE_B, size_bytes=30)

    joined = [
        bank.cumulative_object_store(op=op, node_id=node)
        for op, node in bank.op_node_ids()
        if op is actor_op
    ]

    # Sum over nodes == the operator scope, for the locality split and outputs.
    op_scope = bank.cumulative_object_store(op=actor_op)
    for attr in ("input_bytes_local", "input_bytes_remote", "output_bytes"):
        assert sum(getattr(c, attr).sum for c in joined) == getattr(op_scope, attr).sum
        assert (
            sum(getattr(c, attr).num_samples for c in joined)
            == getattr(op_scope, attr).num_samples
        )

    # Sum over operators == the node scope (this dataset has one actor op).
    for node in (NODE, NODE_B):
        node_scope = bank.cumulative_object_store(node_id=node)
        op_on_node = bank.cumulative_object_store(op=actor_op, node_id=node)
        assert op_on_node.output_bytes.sum == node_scope.output_bytes.sum
        assert op_on_node.input_bytes_local.sum == node_scope.input_bytes_local.sum

    # Exactly one of the two actors is co-located with the input, whichever node
    # the input came from -- so the interesting part is WHICH scope took the hit.
    # A per-operator or per-node rollup alone cannot distinguish these.
    other_node = NODE_B if input_node is NODE else NODE
    colocated = bank.cumulative_object_store(op=actor_op, node_id=input_node)
    assert (
        colocated.input_bytes_local.num_samples,
        colocated.input_bytes_local.sum,
    ) == (
        1,
        100,
    )
    assert colocated.input_bytes_remote.num_samples == 0

    fetched_remotely = bank.cumulative_object_store(op=actor_op, node_id=other_node)
    assert (
        fetched_remotely.input_bytes_remote.num_samples,
        fetched_remotely.input_bytes_remote.sum,
    ) == (1, 100)
    assert fetched_remotely.input_bytes_local.num_samples == 0


def test_op_node_scope_outlives_the_actors_that_created_it():
    """The exported series has to stay monotonic for windowed Grafana queries:
    an operator's last actor leaving a node must not zero out its history."""
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK, input_bms=[_bm(node=NODE, size_bytes=100)]
    )
    bank.on_task_completed(op=actor_op, task_id=TASK)
    bank.deregister_actor(actor_id=ACTOR)

    assert (actor_op, NODE) in bank.op_node_ids()
    cumulative = bank.cumulative_object_store(op=actor_op, node_id=NODE)
    assert cumulative.input_bytes_local.sum == 100
    # The actor row is gone, so no live usage lingers on the joined scope.
    _assert_empty(bank.live_object_store(op=actor_op, node_id=NODE))


def test_op_node_scope_migrates_the_actor_row_but_not_its_history():
    """Relocation moves the actor to the new (op, node) scope.

    Cumulative history is a record of what happened and stays where it was
    recorded. Live accounting is a claim about what is occupying memory right
    now, so the blocks stranded on the dead node are released from it.
    """
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK, input_bms=[_bm(node=NODE, size_bytes=50)]
    )
    ref = _new_output(bank, actor_op, "pre-reloc", task_id=TASK, node=NODE)

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK + 1, input_bms=[_bm(node=NODE_B, size_bytes=80)]
    )

    old_scope = bank.cumulative_object_store(op=actor_op, node_id=NODE)
    new_scope = bank.cumulative_object_store(op=actor_op, node_id=NODE_B)
    # History stays where it was recorded...
    assert old_scope.input_bytes.sum == 50
    assert old_scope.output_bytes.sum == 100
    # ...and the relocated actor's new work lands on the new scope.
    assert new_scope.input_bytes.sum == 80
    assert new_scope.output_bytes.num_samples == 0

    # The output stranded on the dead node is no longer charged anywhere, and a
    # consumption report that arrives for it afterwards is a no-op.
    _assert_empty(bank.live_object_store(op=actor_op, node_id=NODE))
    _consume(bank, ref)
    _assert_empty(bank.live_object_store(op=actor_op, node_id=NODE))


def test_live_object_store_total_bytes_sums_all_live_components():
    object_store = LiveObjStore(
        num_input_bytes=100,
        num_pulled_output_bytes=200,
        num_prebuffered_output_bytes=300,
    )
    assert object_store.total_bytes() == 600


def test_real_actor_pool_operator_satisfies_the_actor_pool_protocol(
    monkeypatch: pytest.MonkeyPatch,
):
    """A protocol member no real operator implements makes ``isinstance``
    silently return False, turning the prebuffer term off everywhere instead of
    failing. Pin the real operator so it cannot drift from the fake."""
    data_context = DataContext.get_current()
    op = MapOperator.create(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        compute_strategy=ActorPoolStrategy(size=1),
    )
    assert isinstance(op, ActorPoolOperator)
    assert isinstance(op.actor_pool, ActorPoolAccounting)
    # And a task-based operator must NOT match, or it would be charged a
    # prebuffer it has no actors to fill. The actor-only backend promotes every
    # task pool to an actor pool (``maybe_promote_compute_strategy``), so this
    # half only has a task operator to check with the backend switched off.
    monkeypatch.delenv("RAY_DATA_ACTOR_ONLY_BACKEND", raising=False)
    task_op = MapOperator.create(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
    )
    assert not isinstance(task_op, ActorPoolOperator)


def test_repeated_relocation_never_drives_a_node_negative():
    """The actor row aggregates every node it has run on, so a scope must
    subtract its OWN share when the actor leaves. Subtracting the aggregate
    made the second relocation take away more than the node ever held."""
    bank = ResourceBank()
    actor_op = _make_op()
    node_c = "node-C"

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK, input_bms=[_bm(node=NODE, size_bytes=50)]
    )
    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK + 1, input_bms=[_bm(node=NODE_B, size_bytes=80)]
    )
    assert bank.live_object_store(actor_id=ACTOR).num_input_bytes == 130

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=node_c)

    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_empty(bank.live_object_store(node_id=NODE_B))
    _assert_empty(bank.live_object_store(op=actor_op, node_id=NODE_B))
    # Both retried tasks now run on node C, with both inputs.
    assert bank.live_object_store(node_id=node_c).num_input_bytes == 130
    assert bank.live_object_store(actor_id=ACTOR).num_input_bytes == 130

    bank.on_task_completed(op=actor_op, task_id=TASK)
    bank.on_task_completed(op=actor_op, task_id=TASK + 1)
    _assert_empty(bank.live_object_store())
    _assert_registries_drained(bank)


def test_retried_task_reyielding_the_same_object_ref_is_a_rematerialization():
    """Streaming-generator return ids are derived from (task_id, put_index),
    so a retried task yields the very same ObjectRefs. That is one logical
    object re-materialized, not a second one."""
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    _new_output(bank, actor_op, "gen-ref-0", task_id=TASK, node=NODE)

    # Same actor, same node, task retried: must not raise, must not count twice.
    ref = _new_output(bank, actor_op, "gen-ref-0", task_id=TASK, node=NODE)
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE)) == (1, 100)

    bank.on_task_completed(op=actor_op, task_id=TASK)
    _consume(bank, ref)
    _assert_empty(bank.live_object_store())


def test_retried_output_rematerialized_on_another_node_moves_the_charge():
    """A retry after the owning node died re-materializes the same ObjectRef
    elsewhere. The old node must stop being charged for it."""
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    _new_output(bank, actor_op, "gen-ref-0", task_id=TASK, node=NODE)

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    ref = _new_output(bank, actor_op, "gen-ref-0", task_id=TASK, node=NODE_B)

    _assert_empty(bank.live_object_store(node_id=NODE))
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE_B)) == (1, 100)
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1

    bank.on_task_completed(op=actor_op, task_id=TASK)
    _consume(bank, ref)
    _assert_empty(bank.live_object_store())


def test_output_yielded_on_the_new_node_before_the_actor_is_resighted():
    """A retried task can yield on the actor's new node before the executor
    re-sights the actor there.

    The actor holds that block throughout, so its row is charged immediately and
    keeps it across the migration -- the per-actor pull budget comes off that
    row, and under-reporting it hands out budget already spent.
    """
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    early = _new_output(bank, actor_op, "early-on-new", task_id=TASK, node=NODE_B)

    assert bank.live_object_store(node_id=NODE_B).num_pulled_output_blocks == 1
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1
    _assert_empty(bank.live_object_store(node_id=NODE))

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    # Carried across the move, and counted exactly once on the new node.
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1
    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE_B)) == (1, 100)

    bank.on_task_completed(op=actor_op, task_id=TASK)
    _consume(bank, early)
    _assert_empty(bank.live_object_store(actor_id=ACTOR))
    _assert_empty(bank.live_object_store(node_id=NODE_B))
    _assert_empty(bank.live_object_store())
    # Totals alone would not catch a task whose own output count went negative
    # (it just never reaches zero, so the entry is never released).
    _assert_registries_drained(bank)


def test_retry_output_survives_the_migration_of_its_task():
    """A retry's blocks on the new node are still live, so the task's per-node
    entry must carry them across the migration.

    Dropping them looks harmless in the totals, but leaves the task's output
    count one short: it goes negative on consumption and never gets released.
    """
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    early = _new_output(bank, actor_op, "early-on-new", task_id=TASK, node=NODE_B)

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE_B)
    later = _new_output(bank, actor_op, "later-on-new", task_id=TASK, node=NODE_B)

    assert _total_blocks_bytes(bank.live_object_store(node_id=NODE_B)) == (2, 200)

    _consume(bank, early, later)
    bank.on_task_completed(op=actor_op, task_id=TASK)

    _assert_empty(bank.live_object_store())
    _assert_registries_drained(bank)


def _assert_registries_drained(bank: ResourceBank) -> None:
    """No leftover task or actor entries anywhere in the bank."""
    for scope in _all_scopes(bank):
        assert scope.actors.task_info_map == {}, scope


def _all_scopes(bank: ResourceBank):
    return [
        *bank.stats.per_operator.values(),
        *bank.stats.per_node.values(),
        *bank.stats.per_op_node.values(),
    ]


def test_task_submitted_to_a_draining_actor_is_tracked():
    """A downscaled actor is DRAINING, not gone: it drains its in-flight
    work and can still be handed a task. Its row lives in the downscaled index,
    so every lookup has to go through ``get_actor_info``."""
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK, input_bms=[_bm(node=NODE, size_bytes=50)]
    )
    bank.deregister_actor(actor_id=ACTOR)

    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK + 1, input_bms=[_bm(node=NODE, size_bytes=70)]
    )
    assert bank.live_object_store(actor_id=ACTOR).num_input_bytes == 120
    assert bank.live_object_store(node_id=NODE).num_input_bytes == 120

    bank.on_task_completed(op=actor_op, task_id=TASK)
    bank.on_task_completed(op=actor_op, task_id=TASK + 1)
    _assert_empty(bank.live_object_store())


def test_consuming_a_downscaled_actors_outputs_does_not_go_negative():
    """`to_downscaled` drops an actor's outputs, so a block consumed afterwards
    was never counted against it.

    This is the ordinary graceful-downscale path: the actor still has a task
    draining, so its entry survives, and its blocks are consumed as they
    stream. Subtracting a block the entry never held would take it below zero,
    and a negative reads as extra output budget rather than none, since the
    budget is the limit minus what the entry holds.
    """
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    before = _new_output(bank, actor_op, "before", task_id=TASK)
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1

    # Downscale while the task is in flight, so the entry survives.
    bank.deregister_actor(actor_id=ACTOR)
    assert ACTOR in bank.stats.actor_to_operator
    _assert_empty(bank.live_object_store(actor_id=ACTOR))

    # The draining task keeps producing, and those blocks are counted normally.
    after = _new_output(bank, actor_op, "after", task_id=TASK)
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1

    # Consume both while the task is still running. One of them was dropped by
    # the downscale, so only one subtraction has anything to subtract.
    for ref in (before, after):
        _consume(bank, ref)
        usage = bank.live_object_store(actor_id=ACTOR)
        assert usage.num_pulled_output_blocks >= 0, usage
        assert usage.num_pulled_output_bytes >= 0, usage
    _assert_empty(bank.live_object_store(actor_id=ACTOR))

    # The scopes that budget on these blocks tracked both start to finish.
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))


def test_a_draining_task_can_still_produce_outputs_after_downscale():
    """Scaling an actor down does not stop its in-flight task.

    A task that is still draining keeps producing blocks, and those are counted
    against the actor as usual. Only what it produced before the downscale is
    dropped.
    """
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(actor_id=ACTOR, task_id=TASK, input_bms=[])
    before = _new_output(bank, actor_op, "before-downscale", task_id=TASK)

    bank.deregister_actor(actor_id=ACTOR)
    after = _new_output(bank, actor_op, "after-downscale", task_id=TASK)

    # Only the post-downscale block is charged to the actor; both are charged
    # to the operator and node.
    assert bank.live_object_store(actor_id=ACTOR).num_pulled_output_blocks == 1
    assert _total_blocks_bytes(bank.live_object_store(op=actor_op)) == (2, 200)

    bank.on_task_completed(op=actor_op, task_id=TASK)
    _consume(bank, before, after)
    _assert_empty(bank.live_object_store(op=actor_op))
    _assert_empty(bank.live_object_store(node_id=NODE))
    _assert_registries_drained(bank)


def test_operator_estimate_equals_the_sum_of_its_per_node_estimates():
    """Every scope has to derive the prebuffer from the same actor rows, or an
    operator's total stops agreeing with the nodes it is spread over."""
    bank = ResourceBank()
    actor_op = _make_op(
        prebuffered_outputs=2, active_actors_by_node={NODE: 1, NODE_B: 1}
    )
    placements = {"actor-a": NODE, "actor-b": NODE_B}

    for actor_id, node in placements.items():
        bank.maybe_register_actor(op=actor_op, actor_id=actor_id, new_node_id=node)
    for idx, (actor_id, node) in enumerate(placements.items()):
        bank.on_task_submitted(
            actor_id=actor_id,
            task_id=_tid(idx),
            input_bms=[_bm(node=node, size_bytes=40)],
        )
        _new_output(bank, actor_op, f"out-{idx}", task_id=_tid(idx), node=node)

    op_usage = bank.live_object_store(op=actor_op)
    per_node_total = LiveObjStore()
    for node in placements.values():
        per_node_total += bank.live_object_store(op=actor_op, node_id=node)

    assert op_usage.total_blocks() == per_node_total.total_blocks()
    assert op_usage.total_bytes() == per_node_total.total_bytes()
    assert (
        op_usage.num_prebuffered_output_blocks
        == per_node_total.num_prebuffered_output_blocks
        == 4
    )


def test_finalize_dataset_releases_everything_it_tracked():
    bank = ResourceBank()
    actor_op = _make_op()

    bank.maybe_register_actor(op=actor_op, actor_id=ACTOR, new_node_id=NODE)
    bank.on_task_submitted(
        actor_id=ACTOR, task_id=TASK, input_bms=[_bm(node=NODE, size_bytes=50)]
    )
    ref = _new_output(bank, actor_op, "leftover", task_id=TASK, node=NODE)
    # A straggling consumer report that lands after the drain must not be kept.
    bank.on_block_consumed(input_ref=cast(ObjectRef[Block], ref))

    bank.finalize_dataset(ops={actor_op})

    assert bank.live_block_refs == {}
    assert bank._consumed_refs == []
    assert bank.stats.per_node == {}
    assert bank.stats.per_operator == {}
    assert bank.stats.per_op_node == {}
    assert bank.stats.actor_to_operator == {}
    _assert_empty(bank.live_object_store())
    assert bank.node_ids() == set()
    assert bank.op_node_ids() == set()


def test_query_apis_hand_back_detached_snapshots():
    """The scheduling loop reads these every tick; handing out live state lets
    a caller corrupt the bank by mutating what it was given."""
    bank = ResourceBank()
    actor_op = _make_op()
    _new_output(bank, actor_op, "snapshot", node=NODE)

    for usage in (
        bank.live_object_store(op=actor_op),
        bank.live_object_store(node_id=NODE),
        bank.live_object_store(op=actor_op, node_id=NODE),
    ):
        usage.num_pulled_output_bytes += 12345
    assert bank.live_object_store(op=actor_op).num_pulled_output_bytes == 100

    # ``cumulative_object_store`` is deliberately NOT copied for a single
    # scope -- see its docstring. The unscoped roll-up still is, since it has
    # to merge anyway.
    assert bank.cumulative_object_store() is not (
        bank.stats.per_operator[actor_op].cumulative
    )


@pytest.mark.parametrize(
    "cap,expected_blocks",
    [
        (0, 0),
        (2, 1),
        (4, 2),
        # A cap that is not a whole number of yields is a user misconfiguration.
        # Round up rather than crashing the executor over an accounting hint.
        (3, 2),
        (1, 1),
    ],
)
def test_backpressure_cap_converts_objects_to_blocks(cap: int, expected_blocks: int):
    op = _make_op()
    cast(Any, op)._ray_remote_args = {"_actor_generator_backpressure_num_objects": cap}
    assert num_prebuffered_objects(op=op) == expected_blocks


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
