import time
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.execution_flags import WITHHELD_DEADLOCK_WAIT_MAX_S
from ray.data._internal.execution.resource_bank import LiveObjStore
from ray.data._internal.experimental.execution.operators import (
    actor_pool_map_operator as op_module,
)
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    HeadroomFunding,
)
from ray.data._internal.experimental.execution.withheld_deadlock import (
    ActorWaits,
    PlanWaits,
    WithheldDeadlock,
    find_withheld_deadlock,
)

WITHIN_NODE_BUDGET = HeadroomFunding.WITHIN_NODE_BUDGET
WAIT_FOR_PEERS = HeadroomFunding.WAIT_FOR_PEERS
IMMEDIATE = HeadroomFunding.IMMEDIATE


def _stuck(actor_id, *plans, tasks=1):
    return ActorWaits(
        actor_id=actor_id,
        stuck=True,
        withheld_plans=frozenset(plans),
        num_tasks_in_flight=tasks,
    )


def _free(actor_id, tasks=1):
    return ActorWaits(actor_id=actor_id, stuck=False, num_tasks_in_flight=tasks)


def _plan(plan_id, *running, unstarted=False):
    return PlanWaits(
        plan_id=plan_id,
        running_actor_ids=frozenset(running),
        has_unstarted_tasks=unstarted,
    )


def test_actor_waiting_on_its_own_plan_is_deadlocked():
    """Its budget is full of the plan's withheld blocks, and the plan's last
    task runs on it: it can never finish."""
    result = find_withheld_deadlock(
        [_stuck("a", "p")], [_plan("p", "a")], has_free_task_slot=False
    )
    assert result.deadlocked == {"a"}
    assert result.targets == ["a"]


def test_stalled_plans_are_the_plans_that_wont_complete():
    """Plan p waits on the deadlocked actor a, so it stalls. Plan q runs on
    an actor that can still pull, so it completes. A non-stuck peer's withheld
    blocks of p never free up by waiting; its blocks of q do."""
    result = find_withheld_deadlock(
        [_stuck("a", "p"), _free("b")],
        [_plan("p", "a"), _plan("q", "b")],
        has_free_task_slot=False,
    )
    assert result.deadlocked == {"a"}
    assert result.stalled_plans == {"p"}


def test_actors_waiting_on_each_other_are_deadlocked():
    result = find_withheld_deadlock(
        [_stuck("a", "p"), _stuck("b", "q")],
        [_plan("p", "b"), _plan("q", "a")],
        has_free_task_slot=False,
    )
    assert result.deadlocked == {"a", "b"}
    assert set(result.targets) == {"a", "b"}


def test_waiting_on_a_progressing_producer_is_not_a_deadlock():
    """The plan's task runs on an actor that can still pull: waiting resolves
    it, so nothing is granted."""
    result = find_withheld_deadlock(
        [_stuck("a", "p"), _free("b")], [_plan("p", "b")], has_free_task_slot=False
    )
    assert result.deadlocked == set()
    assert result.targets == []


def test_only_the_producer_others_wait_on_is_a_target():
    """``a`` only waits on ``b``'s output; room for ``a`` changes nothing, room
    for ``b`` unblocks both."""
    result = find_withheld_deadlock(
        [_stuck("a", "p", tasks=0), _stuck("b", "p")],
        [_plan("p", "b")],
        has_free_task_slot=False,
    )
    assert result.deadlocked == {"a", "b"}
    assert result.targets == ["b"]


def test_chain_through_a_progressing_actor_resolves():
    """``a`` waits on ``b``, ``b`` waits on ``c``, ``c`` can still pull."""
    result = find_withheld_deadlock(
        [_stuck("a", "p"), _stuck("b", "q"), _free("c")],
        [_plan("p", "b"), _plan("q", "c")],
        has_free_task_slot=False,
    )
    assert result.deadlocked == set()


@pytest.mark.parametrize(
    "has_free_task_slot, other_actor, deadlocked",
    [
        # Every slot is held by the stuck actor's stalled tasks.
        (False, None, True),
        # A free slot: the missing task can start.
        (True, None, False),
        # A progressing actor's task will finish and free its slot.
        (False, _free("b"), False),
    ],
)
def test_unstarted_task_waits_on_task_slots(
    has_free_task_slot, other_actor, deadlocked
):
    actors = [_stuck("a", "p", tasks=2)] + ([other_actor] if other_actor else [])
    result = find_withheld_deadlock(
        actors, [_plan("p", unstarted=True)], has_free_task_slot=has_free_task_slot
    )
    assert (result.deadlocked == {"a"}) is deadlocked
    # With no plan task running, unblocking whoever holds the slots helps.
    assert result.targets == (["a"] if deadlocked else [])


# ---------------------------------------------------------------------------
# Relief: how the operator turns a detected deadlock into grants
# ---------------------------------------------------------------------------
class _FakeTime:
    """``time`` stand-in for the operator module: a settable clock."""

    def __init__(self, now: float = 1000.0):
        self.now = now

    def perf_counter(self) -> float:
        return self.now

    def __getattr__(self, name):
        return getattr(time, name)


def _relief_op(deadlocks, grants):
    """An operator whose deadlock detection and pool are scripted.

    ``deadlocks`` is the ``withheld_deadlock()`` result per pass, in order
    (the last one repeats). ``grants`` maps ``(actor_id, funding)`` to the
    bytes the pool grants for that call; any other call grants 0. The pool
    records every call as ``(actor_id, funding)``."""
    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))
    op._name = "Map"
    op._additional_split_factor = None
    op._actor_only_metrics = None
    op._metrics_op_tag = None
    op._withheld_wait_started = {}
    pool = cast(Any, _ExperimentalActorPool.__new__(_ExperimentalActorPool))
    pool.calls = []
    pool.running_actor_ids = lambda: []
    pool.actor_node = lambda actor_id: "node1"

    def grant(actor_id, *, funding, **kwargs):
        pool.calls.append((actor_id, funding))
        return grants.get((actor_id, funding), 0)

    pool.grant_output_headroom = grant
    op._actor_pool = pool
    bank = MagicMock()
    bank.live_object_store.return_value = LiveObjStore()
    op._resource_bank = bank
    remaining = list(deadlocks)
    op.withheld_deadlock = lambda: (
        remaining.pop(0) if len(remaining) > 1 else remaining[0]
    )
    op._withheld_bytes_by_producer = lambda plan_ids: {}
    op._mean_output_bytes = lambda node_id: 100.0
    op.max_output_bytes_for_actor = lambda actor_id: 0.0
    op.max_outputs_for_actor = lambda actor_id: 0.0
    return op, pool


def test_relief_prefers_a_grant_within_the_node_budget_over_rank():
    """The top-ranked target's grant doesn't fit within the node's budget; a
    lower-ranked one's does. That grant goes first, and nobody is asked to
    wait or oversubscribe in that pass."""
    deadlock = WithheldDeadlock(deadlocked={"a", "b"}, targets=["a", "b"])
    op, pool = _relief_op([deadlock], {("b", WITHIN_NODE_BUDGET): 100})

    assert op.relieve_withheld_deadlock() is True
    assert pool.calls == [("a", WITHIN_NODE_BUDGET), ("b", WITHIN_NODE_BUDGET)]


def test_relief_waits_for_peers_until_the_wait_bound(monkeypatch):
    """The grant doesn't fit the node's budget and the peers could free it by draining: the
    target waits, pass after pass, until the bound; then it is granted
    unbacked bytes right away."""
    clock = _FakeTime()
    monkeypatch.setattr(op_module, "time", clock)
    deadlock = WithheldDeadlock(deadlocked={"a"}, targets=["a"])
    op, pool = _relief_op([deadlock], {("a", IMMEDIATE): 100})

    assert op.relieve_withheld_deadlock() is False
    assert pool.calls == [("a", WITHIN_NODE_BUDGET), ("a", WAIT_FOR_PEERS)]
    assert op._withheld_wait_started == {"a": clock.now}

    pool.calls.clear()
    clock.now += WITHHELD_DEADLOCK_WAIT_MAX_S - 1
    assert op.relieve_withheld_deadlock() is False
    assert pool.calls == [("a", WITHIN_NODE_BUDGET), ("a", WAIT_FOR_PEERS)]

    pool.calls.clear()
    clock.now += 1
    assert op.relieve_withheld_deadlock() is True
    assert pool.calls == [("a", WITHIN_NODE_BUDGET), ("a", IMMEDIATE)]
    assert op._withheld_wait_started == {}


def test_wait_clock_restarts_once_the_actor_is_no_longer_deadlocked(monkeypatch):
    clock = _FakeTime()
    monkeypatch.setattr(op_module, "time", clock)
    stuck = WithheldDeadlock(deadlocked={"a"}, targets=["a"])
    op, pool = _relief_op([stuck, WithheldDeadlock(), stuck], {})

    assert op.relieve_withheld_deadlock() is False
    assert op._withheld_wait_started == {"a": clock.now}

    # Not deadlocked this pass: the clock is dropped.
    clock.now += WITHHELD_DEADLOCK_WAIT_MAX_S
    assert op.relieve_withheld_deadlock() is False
    assert op._withheld_wait_started == {}

    # Deadlocked again: a fresh wait, not an expired one.
    clock.now += 1
    pool.calls.clear()
    assert op.relieve_withheld_deadlock() is False
    assert pool.calls == [("a", WITHIN_NODE_BUDGET), ("a", WAIT_FOR_PEERS)]
    assert op._withheld_wait_started == {"a": clock.now}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
