"""Unit tests for the output-limit escape hatch (``relieve_output_limit_deadlock``).

Fake-driven: the hatch only reads a handful of operator methods and one
ResourceBank field, so the fakes implement exactly that surface.
"""
from types import SimpleNamespace
from typing import Any, Collection, cast

import pytest
from typing_extensions import override

from ray.data._internal.execution.interfaces import ExecutionOptions, RefBundle
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.resource_bank import LiveObjStore, ResourceBankBase
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.output_limit_escape_hatch import (
    num_rows_required,
    relieve_output_limit_deadlock,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import noop_counter
from ray.tests.conftest import *  # noqa


class _FakeQueueOp(InternalQueueOperatorMixin):
    """A non-actor-pool queueing operator (e.g. an OutputSplitter): a real
    ``InternalQueueOperatorMixin`` so ``num_rows_required`` recognizes it, with
    only the surface the hatch reads and no heavy ``__init__``."""

    def __init__(self, name: str, inputs=(), *, rows_needed: int = 0):
        self._name = name
        self._input_deps = list(inputs)
        # Lets a real operator (e.g. OutputSplitter) wire itself downstream.
        self._output_dependencies = []
        self._rows_needed = rows_needed

    @property
    @override
    def name(self) -> str:
        return self._name

    @property
    @override
    def input_dependencies(self):
        return self._input_deps

    @property
    @override
    # pyrefly: ignore[bad-override]  # partial test double
    def _input_queues(self):
        return ()

    @property
    @override
    # pyrefly: ignore[bad-override]  # partial test double
    def _output_queues(self):
        return ()

    @override
    def has_completed(self) -> bool:
        return False

    @override
    def num_output_splits(self) -> int:
        return 1

    @override
    def min_num_rows_needed_to_make_progress(self) -> int:
        return self._rows_needed


class _FakeActorPoolOp(_FakeQueueOp, ExperimentalActorPoolMapOperator):
    """A genuine ``ExperimentalActorPoolMapOperator`` instance (so the hatch's
    isinstance check passes) with the actor-pool-only reads stubbed.
    ``pulled_output_rows`` is what the bank reports as its pulled output."""

    def __init__(
        self,
        name: str,
        inputs=(),
        *,
        output_backpressured: bool,
        pulled_output_rows: int,
        has_budget: bool = True,
        active_tasks: int = 0,
        rows_needed: int = 0,
    ):
        super().__init__(name, inputs, rows_needed=rows_needed)
        self.pulled_output_rows = pulled_output_rows
        self.upgrade_calls = 0
        # How often the walk evaluated this op's output backpressure.
        self.bp_checks = 0
        self._output_backpressured = output_backpressured
        self._has_budget = has_budget
        self._active_tasks = active_tasks

    @override
    def is_output_backpressured(self) -> bool:
        self.bp_checks += 1
        return self._output_backpressured

    @override
    def has_enough_budget_for_prebuffered_outputs(self) -> bool:
        return self._has_budget

    @override
    def upgrade_output_limit(self) -> bool:
        self.upgrade_calls += 1
        return True

    @override
    # pyrefly: ignore[bad-override]  # partial test double
    def get_actor_info(self):
        return SimpleNamespace(active=self._active_tasks)


class _FakeResourceBank(ResourceBankBase):
    """Reports each op's pulled output rows straight off the fake operator;
    every other bank method inherits the base's ``NotImplementedError`` so an
    unexpected read fails loudly."""

    @override
    def live_object_store(
        self, *, op=None, node_id=None, actor_id=None
    ) -> LiveObjStore:
        return LiveObjStore(num_pulled_output_rows=getattr(op, "pulled_output_rows", 0))


def _detect(output_op):
    return relieve_output_limit_deadlock(
        output_operator=output_op, resource_bank=_FakeResourceBank()
    )


@pytest.mark.parametrize(
    "pulled_rows, has_budget, active_downstream_tasks, expect_upgrade",
    [
        # Cap already covers the split's requirement: nothing is wedged.
        (4, True, 0, False),
        # Cap below the requirement with no downstream tasks to relieve it.
        (3, True, 0, True),
        # Same starvation, but a downstream task is running: consumption will
        # free the budget, so wait instead of upgrading.
        (3, True, 1, False),
        # Requirement met, but the budget cannot even hold the prebuffered
        # outputs: a low object-store stall that waiting cannot fix.
        (4, False, 0, True),
    ],
)
def test_upgrade_conditions(
    pulled_rows, has_budget, active_downstream_tasks, expect_upgrade
):
    """map -> OutputSplitter(4): the two upgrade conditions and their negations.

    ``active_downstream_tasks`` is modelled by a second actor-pool op between
    the splitter and the map (a task-launcher forwards its own active-task
    signal, not the splitter's requirement -- see the chain test below), so
    for the "active" case the map's consumer is that op.
    """
    producer = _FakeActorPoolOp(
        "map",
        output_backpressured=True,
        pulled_output_rows=pulled_rows,
        has_budget=has_budget,
    )
    if active_downstream_tasks:
        consumer = _FakeActorPoolOp(
            "map2",
            inputs=[producer],
            output_backpressured=False,
            pulled_output_rows=0,
            active_tasks=active_downstream_tasks,
            rows_needed=4,
        )
    else:
        consumer = _FakeQueueOp("split", inputs=[producer], rows_needed=4)

    upgraded = _detect(consumer)

    assert (upgraded is producer) == expect_upgrade
    assert producer.upgrade_calls == (1 if expect_upgrade else 0)
    # The producer was evaluated either way (a no-fire is a decision, not the
    # traversal skipping it).
    assert producer.bp_checks == 1


def test_pass_through_forwards_requirement_but_launcher_resets_it():
    """Pass-throughs (limit) forward the downstream requirement to their
    producer; a task-launcher resets it to its own bundle target."""
    # map1 -> limit -> OutputSplitter(2): the split's 2 rows reach map1
    # through the limit, and map1 (1 row pulled) is starved -> upgraded.
    map1 = _FakeActorPoolOp("map1", output_backpressured=True, pulled_output_rows=1)
    limit = _FakeQueueOp("limit", inputs=[map1])
    splitter = _FakeQueueOp("split", inputs=[limit], rows_needed=2)
    assert num_rows_required(splitter) == 2 and num_rows_required(limit) == 0
    assert _detect(splitter) is map1

    # map1 -> limit -> map2 -> OutputSplitter(2): map2 launches tasks, so the
    # split's requirement stops there; map1 only owes map2's own requirement
    # (0) and is NOT starved even with nothing pulled.
    map1 = _FakeActorPoolOp("map1", output_backpressured=True, pulled_output_rows=0)
    limit = _FakeQueueOp("limit", inputs=[map1])
    map2 = _FakeActorPoolOp(
        "map2", inputs=[limit], output_backpressured=False, pulled_output_rows=0
    )
    splitter = _FakeQueueOp("split", inputs=[map2], rows_needed=2)
    assert _detect(splitter) is None
    assert map1.upgrade_calls == 0
    assert map1.bp_checks == 1  # visited, not upgraded


def test_at_most_one_upgrade_per_call_and_detection_converges():
    """map -> OutputSplitter(2), consumer paused: the map stays backpressured
    and fills its (upgraded) cap between ticks. Detection must upgrade until
    the cap meets the split's requirement and then stop -- never run away --
    no matter how many ticks elapse."""

    class _FillingOp(_FakeActorPoolOp):
        @override
        def upgrade_output_limit(self) -> bool:
            # Raise the cap by one block and let the stalled actor fill it,
            # which is what the next tick observes.
            self.upgrade_calls += 1
            self.pulled_output_rows += 1
            return True

    producer = _FillingOp("map", output_backpressured=True, pulled_output_rows=0)
    splitter = _FakeQueueOp("split", inputs=[producer], rows_needed=2)

    first = _detect(splitter)
    assert first is producer and producer.upgrade_calls == 1  # one per call

    for _ in range(1000):
        _detect(splitter)

    assert producer.pulled_output_rows == 2  # settled at the requirement
    assert producer.upgrade_calls == 2  # 0 -> 1 -> 2, then no more


def test_completed_operators_are_skipped():
    producer = _FakeActorPoolOp("map", output_backpressured=True, pulled_output_rows=0)
    producer.has_completed = lambda: True  # type: ignore[method-assign]
    splitter = _FakeQueueOp("split", inputs=[producer], rows_needed=2)

    assert _detect(splitter) is None
    assert producer.upgrade_calls == 0
    assert producer.bp_checks == 0


def test_budget_check_counts_only_working_actors():
    """Condition (b) of the hatch reads the operator's budget check. Only
    actors with tasks in flight count: an idle actor (e.g. one the sizer just
    added) keeps a full, untouched budget that relieves nothing, so a stall on
    the working actors must still read as "not enough budget"."""

    def _op(states, prebuffered_bytes):
        bank = SimpleNamespace(
            live_object_store=lambda actor_id: LiveObjStore(
                num_prebuffered_output_bytes=prebuffered_bytes.get(actor_id, 0),
                num_prebuffered_output_blocks=1 if actor_id in prebuffered_bytes else 0,
            )
        )
        return SimpleNamespace(
            actor_pool=SimpleNamespace(
                alive_actor_states_by_node=lambda: {"n1": states}
            ),
            resource_bank=bank,
            max_output_bytes_for_actor=lambda actor_id: 100,
            max_outputs_for_actor=lambda actor_id: 10,
        )

    check = ExperimentalActorPoolMapOperator.has_enough_budget_for_prebuffered_outputs
    busy = SimpleNamespace(num_tasks_in_flight=5)
    idle = SimpleNamespace(num_tasks_in_flight=0)

    # The working actor is stalled (300 prebuffered > 100 budget); the idle
    # actor's full budget does not count.
    assert check(cast(Any, _op({"busy": busy, "idle": idle}, {"busy": 300}))) is False
    # A working actor with room reads as enough.
    ok = SimpleNamespace(num_tasks_in_flight=1)
    stalled_and_ok = _op({"busy": busy, "ok": ok}, {"busy": 300, "ok": 10})
    assert check(cast(Any, stalled_and_ok)) is True
    # Nothing working: nothing is stalled.
    assert check(cast(Any, _op({"idle": idle}, {}))) is True


class _RemoteBundleSplitter(OutputSplitter):
    """Every bundle lives on a node no split prefers, so dispatch has to wait
    for the locality buffer cap."""

    @staticmethod
    @override
    def _get_locations(bundle: RefBundle) -> Collection[str]:
        return ["elsewhere"]


@pytest.mark.parametrize("bundles_pulled, expect_upgrade", [(3, True), (4, False)])
def test_locality_buffer_starves_producer(
    ray_start_regular_shared, bundles_pulled, expect_upgrade
):
    """map -> OutputSplitter(2, locality hints), with a real splitter.

    No buffered bundle is on a preferred node, so the split holds them until
    2n = 4 are queued. The held bundles are the map's pulled outputs, which
    exhaust its budget: with 3 pulled the map is wedged and must be upgraded;
    with 4 the split dispatches and waiting suffices.
    """
    rows_per_bundle = 5
    producer = _FakeActorPoolOp(
        "map",
        output_backpressured=True,
        pulled_output_rows=bundles_pulled * rows_per_bundle,
    )
    splitter = _RemoteBundleSplitter(
        producer,
        2,
        equal=False,
        data_context=DataContext.get_current(),
        locality_hints=["node0", "node1"],
    )
    splitter.start(ExecutionOptions(actor_locality_enabled=True), noop_counter())
    for bundle in make_ref_bundles(
        [list(range(rows_per_bundle)) for _ in range(bundles_pulled)]
    ):
        splitter.add_input(bundle, 0)
    assert splitter.has_next() != expect_upgrade

    upgraded = _detect(splitter)

    assert (upgraded is producer) == expect_upgrade
    assert producer.upgrade_calls == (1 if expect_upgrade else 0)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
