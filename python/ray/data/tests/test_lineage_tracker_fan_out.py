import sys
from dataclasses import dataclass
from enum import Enum
from typing import Collection, List, Tuple

import pytest

from ray.data._internal.execution.lineage_tracker import (
    LineageTracker,
    ObjectReuseStatus,
    ParentBlockOutput,
)

#: The single seed output block that every child consumes in
#: :attr:`FanOutMode.SHARED` fan-outs.
_SHARED_OUTPUT_INDEX = 0


def _child_output_indices(child_position: int, num_inputs_per_child: int) -> List[int]:
    """Return the seed output indices consumed by the child at ``child_position``.

    Each child gets its own contiguous, non-overlapping slice of the seed's
    outputs: child ``i`` consumes ``[i * n, ..., i * n + n - 1]`` for ``n``
    inputs per child.
    """
    start = child_position * num_inputs_per_child
    return list(range(start, start + num_inputs_per_child))


def _register_fan_out(
    tracker: LineageTracker,
    seed_task_id: str,
    child_task_ids: List[str],
    num_inputs_per_child: int = 1,
) -> None:
    """Register and complete a seed task, then fan out children over its outputs.

    ``child_task_ids[i]`` consumes ``num_inputs_per_child`` of the seed's output
    blocks, starting at index ``i * num_inputs_per_child``, so the children
    partition the seed's outputs. The children are left submitted-but-not-
    complete; each test decides which ones finish.
    """
    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_complete(seed_task_id)
    for child_position, child_task_id in enumerate(child_task_ids):
        tracker.register_task_submission(
            child_task_id,
            dependencies=[
                ParentBlockOutput(
                    parent_data_task_id=seed_task_id, output_index=output_index
                )
                for output_index in _child_output_indices(
                    child_position, num_inputs_per_child
                )
            ],
        )


def _register_shared_output_fan_out(
    tracker: LineageTracker,
    seed_task_id: str,
    child_task_ids: List[str],
    shared_output_index: int = _SHARED_OUTPUT_INDEX,
) -> None:
    """Register and complete a seed task, then fan out children over one output.

    Unlike :func:`_register_fan_out`, every child consumes the *same* seed output
    block ``shared_output_index``, so the seed produces a single output shared by
    the whole fan-out instead of one block per child. The children are left
    submitted-but-not-complete; each test decides which ones finish.
    """
    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_complete(seed_task_id)
    for child_task_id in child_task_ids:
        tracker.register_task_submission(
            child_task_id,
            dependencies=[
                ParentBlockOutput(
                    parent_data_task_id=seed_task_id, output_index=shared_output_index
                )
            ],
        )


def _register_downstream_chain(
    tracker: LineageTracker,
    parent_task_id: str,
    parent_output_index: int,
    chain_task_ids: List[str],
    complete_leaf: bool = True,
) -> None:
    """Hang a linear chain off ``parent_task_id``'s output ``parent_output_index``.

    ``chain_task_ids[0]`` consumes the given output of the (already completed)
    ``parent_task_id``; every later task consumes its predecessor's output 0.
    Every task is completed except the leaf when ``complete_leaf`` is False, which
    leaves it in flight so a test can fail it.
    """
    tracker.register_task_submission(
        chain_task_ids[0],
        dependencies=[
            ParentBlockOutput(
                parent_data_task_id=parent_task_id, output_index=parent_output_index
            )
        ],
    )
    for upstream_task_id, downstream_task_id in zip(chain_task_ids, chain_task_ids[1:]):
        tracker.register_task_complete(upstream_task_id)
        tracker.register_task_submission(
            downstream_task_id,
            dependencies=[
                ParentBlockOutput(parent_data_task_id=upstream_task_id, output_index=0)
            ],
        )
    if complete_leaf:
        tracker.register_task_complete(chain_task_ids[-1])


def _assert_reuse_statuses(
    tracker: LineageTracker,
    data_task_id: str,
    reused_output_indices: Collection[int],
    all_output_indices: Collection[int],
    plan_id: str,
    task_is_unrelated: bool = False,
) -> None:
    """Assert which of ``data_task_id``'s outputs ``plan_id`` still needs.

    Outputs in ``reused_output_indices`` must be OBJECT_REUSED (a child pending
    that plan's reconstruction still claims them); every other output in
    ``all_output_indices`` must be OBJECT_PRUNED. Reuse status is always scoped
    to a plan, so an output nothing in this plan claims is pruned regardless of
    whether its consumer completed, is in flight, or belongs to another plan.
    """
    for output_index in all_output_indices:
        expected_status = ObjectReuseStatus.OBJECT_UNRELATED
        if not task_is_unrelated:
            expected_status = (
                ObjectReuseStatus.OBJECT_REUSED
                if output_index in reused_output_indices
                else ObjectReuseStatus.OBJECT_PRUNED
            )
        assert (
            tracker.get_object_reuse_status(
                data_task_id, output_index=output_index, plan_id=plan_id
            )
            == expected_status
        )


class FanOutMode(Enum):
    """How the seed's output blocks are distributed across the fan-out's children."""

    #: Each child consumes its own contiguous, non-overlapping slice of the
    #: seed's outputs, so the seed produces one block (or
    #: ``num_inputs_per_child`` blocks) per child.
    PARTITIONED = "partitioned"
    #: Every child consumes the *same* seed output block, so the seed produces a
    #: single output shared by the whole fan-out.
    SHARED = "shared"


class SiblingState(Enum):
    """What the children that never fail are doing when a failure lands."""

    #: The surviving siblings completed before the first failure.
    COMPLETED = "completed"
    #: The surviving siblings were submitted and are never completed, so they
    #: are still in flight for the whole test.
    IN_FLIGHT = "in_flight"


class FailureMode(Enum):
    """How the failures are interleaved with the recoveries they trigger."""

    #: Every child fails before any recovery begins, so all the plans are live
    #: on the seed at the same time.
    TOGETHER = "together"
    #: Each child fails in its own round, and that round's plan is fully
    #: discharged -- seed recovered, retry resubmitted *and* completed -- before
    #: the next child fails.
    SEQUENTIAL_RECOVERED = "sequential_recovered"
    #: Each child fails in its own round, but its retry is only resubmitted and
    #: left executing, so every earlier child is still mid-reconstruction when
    #: the next one fails.
    SEQUENTIAL_IN_RECOVERY = "sequential_in_recovery"


@dataclass(frozen=True)
class FanOutCase:
    """One fan-out failure/recovery scenario for ``test_fan_out_child_failure_recovery``."""

    #: How many children hang off the seed.
    num_children: int
    #: Positions (within ``child_task_ids``) of the children that fail, in the
    #: order they fail.
    failed_child_positions: Tuple[int, ...]
    fan_out_mode: FanOutMode = FanOutMode.PARTITIONED
    sibling_state: SiblingState = SiblingState.COMPLETED
    failure_mode: FailureMode = FailureMode.TOGETHER
    #: How many seed outputs each child consumes. ``FanOutMode.SHARED`` ignores
    #: this: there every child consumes the one shared block.
    num_inputs_per_child: int = 1

    @property
    def child_task_ids(self) -> List[str]:
        return [f"child_{i}" for i in range(self.num_children)]

    @property
    def seed_output_indices(self) -> List[int]:
        """Every output block the seed produces for this fan-out."""
        if self.fan_out_mode is FanOutMode.SHARED:
            return [_SHARED_OUTPUT_INDEX]
        return list(range(self.num_children * self.num_inputs_per_child))

    def output_indices_for(self, child_position: int) -> List[int]:
        """The seed outputs consumed by the child at ``child_position``."""
        if self.fan_out_mode is FanOutMode.SHARED:
            return [_SHARED_OUTPUT_INDEX]
        return _child_output_indices(child_position, self.num_inputs_per_child)


def _fail_child_and_open_plan(
    tracker: LineageTracker,
    seed_task_id: str,
    child_task_id: str,
    existing_plan_ids: Collection[str],
) -> str:
    """Fail ``child_task_id`` and assert it opens a fresh plan rooted at the seed.

    Every failure in a fan-out traces back through its own branch to the seed,
    which is the only retry root no matter how many siblings failed or are still
    running, and a fresh failure always opens a plan id of its own.
    """
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(child_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None
    assert plan_id not in existing_plan_ids
    return plan_id


def _assert_plan_claims_only_child(
    tracker: LineageTracker,
    seed_task_id: str,
    child_task_id: str,
    child_output_indices: Collection[int],
    seed_output_indices: Collection[int],
    plan_id: str,
) -> None:
    """Assert ``plan_id`` claims exactly ``child_task_id`` and the outputs it needs.

    Plans never merge: the seed's pending children under this plan are exactly
    ``{child_task_id}``, and only the outputs that child consumes are
    OBJECT_REUSED. Every other output is OBJECT_PRUNED regardless of what its
    consumer is doing -- completed, still in flight, mid-reconstruction under
    another plan, or claimed by a sibling plan.
    """
    assert tracker.get_pending_children(seed_task_id, plan_id=plan_id) == {
        child_task_id: {seed_task_id: list(child_output_indices)}
    }
    _assert_reuse_statuses(
        tracker,
        seed_task_id,
        child_output_indices,
        seed_output_indices,
        plan_id=plan_id,
    )


def _assert_seed_discharged(
    tracker: LineageTracker,
    seed_task_id: str,
    seed_output_indices: Collection[int],
    plan_id: str,
) -> None:
    """Assert ``plan_id`` no longer claims the seed at all.

    Once the seed completes for a plan, reconstruction has moved downstream: the
    seed has no pending children left for that plan and the plan stops claiming
    it entirely, so all of its outputs report OBJECT_UNRELATED. This holds even
    when the plan's own child has not been resubmitted yet.
    """
    assert tracker.get_pending_children(seed_task_id, plan_id=plan_id) == {}
    _assert_reuse_statuses(
        tracker,
        seed_task_id,
        [],
        seed_output_indices,
        plan_id=plan_id,
        task_is_unrelated=True,
    )


def _assert_child_is_plan_target(
    tracker: LineageTracker, child_task_id: str, plan_id: str
) -> None:
    """Assert ``child_task_id`` is the leaf target of ``plan_id``.

    Nothing consumes a leaf child's outputs, so its plan has no pending children
    below it and the outputs it re-produces are OBJECT_NEW.
    """
    assert tracker.get_pending_children(child_task_id, plan_id=plan_id) == {}
    assert (
        tracker.get_object_reuse_status(child_task_id, output_index=0, plan_id=plan_id)
        == ObjectReuseStatus.OBJECT_NEW
    )


def _resubmit_child_retry(
    tracker: LineageTracker,
    seed_task_id: str,
    child_task_id: str,
    child_output_indices: Collection[int],
) -> None:
    """Resubmit ``child_task_id`` against the recovered seed's fresh outputs."""
    tracker.register_task_submission(
        child_task_id,
        dependencies=[
            ParentBlockOutput(
                parent_data_task_id=seed_task_id, output_index=output_index
            )
            for output_index in child_output_indices
        ],
    )


_FAN_OUT_RECOVERY_CASES = [
    # Siblings complete, then the failures land together: one plan per failed
    # child, all live on the same seed at once.
    pytest.param(
        FanOutCase(num_children=2, failed_child_positions=(0,)),
        id="one_of_two_fails_together",
    ),
    pytest.param(
        FanOutCase(num_children=2, failed_child_positions=(0,), num_inputs_per_child=2),
        id="one_of_two_fails_together_multi_input",
    ),
    pytest.param(
        FanOutCase(num_children=5, failed_child_positions=(2,)),
        id="one_of_five_fails_together",
    ),
    pytest.param(
        FanOutCase(num_children=2, failed_child_positions=(0, 1)),
        id="two_of_two_fail_together",
    ),
    pytest.param(
        FanOutCase(num_children=5, failed_child_positions=(0, 1, 2, 3, 4)),
        id="five_of_five_fail_together",
    ),
    pytest.param(
        FanOutCase(num_children=5, failed_child_positions=(2, 4)),
        id="two_of_five_fail_together",
    ),
    # Siblings complete, and each failure lands only after the previous round's
    # child has already recovered.
    pytest.param(
        FanOutCase(
            num_children=2,
            failed_child_positions=(0, 1),
            failure_mode=FailureMode.SEQUENTIAL_RECOVERED,
        ),
        id="two_of_two_fail_sequentially",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(0, 1, 2, 3, 4),
            failure_mode=FailureMode.SEQUENTIAL_RECOVERED,
        ),
        id="five_of_five_fail_sequentially",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2, 4),
            failure_mode=FailureMode.SEQUENTIAL_RECOVERED,
        ),
        id="two_of_five_fail_sequentially",
    ),
    # The same seed output fans out to every child, and the failures land
    # together: sibling plans claim the very same block at once.
    pytest.param(
        FanOutCase(
            num_children=2,
            failed_child_positions=(0,),
            fan_out_mode=FanOutMode.SHARED,
        ),
        id="shared_output_one_of_two_fails_together",
    ),
    pytest.param(
        FanOutCase(
            num_children=2,
            failed_child_positions=(0, 1),
            fan_out_mode=FanOutMode.SHARED,
        ),
        id="shared_output_two_of_two_fail_together",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2, 4),
            fan_out_mode=FanOutMode.SHARED,
        ),
        id="shared_output_two_of_five_fail_together",
    ),
    # The same seed output fans out to every child, and the failures land in
    # separate rounds.
    pytest.param(
        FanOutCase(
            num_children=2,
            failed_child_positions=(0, 1),
            fan_out_mode=FanOutMode.SHARED,
            failure_mode=FailureMode.SEQUENTIAL_RECOVERED,
        ),
        id="shared_output_two_of_two_fail_sequentially",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2, 4),
            fan_out_mode=FanOutMode.SHARED,
            failure_mode=FailureMode.SEQUENTIAL_RECOVERED,
        ),
        id="shared_output_two_of_five_fail_sequentially",
    ),
    # The surviving siblings never complete: they are still in flight when the
    # failures land, which must not change what the plans claim.
    pytest.param(
        FanOutCase(
            num_children=2,
            failed_child_positions=(0,),
            sibling_state=SiblingState.IN_FLIGHT,
        ),
        id="in_flight_siblings_one_of_two_fails_together",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2,),
            sibling_state=SiblingState.IN_FLIGHT,
        ),
        id="in_flight_siblings_one_of_five_fails_together",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2, 4),
            sibling_state=SiblingState.IN_FLIGHT,
        ),
        id="in_flight_siblings_two_of_five_fail_together",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2, 4),
            sibling_state=SiblingState.IN_FLIGHT,
            failure_mode=FailureMode.SEQUENTIAL_RECOVERED,
        ),
        id="in_flight_siblings_two_of_five_fail_sequentially",
    ),
    # Each round leaves its retry executing, so several reconstructions are in
    # progress at once and none of them may be resubmitted by a later plan.
    pytest.param(
        FanOutCase(
            num_children=2,
            failed_child_positions=(0, 1),
            failure_mode=FailureMode.SEQUENTIAL_IN_RECOVERY,
        ),
        id="two_of_two_fail_while_retries_in_recovery",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(0, 1, 2, 3, 4),
            failure_mode=FailureMode.SEQUENTIAL_IN_RECOVERY,
        ),
        id="five_of_five_fail_while_retries_in_recovery",
    ),
    pytest.param(
        FanOutCase(
            num_children=5,
            failed_child_positions=(2, 4),
            failure_mode=FailureMode.SEQUENTIAL_IN_RECOVERY,
        ),
        id="two_of_five_fail_while_retries_in_recovery",
    ),
]


@pytest.mark.parametrize("case", _FAN_OUT_RECOVERY_CASES)
def test_fan_out_child_failure_recovery(case: FanOutCase):
    """Fail children of a fan-out and check only the failed branches recover.

    Graph (``FanOutMode.PARTITIONED``, one output block per edge)::

        seed_task --output 0--> child_0
                  --output 1--> child_1
                  ...

    Every case here is the same shape -- one completed seed with ``n`` children
    hanging off its outputs, some subset of which fail -- varied along four axes
    (see :class:`FanOutCase`): how the seed's outputs are spread over the
    children (``fan_out_mode``, including the shared-block variant where all the
    edges above carry ``output 0`` and ``num_inputs_per_child`` for edges
    carrying several blocks), whether the children that never fail completed or
    are still in flight (``sibling_state``), which children fail
    (``failed_child_positions``), and how the failures interleave with the
    recoveries they trigger (``failure_mode``).

    Across all of them, verifies that:
      - Every failure returns the seed -- the chain root -- as the only task to
        retry, under a plan id of its own. Sibling children never add extra retry
        roots, and a fresh failure never joins an existing plan, so the seed
        recovers once per failed child.
      - Plans stay scoped to their own branch: the seed's pending children under
        a plan are exactly that plan's failed child, reporting exactly the output
        indices that child consumed. Children that completed, that are still in
        flight, or that are mid-reconstruction under another plan are never
        dragged in -- in particular, an in-flight retry is never resubmitted by a
        later plan.
      - Reuse status is scoped to a plan: only the outputs the plan's own child
        needs are OBJECT_REUSED. Every other output is OBJECT_PRUNED, whatever
        its consumer is doing. A block shared by several children is
        independently OBJECT_REUSED for each plan that needs it re-produced.
      - Completing the seed for one plan discharges only that plan: the seed
        stops being claimed by it (OBJECT_UNRELATED) and has no pending children
        for it, while every other live plan keeps its pending child and reused
        outputs untouched.
      - Each failed child is the leaf target of its own plan, so it has no
        pending children and reports OBJECT_NEW -- nothing consumes its outputs.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_ids = case.child_task_ids
    seed_output_indices = case.seed_output_indices

    # Complete the seed task, then fan out the children over its outputs.
    if case.fan_out_mode is FanOutMode.SHARED:
        _register_shared_output_fan_out(tracker, seed_task_id, child_task_ids)
    else:
        _register_fan_out(
            tracker, seed_task_id, child_task_ids, case.num_inputs_per_child
        )

    # Settle the children that never fail. When they are left in flight instead,
    # their branches must still stay out of every reconstruction plan.
    if case.sibling_state is SiblingState.COMPLETED:
        for child_position, child_task_id in enumerate(child_task_ids):
            if child_position not in case.failed_child_positions:
                tracker.register_task_complete(child_task_id)

    if case.failure_mode is FailureMode.TOGETHER:
        # Every child fails before any recovery begins, so each failure opens a
        # plan of its own and all of them are live on the seed at once.
        plan_ids = []
        for child_position in case.failed_child_positions:
            plan_ids.append(
                _fail_child_and_open_plan(
                    tracker, seed_task_id, child_task_ids[child_position], plan_ids
                )
            )

        # Recovery resubmits the seed once per reconstruction, and each plan
        # claims just its own child and the outputs that child needs.
        for child_position, plan_id in zip(case.failed_child_positions, plan_ids):
            tracker.register_task_submission(seed_task_id, dependencies=[])
            _assert_plan_claims_only_child(
                tracker,
                seed_task_id,
                child_task_ids[child_position],
                case.output_indices_for(child_position),
                seed_output_indices,
                plan_id,
            )

        # Discharge the plans one at a time: completing the seed for one plan and
        # resubmitting its child must leave every other plan untouched.
        for plan_position, (child_position, plan_id) in enumerate(
            zip(case.failed_child_positions, plan_ids)
        ):
            child_task_id = child_task_ids[child_position]

            tracker.register_task_complete(seed_task_id, plan_id=plan_id)
            _resubmit_child_retry(
                tracker,
                seed_task_id,
                child_task_id,
                case.output_indices_for(child_position),
            )

            _assert_seed_discharged(tracker, seed_task_id, seed_output_indices, plan_id)
            _assert_child_is_plan_target(tracker, child_task_id, plan_id)

            # The plans that have not been discharged yet still claim their own
            # child and outputs.
            for other_plan_position in range(plan_position + 1, len(plan_ids)):
                other_position = case.failed_child_positions[other_plan_position]
                _assert_plan_claims_only_child(
                    tracker,
                    seed_task_id,
                    child_task_ids[other_position],
                    case.output_indices_for(other_position),
                    seed_output_indices,
                    plan_ids[other_plan_position],
                )

            # This plan's retry completes, closing it out.
            tracker.register_task_complete(child_task_id, plan_id=plan_id)
        return

    # Sequential failures: each child fails in a round of its own, after the
    # previous round has already recovered the seed.
    plan_ids = []
    for child_position in case.failed_child_positions:
        child_task_id = child_task_ids[child_position]
        child_output_indices = case.output_indices_for(child_position)

        # This round's failure is fresh, so it opens a new plan rooted at the
        # seed no matter what the earlier rounds left behind.
        plan_id = _fail_child_and_open_plan(
            tracker, seed_task_id, child_task_id, plan_ids
        )

        # Resubmit the seed to emulate this round's recovery.
        tracker.register_task_submission(seed_task_id, dependencies=[])
        _assert_plan_claims_only_child(
            tracker,
            seed_task_id,
            child_task_id,
            child_output_indices,
            seed_output_indices,
            plan_id,
        )

        # Every earlier round's plan was already discharged on the seed when the
        # seed completed for it, whether or not its child has finished.
        for earlier_plan_id in plan_ids:
            _assert_seed_discharged(
                tracker, seed_task_id, seed_output_indices, earlier_plan_id
            )
        plan_ids.append(plan_id)

        # The seed's re-execution completes for this plan and this round's retry
        # is resubmitted against the recovered outputs.
        tracker.register_task_complete(seed_task_id, plan_id=plan_id)
        _resubmit_child_retry(
            tracker, seed_task_id, child_task_id, child_output_indices
        )

        _assert_seed_discharged(tracker, seed_task_id, seed_output_indices, plan_id)
        _assert_child_is_plan_target(tracker, child_task_id, plan_id)

        if case.failure_mode is FailureMode.SEQUENTIAL_RECOVERED:
            # The retry completes, closing out the round before the next failure.
            tracker.register_task_complete(child_task_id, plan_id=plan_id)

    if case.failure_mode is FailureMode.SEQUENTIAL_IN_RECOVERY:
        # Every retry is still in flight. The seed is discharged for all of the
        # plans, and each retry is still the leaf target of its own plan.
        for child_position, plan_id in zip(case.failed_child_positions, plan_ids):
            _assert_seed_discharged(tracker, seed_task_id, seed_output_indices, plan_id)
            _assert_child_is_plan_target(
                tracker, child_task_ids[child_position], plan_id
            )

        # The in-flight retries finally complete, closing out every plan.
        for child_position, plan_id in zip(case.failed_child_positions, plan_ids):
            tracker.register_task_complete(
                child_task_ids[child_position], plan_id=plan_id
            )


def test_seed_fail_mid_fan_out_prunes_consumed_output_and_marks_new_output_new():
    """Fail the seed itself after it produced output 0 but before output 1 exists.

    Graph::

        seed_task --output 0--> child_0  (submitted before the failure, still executing)
                  --output 1--> child_1  (only submitted after the seed recovers)

    Unlike the other tests in this file, the failure is on the *seed*, not on a
    child, so the seed is the target of its own reconstruction plan, and the
    fan-out is only half-built when it happens: ``child_0`` is in flight against
    output 0 and output 1 has no consumer yet. Verifies that:
      - Failing the seed returns the seed itself as the retry root, along with the
        plan id keying the recovery.
      - After the seed is resubmitted, ``child_0`` is *not* a pending child: it
        never failed and is still executing, so it must not be re-submitted.
      - Output 0 is OBJECT_PRUNED (already claimed by the in-flight ``child_0``)
        while output 1 is OBJECT_NEW -- no child has ever consumed it, so the
        recovered seed genuinely produces it for the first time.
      - Submitting ``child_1`` against output 1 flips that output to
        OBJECT_PRUNED, and once the seed completes for the plan both outputs stay
        pruned and it has no pending children.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_ids = ["child_0", "child_1"]
    seed_output_indices = range(len(child_task_ids))

    # The seed is submitted, and child_0 starts consuming its first output block.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_submission(
        child_task_ids[0],
        dependencies=[
            ParentBlockOutput(parent_data_task_id=seed_task_id, output_index=0)
        ],
    )

    # The seed fails before output 1 is ever produced. It has no parents, so it is
    # its own retry root and the target of the reconstruction plan.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(seed_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit the seed to emulate recovery.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # child_0 is still executing against the original output 0 and never failed,
    # so it is not pending reconstruction and must not be resubmitted.
    assert tracker.get_pending_children(seed_task_id, plan_id=plan_id) == {}

    # Output 0 was already consumed by the in-flight child_0 -> pruned. Output 1
    # has no consumer at all, so the recovered seed produces it fresh -> new.
    assert (
        tracker.get_object_reuse_status(seed_task_id, output_index=0, plan_id=plan_id)
        == ObjectReuseStatus.OBJECT_PRUNED
    )
    assert (
        tracker.get_object_reuse_status(seed_task_id, output_index=1, plan_id=plan_id)
        == ObjectReuseStatus.OBJECT_NEW
    )

    # child_1 is submitted against the newly produced output 1, which claims it.
    tracker.register_task_submission(
        child_task_ids[1],
        dependencies=[
            ParentBlockOutput(parent_data_task_id=seed_task_id, output_index=1)
        ],
    )
    assert (
        tracker.get_object_reuse_status(seed_task_id, output_index=1, plan_id=plan_id)
        == ObjectReuseStatus.OBJECT_PRUNED
    )

    # The seed has no pending children and both outputs are now claimed.
    assert tracker.get_pending_children(seed_task_id, plan_id=plan_id) == {}
    _assert_reuse_statuses(
        tracker, seed_task_id, [], seed_output_indices, plan_id=plan_id
    )

    # Each child has no pending children or outputs related to the reconstruction.
    for child_task_id in child_task_ids:
        assert tracker.get_pending_children(child_task_id, plan_id=plan_id) == {}
        assert (
            tracker.get_object_reuse_status(
                child_task_id, output_index=0, plan_id=plan_id
            )
            == ObjectReuseStatus.OBJECT_UNRELATED
        )

    # The seed's re-execution closes out the plan, then both children finish:
    # child_0 (which survived the seed failure) first.
    tracker.register_task_complete(seed_task_id, plan_id=plan_id)
    tracker.register_task_complete(child_task_ids[0])
    tracker.register_task_complete(child_task_ids[1])

    # The seed still has no pending children and both outputs remain pruned.
    assert tracker.get_pending_children(seed_task_id, plan_id=plan_id) == {}
    _assert_reuse_statuses(
        tracker, seed_task_id, [], seed_output_indices, plan_id=plan_id
    )


def test_fan_out_mid_graph_branch_leaf_fail_recovers_seed_and_failed_branch_only():
    """Fail a deep leaf below a mid-graph fan-out and recover only that branch.

    Graph::

        seed_task --output 0--> prefix_task --output 0--> fan_out_task
            fan_out_task --output 0--> branch_0_task_0 --output 0-->
                branch_0_task_1 --output 0--> branch_0_task_2  (fails)
            fan_out_task --output 1--> branch_1_task_0 --output 0-->
                branch_1_task_1 --output 0--> branch_1_task_2  (completes)

    This combines the linear-chain and fan-out cases: the fan-out does not sit on
    the seed itself but three levels down, and each of its two children roots a
    3-deep chain of its own. Verifies that:
      - Failing ``branch_0_task_2`` traces up through its own branch, through the
        fan-out and the linear prefix, all the way to the seed as the single
        retry root, and yields the plan id the whole recovery is keyed by.
      - Before anything completes for the plan, the whole failed path --
        seed -> prefix -> fan-out -> branch_0_task_0 -> branch_0_task_1 ->
        branch_0_task_2 -- is pending reconstruction, each parent reporting
        exactly its one pending child on output 0, and each of those outputs is
        OBJECT_REUSED.
      - The surviving half is untouched: it is not part of the plan, so
        ``fan_out_task``'s output 1 is OBJECT_PRUNED, no task in ``branch_1`` is
        a pending child, and every output along that branch reports
        OBJECT_PRUNED. So exactly half the downstream graph is reconstructed.
      - As reconstruction walks down the failed path, each parent's output flips
        to OBJECT_UNRELATED once it completes for the plan -- the plan stops
        claiming that parent entirely -- and the parent has no pending children
        left, while the surviving branch is still left alone.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    prefix_task_id = "prefix_task"
    fan_out_task_id = "fan_out_task"
    failed_branch_task_ids = [f"branch_0_task_{i}" for i in range(3)]
    surviving_branch_task_ids = [f"branch_1_task_{i}" for i in range(3)]
    failed_leaf_task_id = failed_branch_task_ids[-1]

    # Linear prefix: seed -> prefix -> fan_out, one output block per edge. Each
    # task completes before its child is submitted.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_complete(seed_task_id)
    tracker.register_task_submission(
        prefix_task_id,
        dependencies=[
            ParentBlockOutput(parent_data_task_id=seed_task_id, output_index=0)
        ],
    )
    tracker.register_task_complete(prefix_task_id)
    tracker.register_task_submission(
        fan_out_task_id,
        dependencies=[
            ParentBlockOutput(parent_data_task_id=prefix_task_id, output_index=0)
        ],
    )
    tracker.register_task_complete(fan_out_task_id)

    # The fan-out's two outputs each root a 3-deep chain. branch_1 finishes
    # entirely; branch_0's leaf is left in flight so it can fail.
    _register_downstream_chain(
        tracker,
        fan_out_task_id,
        parent_output_index=0,
        chain_task_ids=failed_branch_task_ids,
        complete_leaf=False,
    )
    _register_downstream_chain(
        tracker,
        fan_out_task_id,
        parent_output_index=1,
        chain_task_ids=surviving_branch_task_ids,
        complete_leaf=True,
    )

    # Fail branch_0's leaf; reconstruction traces up its branch, through the
    # fan-out and the prefix, to the seed as the one retry root.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(failed_leaf_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit the seed to emulate recovery of the graph root.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Every task on the failed path is pending reconstruction. Each edge carries
    # output 0 -- including fan_out -> branch_0_task_0 -- so each parent reports
    # its single pending child on output 0 and that output is reused.
    recovered_path_task_ids = [
        seed_task_id,
        prefix_task_id,
        fan_out_task_id,
    ] + failed_branch_task_ids
    for parent_task_id, child_task_id in zip(
        recovered_path_task_ids, recovered_path_task_ids[1:]
    ):
        assert tracker.get_pending_children(parent_task_id, plan_id=plan_id) == {
            child_task_id: {parent_task_id: [0]}
        }
        assert (
            tracker.get_object_reuse_status(
                parent_task_id, output_index=0, plan_id=plan_id
            )
            == ObjectReuseStatus.OBJECT_REUSED
        )

    # The failed leaf is the plan's target and has no children of its own.
    assert tracker.get_pending_children(failed_leaf_task_id, plan_id=plan_id) == {}
    assert (
        tracker.get_object_reuse_status(
            failed_leaf_task_id, output_index=0, plan_id=plan_id
        )
        == ObjectReuseStatus.OBJECT_NEW
    )

    # The surviving half of the fan-out is untouched: output 1 is not claimed by
    # the plan, and no task along that branch is pending reconstruction.
    assert (
        tracker.get_object_reuse_status(
            fan_out_task_id, output_index=1, plan_id=plan_id
        )
        == ObjectReuseStatus.OBJECT_PRUNED
    )

    # Walk reconstruction down the failed path: once a parent completes for the
    # plan, it has no pending children left and the plan no longer claims its
    # output at all -> unrelated.
    for parent_task_id, child_task_id in zip(
        recovered_path_task_ids, recovered_path_task_ids[1:]
    ):
        assert tracker.get_pending_children(parent_task_id, plan_id=plan_id) == {
            child_task_id: {parent_task_id: [0]}
        }
        assert (
            tracker.get_object_reuse_status(
                parent_task_id, output_index=0, plan_id=plan_id
            )
            == ObjectReuseStatus.OBJECT_REUSED
        )

        tracker.register_task_complete(parent_task_id, plan_id=plan_id)
        tracker.register_task_submission(
            child_task_id,
            dependencies=[
                ParentBlockOutput(parent_data_task_id=parent_task_id, output_index=0)
            ],
        )

        assert tracker.get_pending_children(parent_task_id, plan_id=plan_id) == {}
        assert (
            tracker.get_object_reuse_status(
                parent_task_id, output_index=0, plan_id=plan_id
            )
            == ObjectReuseStatus.OBJECT_UNRELATED
        )

    assert (
        tracker.get_object_reuse_status(
            failed_leaf_task_id, output_index=0, plan_id=plan_id
        )
        == ObjectReuseStatus.OBJECT_NEW
    )

    # The leaf's own re-execution closes out the plan.
    tracker.register_task_complete(failed_leaf_task_id, plan_id=plan_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
