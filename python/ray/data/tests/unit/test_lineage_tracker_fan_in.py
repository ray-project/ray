import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import pytest

from ray.data._internal.execution.lineage_tracker import (
    DataTaskId,
    LineageTracker,
    ObjectReuseStatus,
    OutputIndex,
    ParentBlockOutput,
    PlanId,
)


@dataclass(frozen=True)
class PlanRef:
    """A stand-in for a plan ID, which only the tracker can hand out.
    Used to get a reference to the plan ID handed out by the tracker.
    """

    name: str


class Action:
    """One call against the tracker, and what that call must return.

    Actions are inert data: they are built once as part of a case and only
    :func:`run_actions` ever touches a tracker with them.
    """

    def apply(self, runner: "_ActionRunner") -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class Submit(Action):
    """``register_task_submission``."""

    data_task_id: DataTaskId
    dependencies: Dict[DataTaskId, Sequence[OutputIndex]] = field(default_factory=dict)
    plan: Optional[PlanRef] = None

    def apply(self, runner: "_ActionRunner") -> None:
        runner.tracker.register_task_submission(
            self.data_task_id,
            dependencies=[
                ParentBlockOutput(
                    parent_data_task_id=parent_task_id, output_index=output_index
                )
                for parent_task_id, output_indices in self.dependencies.items()
                for output_index in output_indices
            ],
            plan_id=runner.resolve(self.plan),
        )


@dataclass(frozen=True)
class Complete(Action):
    """``register_task_complete``."""

    data_task_id: DataTaskId
    #: The plan this completion is for, or ``None`` for a fresh attempt.
    plan: Optional[PlanRef] = None

    def apply(self, runner: "_ActionRunner") -> None:
        runner.tracker.register_task_complete(
            self.data_task_id, plan_id=runner.resolve(self.plan)
        )


@dataclass(frozen=True)
class Fail(Action):
    """``register_task_failed``, with the seed tasks it must report.

    The returned plan ID is bound to the plan ref passed.
    All subsequent failures of the same reconstruction attempt
    must pass the same plan ref.
    """

    data_task_id: DataTaskId
    expected_seed_task_ids: Sequence[DataTaskId]
    plan: PlanRef

    def apply(self, runner: "_ActionRunner") -> None:
        bound_plan_id = runner.bound_plan_id(self.plan)
        seed_task_ids, plan_id = runner.tracker.register_task_failed(
            self.data_task_id, plan_id=bound_plan_id
        )
        assert seed_task_ids == list(self.expected_seed_task_ids)
        assert plan_id is not None
        if bound_plan_id is None:
            runner.bind(self.plan, plan_id)
        else:
            assert plan_id == bound_plan_id


@dataclass(frozen=True)
class ExpectPendingChildren(Action):
    """``get_pending_children``, with the mapping it must return."""

    data_task_id: DataTaskId
    plan: PlanRef
    #: Each pending child task ID mapped to the output indices every one of its
    #: parents produced for it.
    expected: Dict[DataTaskId, Dict[DataTaskId, Sequence[OutputIndex]]]

    def apply(self, runner: "_ActionRunner") -> None:
        pending_children = runner.tracker.get_pending_children(
            self.data_task_id, plan_id=runner.resolve(self.plan)
        )
        assert pending_children == {
            child_task_id: {
                parent_task_id: list(output_indices)
                for parent_task_id, output_indices in dependencies_by_parent.items()
            }
            for child_task_id, dependencies_by_parent in self.expected.items()
        }


@dataclass(frozen=True)
class ExpectObjectReuseStatus(Action):
    """``get_object_reuse_status``, with the status it must return."""

    data_task_id: DataTaskId
    output_index: OutputIndex
    plan: PlanRef
    expected: ObjectReuseStatus

    def apply(self, runner: "_ActionRunner") -> None:
        assert (
            runner.tracker.get_object_reuse_status(
                self.data_task_id,
                output_index=self.output_index,
                plan_id=runner.resolve(self.plan),
            )
            == self.expected
        )


class _ActionRunner:
    """Runs actions against one tracker, resolving plan refs as it goes."""

    def __init__(self) -> None:
        self.tracker = LineageTracker()
        self._plan_ids: Dict[str, PlanId] = {}

    def bound_plan_id(self, plan_ref: PlanRef) -> Optional[PlanId]:
        """The plan ID bound to ``plan_ref``, or ``None`` if none is yet."""
        return self._plan_ids.get(plan_ref.name)

    def resolve(self, plan_ref: Optional[PlanRef]) -> Optional[PlanId]:
        """The plan ID ``plan_ref`` stands for, or ``None`` for a fresh attempt."""
        if plan_ref is None:
            return None
        assert (
            plan_ref.name in self._plan_ids
        ), f"Plan {plan_ref.name!r} is named before a failure opened it."
        return self._plan_ids[plan_ref.name]

    def bind(self, plan_ref: PlanRef, plan_id: PlanId) -> None:
        """Bind the plan ID a failure just opened to ``plan_ref``."""
        assert plan_id not in self._plan_ids.values(), (
            f"Plan {plan_id!r} opened for {plan_ref.name!r} is already in flight "
            f"as {self._plan_ids!r}."
        )
        self._plan_ids[plan_ref.name] = plan_id


def run_actions(actions: Sequence[Action]) -> None:
    """Run ``actions`` in order against a fresh tracker."""
    runner = _ActionRunner()
    for step, action in enumerate(actions):
        try:
            action.apply(runner)
        except (AssertionError, ValueError) as error:
            raise AssertionError(
                f"action {step} failed: {action}\n\n" f"{type(error).__name__}: {error}"
            ) from error


_SEED_TASK_IDS = ["seed_task_0", "seed_task_1"]
_CHILD_TASK_ID: DataTaskId = "child_task"
#: The plan opened by the failure of the child every fan-in case fails.
_CHILD_PLAN = PlanRef("child_plan")


def test_fan_in_child_fail_recovers_all_seeds_and_reuse_status():
    """Fail a child that fans in from two seeds, then recover both seeds.

    Graph: ``seed_task_0 -> child_task <- seed_task_1``
    """
    run_actions(
        [
            Submit(_SEED_TASK_IDS[0]),
            Complete(_SEED_TASK_IDS[0]),
            Submit(_SEED_TASK_IDS[1]),
            Complete(_SEED_TASK_IDS[1]),
            Submit(_CHILD_TASK_ID, {_SEED_TASK_IDS[0]: [0], _SEED_TASK_IDS[1]: [0]}),
            # Reconstruction traces up to both of the child's seed parents.
            Fail(
                _CHILD_TASK_ID,
                expected_seed_task_ids=_SEED_TASK_IDS,
                plan=_CHILD_PLAN,
            ),
            # Recover the seeds one at a time.
            Submit(_SEED_TASK_IDS[0], plan=_CHILD_PLAN),
            ExpectPendingChildren(
                _SEED_TASK_IDS[0],
                _CHILD_PLAN,
                {_CHILD_TASK_ID: {_SEED_TASK_IDS[0]: [0], _SEED_TASK_IDS[1]: [0]}},
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[0], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            Complete(_SEED_TASK_IDS[0], plan=_CHILD_PLAN),
            Submit(_SEED_TASK_IDS[1], plan=_CHILD_PLAN),
            ExpectPendingChildren(
                _SEED_TASK_IDS[1],
                _CHILD_PLAN,
                {_CHILD_TASK_ID: {_SEED_TASK_IDS[0]: [0], _SEED_TASK_IDS[1]: [0]}},
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[1], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            Complete(_SEED_TASK_IDS[1], plan=_CHILD_PLAN),
            # One resubmission of the child discharges both seeds at once.
            Submit(
                _CHILD_TASK_ID,
                {_SEED_TASK_IDS[0]: [0], _SEED_TASK_IDS[1]: [0]},
                plan=_CHILD_PLAN,
            ),
            ExpectPendingChildren(_CHILD_TASK_ID, _CHILD_PLAN, {}),
            ExpectObjectReuseStatus(
                _CHILD_TASK_ID, 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_NEW
            ),
            ExpectPendingChildren(_SEED_TASK_IDS[0], _CHILD_PLAN, {}),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[0], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_UNRELATED
            ),
            ExpectPendingChildren(_SEED_TASK_IDS[1], _CHILD_PLAN, {}),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[1], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_UNRELATED
            ),
        ]
    )


def test_fan_in_child_fail_recovers_all_seeds_and_reuse_status_multi_input():
    """Same as the single-input fan-in case, but with multiple outputs per seed.

    Graph: ``seed_task_0 -> child_task <- seed_task_1``
    """
    run_actions(
        [
            Submit(_SEED_TASK_IDS[0]),
            Complete(_SEED_TASK_IDS[0]),
            Submit(_SEED_TASK_IDS[1]),
            Complete(_SEED_TASK_IDS[1]),
            Submit(
                _CHILD_TASK_ID,
                {_SEED_TASK_IDS[0]: [0, 1], _SEED_TASK_IDS[1]: [0, 1]},
            ),
            Fail(
                _CHILD_TASK_ID,
                expected_seed_task_ids=_SEED_TASK_IDS,
                plan=_CHILD_PLAN,
            ),
            Submit(_SEED_TASK_IDS[0], plan=_CHILD_PLAN),
            ExpectPendingChildren(
                _SEED_TASK_IDS[0],
                _CHILD_PLAN,
                {
                    _CHILD_TASK_ID: {
                        _SEED_TASK_IDS[0]: [0, 1],
                        _SEED_TASK_IDS[1]: [0, 1],
                    }
                },
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[0], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[0], 1, _CHILD_PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            Complete(_SEED_TASK_IDS[0], plan=_CHILD_PLAN),
            Submit(_SEED_TASK_IDS[1], plan=_CHILD_PLAN),
            ExpectPendingChildren(
                _SEED_TASK_IDS[1],
                _CHILD_PLAN,
                {
                    _CHILD_TASK_ID: {
                        _SEED_TASK_IDS[0]: [0, 1],
                        _SEED_TASK_IDS[1]: [0, 1],
                    }
                },
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[1], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[1], 1, _CHILD_PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            Complete(_SEED_TASK_IDS[1], plan=_CHILD_PLAN),
            Submit(
                _CHILD_TASK_ID,
                {_SEED_TASK_IDS[0]: [0, 1], _SEED_TASK_IDS[1]: [0, 1]},
                plan=_CHILD_PLAN,
            ),
            ExpectPendingChildren(_CHILD_TASK_ID, _CHILD_PLAN, {}),
            ExpectObjectReuseStatus(
                _CHILD_TASK_ID, 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_NEW
            ),
            ExpectPendingChildren(_SEED_TASK_IDS[0], _CHILD_PLAN, {}),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[0], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_UNRELATED
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[0], 1, _CHILD_PLAN, ObjectReuseStatus.OBJECT_UNRELATED
            ),
            ExpectPendingChildren(_SEED_TASK_IDS[1], _CHILD_PLAN, {}),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[1], 0, _CHILD_PLAN, ObjectReuseStatus.OBJECT_UNRELATED
            ),
            ExpectObjectReuseStatus(
                _SEED_TASK_IDS[1], 1, _CHILD_PLAN, ObjectReuseStatus.OBJECT_UNRELATED
            ),
        ]
    )


_SEED: DataTaskId = "seed_task"
_LEFT: DataTaskId = "left_task"
_RIGHT: DataTaskId = "right_task"
_COMMON: DataTaskId = "common_task"
_PLAN = PlanRef("diamond_plan")

_PENDING_COMMON_CHILD = {_COMMON: {_LEFT: [0], _RIGHT: [0]}}

_BUILD_DIAMOND = [
    Submit(_SEED),
    Submit(_LEFT, {_SEED: [0]}),
    Submit(_RIGHT, {_SEED: [1]}),
    # Every consumer of the seed's outputs has been submitted, so it can complete.
    Complete(_SEED),
    Submit(_COMMON, {_LEFT: [0], _RIGHT: [0]}),
    Complete(_LEFT),
    Complete(_RIGHT),
]

_REPLAY_SEED_FOR_BOTH_BRANCHES = [
    Submit(_SEED, plan=_PLAN),
    ExpectPendingChildren(_SEED, _PLAN, {_LEFT: {_SEED: [0]}, _RIGHT: {_SEED: [1]}}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
    # Seed task can complete (finish outputting all blocks) before any branch
    # is resubmitted.
    Complete(_SEED, plan=_PLAN),
    # First output of the seed is ready to be scheduled.
    Submit(_LEFT, {_SEED: [0]}, plan=_PLAN),
    ExpectPendingChildren(_SEED, _PLAN, {_RIGHT: {_SEED: [1]}}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_PRUNED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
    # Second output of the seed is ready to be scheduled.
    Submit(_RIGHT, {_SEED: [1]}, plan=_PLAN),
    ExpectPendingChildren(_SEED, _PLAN, {}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    # Right branch finishes first.
    ExpectPendingChildren(_RIGHT, _PLAN, _PENDING_COMMON_CHILD),
    # Left branch finishes second.
    ExpectPendingChildren(_LEFT, _PLAN, _PENDING_COMMON_CHILD),
]

_REPLAY_SEED_FOR_LEFT_BRANCH = [
    Submit(_SEED, plan=_PLAN),
    # First output of the seed is ready to be scheduled.
    ExpectPendingChildren(_SEED, _PLAN, {_LEFT: {_SEED: [0]}}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_PRUNED),
    Complete(_SEED, plan=_PLAN),
    # Left branch is submitted.
    Submit(_LEFT, {_SEED: [0]}, plan=_PLAN),
    ExpectPendingChildren(_SEED, _PLAN, {}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    ExpectPendingChildren(_LEFT, _PLAN, _PENDING_COMMON_CHILD),
]

_REPLAY_SEED_FOR_RIGHT_BRANCH = [
    Submit(_SEED, plan=_PLAN),
    # First output of the seed is ready to be scheduled.
    ExpectPendingChildren(_SEED, _PLAN, {_RIGHT: {_SEED: [1]}}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_PRUNED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
    Complete(_SEED, plan=_PLAN),
    # Right branch is submitted.
    Submit(_RIGHT, {_SEED: [1]}, plan=_PLAN),
    ExpectPendingChildren(_SEED, _PLAN, {}),
    ExpectObjectReuseStatus(_SEED, 0, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    ExpectObjectReuseStatus(_SEED, 1, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    ExpectPendingChildren(_RIGHT, _PLAN, _PENDING_COMMON_CHILD),
]

_COMPLETE_LEFT_ATTEMPT = [
    Complete(_LEFT, plan=_PLAN),
    ExpectPendingChildren(_LEFT, _PLAN, _PENDING_COMMON_CHILD),
    ExpectObjectReuseStatus(_LEFT, 0, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
]

_COMPLETE_RIGHT_ATTEMPT = [
    Complete(_RIGHT, plan=_PLAN),
    ExpectPendingChildren(_RIGHT, _PLAN, _PENDING_COMMON_CHILD),
    ExpectObjectReuseStatus(_RIGHT, 0, _PLAN, ObjectReuseStatus.OBJECT_REUSED),
]

_RESUBMIT_COMMON_CHILD = [
    Submit(_COMMON, {_LEFT: [0], _RIGHT: [0]}, plan=_PLAN),
    ExpectPendingChildren(_LEFT, _PLAN, {}),
    ExpectObjectReuseStatus(_LEFT, 0, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
    ExpectPendingChildren(_RIGHT, _PLAN, {}),
    ExpectObjectReuseStatus(_RIGHT, 0, _PLAN, ObjectReuseStatus.OBJECT_UNRELATED),
]

_COMPLETE_COMMON_CHILD_RECONSTRUCTION = [
    ExpectPendingChildren(_COMMON, _PLAN, {}),
    ExpectObjectReuseStatus(_COMMON, 0, _PLAN, ObjectReuseStatus.OBJECT_NEW),
    Complete(_COMMON, plan=_PLAN),
]

_FAIL_COMMON_CHILD = Fail(_COMMON, expected_seed_task_ids=[_SEED], plan=_PLAN)

_DIAMOND_RECOVERY_CASES = [
    pytest.param(
        [
            *_BUILD_DIAMOND,
            _FAIL_COMMON_CHILD,
            *_REPLAY_SEED_FOR_BOTH_BRANCHES,
            *_COMPLETE_RIGHT_ATTEMPT,
            *_COMPLETE_LEFT_ATTEMPT,
            *_RESUBMIT_COMMON_CHILD,
            *_COMPLETE_COMMON_CHILD_RECONSTRUCTION,
        ],
        id="common_child_fails_once_and_reconstructs_diamond",
    ),
    pytest.param(
        [
            *_BUILD_DIAMOND,
            _FAIL_COMMON_CHILD,
            *_REPLAY_SEED_FOR_BOTH_BRANCHES,
            *_COMPLETE_RIGHT_ATTEMPT,
            *_COMPLETE_LEFT_ATTEMPT,
            *_RESUBMIT_COMMON_CHILD,
            _FAIL_COMMON_CHILD,
            *_REPLAY_SEED_FOR_BOTH_BRANCHES,
            *_COMPLETE_RIGHT_ATTEMPT,
            *_COMPLETE_LEFT_ATTEMPT,
            *_RESUBMIT_COMMON_CHILD,
            *_COMPLETE_COMMON_CHILD_RECONSTRUCTION,
        ],
        id="common_child_fails_twice_and_reconstructs_diamond_each_time",
    ),
    pytest.param(
        [
            *_BUILD_DIAMOND,
            _FAIL_COMMON_CHILD,
            *_REPLAY_SEED_FOR_BOTH_BRANCHES,
            Fail(_LEFT, expected_seed_task_ids=[_SEED], plan=_PLAN),
            *_REPLAY_SEED_FOR_LEFT_BRANCH,
            *_COMPLETE_LEFT_ATTEMPT,
            *_COMPLETE_RIGHT_ATTEMPT,
            *_RESUBMIT_COMMON_CHILD,
            *_COMPLETE_COMMON_CHILD_RECONSTRUCTION,
        ],
        id="common_child_fails_and_left_branch_fails_mid_reconstruction",
    ),
    pytest.param(
        [
            *_BUILD_DIAMOND,
            _FAIL_COMMON_CHILD,
            *_REPLAY_SEED_FOR_BOTH_BRANCHES,
            *_COMPLETE_LEFT_ATTEMPT,
            *_RESUBMIT_COMMON_CHILD,
            _FAIL_COMMON_CHILD,
            *_REPLAY_SEED_FOR_BOTH_BRANCHES,
            # The stale attempt from the first round, then the fresh one.
            *_COMPLETE_RIGHT_ATTEMPT,
            *_COMPLETE_RIGHT_ATTEMPT,
            *_COMPLETE_LEFT_ATTEMPT,
            *_RESUBMIT_COMMON_CHILD,
            *_COMPLETE_COMMON_CHILD_RECONSTRUCTION,
        ],
        id="common_child_fails_twice_but_first_right_reconstruction_is_still_in_progress",
    ),
]


@pytest.mark.parametrize("actions", _DIAMOND_RECOVERY_CASES)
def test_diamond_failure_recovery(actions: List[Action]):
    """Fail tasks in a diamond and rebuild it

    Graph: ``seed_task -> {left_task, right_task} -> common_task``, where the seed
    fans out over two disjoint output blocks that fan back in to a single common
    child.
    """
    run_actions(actions)


@dataclass(frozen=True)
class AllToAllGraph:
    """A stack of stages wired all to all, one width in ``layer_widths`` per stage.

    Every task of a stage produces one output block per consumer in the stage
    below, and every consumer takes one block -- the one at its own position --
    from every task of the stage above. ``(1, 2, 2, 1)`` is therefore a seed that
    fans out to two children, which both feed both grandchildren, which both feed
    a single sink.
    """

    layer_widths: Tuple[int, ...]

    @property
    def num_stages(self) -> int:
        return len(self.layer_widths)

    @property
    def id(self) -> str:
        """The shape, as it reads in the test id of every case built on it."""
        return "widths_" + "_".join(str(width) for width in self.layer_widths)

    def task(self, stage: int, position: int) -> DataTaskId:
        return f"stage_{stage}_task_{position}"

    def positions(self, stage: int) -> List[int]:
        """Every task position within ``stage``."""
        return list(range(self.layer_widths[stage]))

    def tasks(self, stage: int) -> List[DataTaskId]:
        return [self.task(stage, position) for position in self.positions(stage)]

    def inputs(self, stage: int, position: int) -> Dict[DataTaskId, List[OutputIndex]]:
        """The blocks the task at ``position`` in ``stage`` takes from above.

        Every task of the stage above produces one output block per consumer, and
        this consumer takes the block at its own position from every one of them.
        """
        if stage == 0:
            return {}
        return {parent_task_id: [position] for parent_task_id in self.tasks(stage - 1)}


_FIRST_FAILURE_PLAN = PlanRef("plan_of_first_failed_task")
_SECOND_FAILURE_PLAN = PlanRef("plan_of_second_failed_task")


def _submit_stage(
    graph: AllToAllGraph,
    stage: int,
    positions: Optional[Sequence[int]] = None,
    plan: Optional[PlanRef] = None,
) -> List[Action]:
    """Submit the tasks of ``stage`` against the outputs of the stage above.

    Args:
        graph: The graph to submit the stage of.
        stage: The stage containing the tasks to submit.
        positions: The positions of tasks to submit. If not provided, all positions in the stage are submitted.
        plan: The plan to submit the stage against. If not provided, no plan is submitted.

    Returns:
        The sequence of submit actions to perform.
    """
    if positions is None:
        positions = graph.positions(stage)
    return [
        Submit(graph.task(stage, position), graph.inputs(stage, position), plan=plan)
        for position in positions
    ]


def _complete_stage(
    graph: AllToAllGraph,
    stage: int,
    positions: Optional[Sequence[int]] = None,
    plan: Optional[PlanRef] = None,
) -> List[Action]:
    """Complete the tasks of ``stage``.

    Args:
        graph: The graph to complete the stage of.
        stage: The stage to complete.
        positions: The positions to complete. If not provided, all positions in the stage are completed.
        plan: The plan to complete the stage against. If not provided, no plan is completed.

    Returns:
        The sequence of complete actions to perform.
    """
    if positions is None:
        positions = graph.positions(stage)
    return [Complete(graph.task(stage, position), plan=plan) for position in positions]


def _fail(graph: AllToAllGraph, stage: int, position: int, plan: PlanRef) -> Action:
    """Fail one task of ``stage``.

    Args:
        graph: The graph to fail the task of.
        stage: The stage to fail the task of.
        position: The position of the task to fail.
        plan: The plan to fail the task against.

    Returns:
        The sequence of fail actions to perform.
    """
    return Fail(
        graph.task(stage, position),
        expected_seed_task_ids=sorted(graph.tasks(0)),
        plan=plan,
    )


def _expect_claims_on_stage(
    graph: AllToAllGraph,
    stage: int,
    plan: PlanRef,
    pending_positions: Sequence[int],
) -> List[Action]:
    """Verify the given stage only
    has child dependencies corresponding to the given pending positions.

    Args:
        graph: The graph to expect the claims on.
        stage: The stage to check the child dependencies of.
        plan: The plan to expect the claims on.
        pending_positions: The positions to expect the claims on.

    Returns:
        The sequence of expect actions to perform.
    """
    consumer_stage = stage + 1
    expected_pending_consumers = {
        graph.task(consumer_stage, position): {
            parent_task_id: [position] for parent_task_id in graph.tasks(stage)
        }
        for position in pending_positions
    }
    actions: List[Action] = []
    for parent_task_id in graph.tasks(stage):
        actions.append(
            ExpectPendingChildren(parent_task_id, plan, expected_pending_consumers)
        )
        for position in graph.positions(consumer_stage):
            if not pending_positions:
                expected_status = ObjectReuseStatus.OBJECT_UNRELATED
            elif position in pending_positions:
                expected_status = ObjectReuseStatus.OBJECT_REUSED
            else:
                expected_status = ObjectReuseStatus.OBJECT_PRUNED
            actions.append(
                ExpectObjectReuseStatus(parent_task_id, position, plan, expected_status)
            )
    return actions


def _sink_fails(graph: AllToAllGraph) -> List[Action]:
    sink_task_id = graph.task(3, 0)
    actions = [
        # Build the graph down to the sink
        *_submit_stage(graph, 0),
        *_submit_stage(graph, 1),
        *_complete_stage(graph, 0),
        *_submit_stage(graph, 2),
        *_complete_stage(graph, 1),
        *_submit_stage(graph, 3),
        *_complete_stage(graph, 2),
        _fail(graph, stage=3, position=0, plan=_FIRST_FAILURE_PLAN),
        # Trigger seed stage reconstruction
        *_submit_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
    ]
    for stage in (1, 2):
        for position in graph.positions(stage):
            # We expext each position within the stage to be a pending dependency of the stage above.
            actions += _expect_claims_on_stage(
                graph,
                stage - 1,
                _FIRST_FAILURE_PLAN,
                pending_positions=graph.positions(stage)[position:],
            )
            actions += _submit_stage(graph, stage, [position], _FIRST_FAILURE_PLAN)
            # Once the position is submitted, we expect the stage above to no longer owe it.
            actions += _expect_claims_on_stage(
                graph,
                stage - 1,
                _FIRST_FAILURE_PLAN,
                pending_positions=graph.positions(stage)[position + 1 :],
            )
        actions += _complete_stage(graph, stage, plan=_FIRST_FAILURE_PLAN)
    actions += [
        # We expect the sink to be the only task of the stage above that is a pending dependency.
        *_expect_claims_on_stage(graph, 2, _FIRST_FAILURE_PLAN, pending_positions=[0]),
        *_submit_stage(graph, 3, [0], _FIRST_FAILURE_PLAN),
        # Once resolved, the sink should no longer be a pending dependency.
        *_expect_claims_on_stage(graph, 2, _FIRST_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(sink_task_id, _FIRST_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            sink_task_id, 0, _FIRST_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 3, [0], plan=_FIRST_FAILURE_PLAN),
    ]
    return actions


def _two_grandchildren_fail(graph: AllToAllGraph) -> List[Action]:
    first_task_id, second_task_id = graph.task(2, 0), graph.task(2, 1)
    actions = [
        *_submit_stage(graph, 0),
        *_submit_stage(graph, 1),
        *_complete_stage(graph, 0),
        *_submit_stage(graph, 2),
        *_complete_stage(graph, 1),
        # The tasks of the failed stage that never fail finished first.
        *_complete_stage(graph, 2, graph.positions(2)[2:]),
        _fail(graph, stage=2, position=0, plan=_FIRST_FAILURE_PLAN),
        _fail(graph, stage=2, position=1, plan=_SECOND_FAILURE_PLAN),
        # The seed stage re-executes once, registered against both plans.
        *_submit_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_submit_stage(graph, 0, plan=_SECOND_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_SECOND_FAILURE_PLAN),
    ]
    # Reconstruct child stage for each plan.
    for plan in (_FIRST_FAILURE_PLAN, _SECOND_FAILURE_PLAN):
        actions += _expect_claims_on_stage(
            graph, 0, plan, pending_positions=graph.positions(1)
        )
        for position in graph.positions(1):
            actions += _submit_stage(graph, 1, [position], plan)
            actions += _expect_claims_on_stage(
                graph, 0, plan, pending_positions=graph.positions(1)[position + 1 :]
            )
        actions += _complete_stage(graph, 1, plan=plan)
    actions += [
        # The first plan should only have the first task as a pending dependency.
        *_expect_claims_on_stage(graph, 1, _FIRST_FAILURE_PLAN, pending_positions=[0]),
        *_submit_stage(graph, 2, [0], _FIRST_FAILURE_PLAN),
        *_expect_claims_on_stage(graph, 1, _FIRST_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(first_task_id, _FIRST_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            first_task_id, 0, _FIRST_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 2, [0], plan=_FIRST_FAILURE_PLAN),
        # The second plan should only have the second task as a pending dependency.
        *_expect_claims_on_stage(graph, 1, _SECOND_FAILURE_PLAN, pending_positions=[1]),
        *_submit_stage(graph, 2, [1], _SECOND_FAILURE_PLAN),
        *_expect_claims_on_stage(graph, 1, _SECOND_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(second_task_id, _SECOND_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            second_task_id, 0, _SECOND_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 2, [1], plan=_SECOND_FAILURE_PLAN),
        *_submit_stage(graph, 3),
        *_complete_stage(graph, 3),
    ]
    return actions


def _one_grandchild_fails(graph: AllToAllGraph) -> List[Action]:
    failed_task_id = graph.task(2, 0)
    actions = [
        *_submit_stage(graph, 0),
        *_submit_stage(graph, 1),
        *_complete_stage(graph, 0),
        *_submit_stage(graph, 2),
        *_complete_stage(graph, 1),
        # The tasks of the failed stage that never fail finished first.
        *_complete_stage(graph, 2, graph.positions(2)[1:]),
        _fail(graph, stage=2, position=0, plan=_FIRST_FAILURE_PLAN),
        # Trigger seed stage reconstruction
        *_submit_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
    ]
    # All tasks of the child stage should need to be reconstructed.
    actions += _expect_claims_on_stage(
        graph, 0, _FIRST_FAILURE_PLAN, pending_positions=graph.positions(1)
    )
    for position in graph.positions(1):
        actions += _submit_stage(graph, 1, [position], _FIRST_FAILURE_PLAN)
        actions += _expect_claims_on_stage(
            graph,
            0,
            _FIRST_FAILURE_PLAN,
            pending_positions=graph.positions(1)[position + 1 :],
        )
    actions += _complete_stage(graph, 1, plan=_FIRST_FAILURE_PLAN)
    actions += [
        # The failed task should be the only task of the grandchild stage that is a pending dependency
        # of the child stage.
        *_expect_claims_on_stage(graph, 1, _FIRST_FAILURE_PLAN, pending_positions=[0]),
        *_submit_stage(graph, 2, [0], _FIRST_FAILURE_PLAN),
        *_expect_claims_on_stage(graph, 1, _FIRST_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(failed_task_id, _FIRST_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            failed_task_id, 0, _FIRST_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 2, [0], plan=_FIRST_FAILURE_PLAN),
        *_submit_stage(graph, 3),
        *_complete_stage(graph, 3),
    ]
    return actions


def _two_children_fail(graph: AllToAllGraph) -> List[Action]:
    first_task_id, second_task_id = graph.task(1, 0), graph.task(1, 1)
    return [
        *_submit_stage(graph, 0),
        *_submit_stage(graph, 1),
        *_complete_stage(graph, 0),
        *_complete_stage(graph, 1, graph.positions(1)[2:]),
        _fail(graph, stage=1, position=0, plan=_FIRST_FAILURE_PLAN),
        _fail(graph, stage=1, position=1, plan=_SECOND_FAILURE_PLAN),
        # The seed stage re-executes once, registered against both plans.
        *_submit_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_submit_stage(graph, 0, plan=_SECOND_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_SECOND_FAILURE_PLAN),
        # The first plan should only have the first task as a pending dependency
        # of the seed stage.
        *_expect_claims_on_stage(graph, 0, _FIRST_FAILURE_PLAN, pending_positions=[0]),
        *_submit_stage(graph, 1, [0], _FIRST_FAILURE_PLAN),
        *_expect_claims_on_stage(graph, 0, _FIRST_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(first_task_id, _FIRST_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            first_task_id, 0, _FIRST_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 1, [0], plan=_FIRST_FAILURE_PLAN),
        # The second plan should only have the second task as a pending dependency
        # of the seed stage.
        *_expect_claims_on_stage(graph, 0, _SECOND_FAILURE_PLAN, pending_positions=[1]),
        *_submit_stage(graph, 1, [1], _SECOND_FAILURE_PLAN),
        *_expect_claims_on_stage(graph, 0, _SECOND_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(second_task_id, _SECOND_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            second_task_id, 0, _SECOND_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 1, [1], plan=_SECOND_FAILURE_PLAN),
        *_submit_stage(graph, 2),
        *_complete_stage(graph, 2),
        *_submit_stage(graph, 3),
        *_complete_stage(graph, 3),
    ]


def _one_child_fails(graph: AllToAllGraph) -> List[Action]:
    failed_task_id = graph.task(1, 0)
    return [
        *_submit_stage(graph, 0),
        *_submit_stage(graph, 1),
        *_complete_stage(graph, 0),
        # The tasks of the failed stage that never fail finished first.
        *_complete_stage(graph, 1, graph.positions(1)[1:]),
        _fail(graph, stage=1, position=0, plan=_FIRST_FAILURE_PLAN),
        # Trigger seed stage reconstruction
        *_submit_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        *_complete_stage(graph, 0, plan=_FIRST_FAILURE_PLAN),
        # The failed task should be the only task of the child stage that is a
        # pending dependency of the seed stage.
        *_expect_claims_on_stage(graph, 0, _FIRST_FAILURE_PLAN, pending_positions=[0]),
        *_submit_stage(graph, 1, [0], _FIRST_FAILURE_PLAN),
        *_expect_claims_on_stage(graph, 0, _FIRST_FAILURE_PLAN, pending_positions=[]),
        ExpectPendingChildren(failed_task_id, _FIRST_FAILURE_PLAN, {}),
        ExpectObjectReuseStatus(
            failed_task_id, 0, _FIRST_FAILURE_PLAN, ObjectReuseStatus.OBJECT_NEW
        ),
        *_complete_stage(graph, 1, [0], plan=_FIRST_FAILURE_PLAN),
        *_submit_stage(graph, 2),
        *_complete_stage(graph, 2),
        *_submit_stage(graph, 3),
        *_complete_stage(graph, 3),
    ]


_GRAPHS = [
    AllToAllGraph((1, 2, 2, 1)),
    AllToAllGraph((1, 5, 5, 1)),
]

_CASES = [
    _sink_fails,
    _two_grandchildren_fail,
    _one_grandchild_fails,
    _two_children_fail,
    _one_child_fails,
]

_ALL_TO_ALL_RECOVERY_CASES = [
    pytest.param(case(graph), id=f"{case.__name__.lstrip('_')}_{graph.id}")
    for graph in _GRAPHS
    for case in _CASES
]


@pytest.mark.parametrize("actions", _ALL_TO_ALL_RECOVERY_CASES)
def test_all_to_all_failure_recovery(actions: List[Action]):
    """Fail tasks in one stage of an all-to-all graph and rebuild only their branches.

    Graph: a stack of stages wired all to all (see :class:`AllToAllGraph`), run
    against a shape narrow enough to read and against a five-task-per-stage one.

    Every case is the same three phases -- build the graph down to the stage that
    fails, fail it and reconstruct, then resume execution and drain the rest of
    the graph -- and the cases between them vary which stage fails and whether one
    task of it fails or two.
    """
    run_actions(actions)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
