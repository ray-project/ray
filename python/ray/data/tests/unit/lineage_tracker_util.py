from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ray.data._internal.execution.lineage_tracker import (
    BlockId,
    LineageTaskId,
    LineageTracker,
    ObjectReuseStatus,
    OutputIndex,
    ParentBlockOutput,
    ReconstructionPlanId,
)


def release_inputs_and_submit(
    tracker: LineageTracker,
    lineage_task_id: LineageTaskId,
    dependencies: List[ParentBlockOutput],
    reconstruction_plan_id: Optional[ReconstructionPlanId],
) -> None:
    """Submit a task the way the operator does.

    A re-execution only runs on input its parents released for the plan, so each
    input block is queued for the plan before the task is submitted. A fresh
    task is submitted as is.
    """
    if reconstruction_plan_id is not None:
        for dependency in dependencies:
            tracker.register_block_queued(
                dependency.parent_lineage_task_id,
                dependency.output_index,
                reconstruction_plan_id,
            )
    tracker.register_task_submission(
        lineage_task_id, dependencies, reconstruction_plan_id
    )


def _parent_block_outputs(
    dependencies: Mapping[LineageTaskId, Sequence[OutputIndex]]
) -> List[ParentBlockOutput]:
    """Flatten ``dependencies`` parent by parent, in the order they are listed."""
    return [
        ParentBlockOutput(
            parent_lineage_task_id=parent_task_id, output_index=output_index
        )
        for parent_task_id, output_indices in dependencies.items()
        for output_index in output_indices
    ]


@dataclass(frozen=True)
class PlanRef:
    """A stand-in for a reconstruction plan ID, which only the tracker can hand out.
    Used to get a reference to the reconstruction plan ID handed out by the tracker.
    """

    name: str


class Action:
    """One call against the tracker, and what that call must return.

    Actions are inert data: they are built once as part of a case and only
    :func:`run_actions` ever touches a tracker with them.
    """

    def apply(self, runner: "ActionRunner") -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class Submit(Action):
    """``register_task_submission``."""

    lineage_task_id: LineageTaskId
    dependencies: Mapping[LineageTaskId, Sequence[OutputIndex]] = field(
        default_factory=dict
    )
    plan: Optional[PlanRef] = None

    def apply(self, runner: "ActionRunner") -> None:
        release_inputs_and_submit(
            runner.tracker,
            self.lineage_task_id,
            _parent_block_outputs(self.dependencies),
            runner.resolve_optional(self.plan),
        )


@dataclass(frozen=True)
class SubmitBlocks(Action):
    """``resolve_dependencies`` on the input block IDs, then
    ``register_task_submission`` once per plan.

    Unlike :class:`Submit`, nothing is queued on the task's behalf: the blocks
    were queued as their producers emitted them. A task resubmitted for several
    plans at once runs as one reconstruction task, so its input is resolved once
    and registered under each plan. No plans means a fresh submission.
    """

    lineage_task_id: LineageTaskId
    block_ids: Sequence[BlockId]
    plans: Sequence[PlanRef] = ()

    def apply(self, runner: "ActionRunner") -> None:
        dependencies = runner.tracker.resolve_dependencies(self.block_ids)
        for reconstruction_plan_id in [runner.resolve(plan) for plan in self.plans] or [
            None
        ]:
            runner.tracker.register_task_submission(
                self.lineage_task_id, dependencies, reconstruction_plan_id
            )


@dataclass(frozen=True)
class OutputBlock(Action):
    """``register_block_output``."""

    lineage_task_id: LineageTaskId
    block_id: BlockId
    output_index: OutputIndex

    def apply(self, runner: "ActionRunner") -> None:
        runner.tracker.register_block_output(
            self.lineage_task_id, self.block_id, self.output_index
        )


@dataclass(frozen=True)
class QueueBlock(Action):
    """``register_block_queued``."""

    lineage_task_id: LineageTaskId
    output_index: OutputIndex
    #: The plan the block is released for, or ``None`` for a fresh attempt.
    plan: Optional[PlanRef] = None

    def apply(self, runner: "ActionRunner") -> None:
        runner.tracker.register_block_queued(
            self.lineage_task_id,
            self.output_index,
            runner.resolve_optional(self.plan),
        )


@dataclass(frozen=True)
class Complete(Action):
    """``register_task_complete``."""

    lineage_task_id: LineageTaskId
    #: How many output blocks the completed attempt produced.
    num_outputs: int
    #: The plan this completion is for, or ``None`` for a fresh attempt.
    plan: Optional[PlanRef] = None

    def apply(self, runner: "ActionRunner") -> None:
        runner.tracker.register_task_complete(
            self.lineage_task_id,
            num_outputs=self.num_outputs,
            reconstruction_plan_id=runner.resolve_optional(self.plan),
        )


@dataclass(frozen=True)
class Fail(Action):
    """``register_task_failed``, with the seed tasks it must report.

    The returned reconstruction plan ID is bound to the plan ref passed.
    All subsequent failures of the same reconstruction attempt
    must pass the same plan ref.
    """

    lineage_task_id: LineageTaskId
    expected_seed_task_ids: Sequence[LineageTaskId]
    plan: PlanRef

    def apply(self, runner: "ActionRunner") -> None:
        bound_reconstruction_plan_id = runner.bound_reconstruction_plan_id(self.plan)
        seed_task_ids, reconstruction_plan_id = runner.tracker.register_task_failed(
            self.lineage_task_id, reconstruction_plan_id=bound_reconstruction_plan_id
        )
        assert seed_task_ids == list(self.expected_seed_task_ids)
        assert reconstruction_plan_id is not None
        if bound_reconstruction_plan_id is None:
            runner.bind(self.plan, reconstruction_plan_id)
        else:
            assert reconstruction_plan_id == bound_reconstruction_plan_id


@dataclass(frozen=True)
class GarbageCollect(Action):
    """``try_garbage_collect_lineage``, with whether it must collect the task."""

    lineage_task_id: LineageTaskId
    expected_collected: bool = True

    def apply(self, runner: "ActionRunner") -> None:
        assert (
            runner.tracker.try_garbage_collect_lineage(self.lineage_task_id)
            == self.expected_collected
        )


@dataclass(frozen=True)
class RegisterSeedInput(Action):
    """``register_seed_input``."""

    lineage_task_id: LineageTaskId
    seed_input: Any

    def apply(self, runner: "ActionRunner") -> None:
        runner.tracker.register_seed_input(self.lineage_task_id, self.seed_input)


@dataclass(frozen=True)
class ExpectSeedInput(Action):
    """``get_seed_input``, with the very object it must return."""

    lineage_task_id: LineageTaskId
    expected: Any

    def apply(self, runner: "ActionRunner") -> None:
        assert runner.tracker.get_seed_input(self.lineage_task_id) is self.expected


@dataclass(frozen=True)
class ExpectResolvedDependencies(Action):
    """``resolve_dependencies``, with the parent outputs it must return."""

    block_ids: Sequence[BlockId]
    #: Each producer mapped to the output indices the blocks resolve to.
    expected: Mapping[LineageTaskId, Sequence[OutputIndex]]

    def apply(self, runner: "ActionRunner") -> None:
        assert runner.tracker.resolve_dependencies(
            self.block_ids
        ) == _parent_block_outputs(self.expected)


@dataclass(frozen=True)
class ExpectPendingChildren(Action):
    """``get_pending_children``, with the mapping it must return."""

    lineage_task_id: LineageTaskId
    plan: PlanRef
    #: Each pending child task ID mapped to the output indices every one of its
    #: parents produced for it, listed parent by parent in the child's input order.
    #: None of these scenarios interleave parents within a child's input, so the
    #: flattened mapping is the child's exact input order.
    expected: Mapping[LineageTaskId, Mapping[LineageTaskId, Sequence[OutputIndex]]]

    def apply(self, runner: "ActionRunner") -> None:
        pending_children = runner.tracker.get_pending_children(
            self.lineage_task_id, reconstruction_plan_id=runner.resolve(self.plan)
        )
        assert pending_children == {
            child_task_id: _parent_block_outputs(dependencies_by_parent)
            for child_task_id, dependencies_by_parent in self.expected.items()
        }


@dataclass(frozen=True)
class ExpectObjectReuseStatus(Action):
    """``get_object_reuse_status``, with the status it must return."""

    lineage_task_id: LineageTaskId
    output_index: OutputIndex
    plan: PlanRef
    expected: ObjectReuseStatus

    def apply(self, runner: "ActionRunner") -> None:
        assert (
            runner.tracker.get_object_reuse_status(
                self.lineage_task_id,
                output_index=self.output_index,
                reconstruction_plan_id=runner.resolve(self.plan),
            )
            == self.expected
        )


@dataclass(frozen=True)
class Raises(Action):
    """Run ``action`` and expect the tracker to reject it with a ``ValueError``."""

    action: Action

    def apply(self, runner: "ActionRunner") -> None:
        try:
            self.action.apply(runner)
        except ValueError:
            return
        raise AssertionError(f"Expected {self.action} to raise ValueError.")


class ActionRunner:
    """Runs actions against one tracker, resolving plan refs as it goes."""

    def __init__(self, tracker: LineageTracker) -> None:
        self.tracker = tracker
        self._reconstruction_plan_ids: Dict[str, ReconstructionPlanId] = {}

    def bound_reconstruction_plan_id(
        self, plan_ref: PlanRef
    ) -> Optional[ReconstructionPlanId]:
        """The reconstruction plan ID bound to ``plan_ref``, or ``None`` if none is yet."""
        return self._reconstruction_plan_ids.get(plan_ref.name)

    def resolve(self, plan_ref: PlanRef) -> ReconstructionPlanId:
        """The reconstruction plan ID ``plan_ref`` stands for. The plan must already be open."""
        assert (
            plan_ref.name in self._reconstruction_plan_ids
        ), f"Plan {plan_ref.name!r} is named before a failure opened it."
        return self._reconstruction_plan_ids[plan_ref.name]

    def resolve_optional(
        self, plan_ref: Optional[PlanRef]
    ) -> Optional[ReconstructionPlanId]:
        """The reconstruction plan ID ``plan_ref`` stands for, or ``None`` for a fresh attempt."""
        if plan_ref is None:
            return None
        return self.resolve(plan_ref)

    def bind(
        self, plan_ref: PlanRef, reconstruction_plan_id: ReconstructionPlanId
    ) -> None:
        """Bind the reconstruction plan ID a failure just opened to ``plan_ref``."""
        assert reconstruction_plan_id not in self._reconstruction_plan_ids.values(), (
            f"Plan {reconstruction_plan_id!r} opened for {plan_ref.name!r} is already in flight "
            f"as {self._reconstruction_plan_ids!r}."
        )
        self._reconstruction_plan_ids[plan_ref.name] = reconstruction_plan_id


def run_actions(
    actions: Sequence[Action], tracker: Optional[LineageTracker] = None
) -> None:
    """Run ``actions`` in order against ``tracker``, or a fresh one if not given.

    Plan refs are bound per call, so a plan opened in one call can't be named in
    the next.
    """
    runner = ActionRunner(tracker if tracker is not None else LineageTracker())
    for step, action in enumerate(actions):
        try:
            action.apply(runner)
        except (AssertionError, ValueError) as error:
            raise AssertionError(
                f"action {step} failed: {action}\n\n" f"{type(error).__name__}: {error}"
            ) from error
