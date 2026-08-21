import sys
from typing import Dict, List

import pytest

from ray.data._internal.execution.lineage_tracker import (
    LineageTracker,
    ObjectReuseStatus,
    ParentBlockOutput,
)

# These tests only build linear chains -- one parent per task -- so the only axis
# that varies between otherwise identical scenarios is how many of the parent's
# output blocks the child consumes.
OUTPUT_INDICES = [[0], [0, 1]]


def _dependencies(
    parent_task_id: str, output_indices: List[int]
) -> List[ParentBlockOutput]:
    """Build the dependency edges from ``parent_task_id``'s output blocks."""
    return [
        ParentBlockOutput(parent_data_task_id=parent_task_id, output_index=output_index)
        for output_index in output_indices
    ]


def _register_linear_chain(
    tracker: LineageTracker, task_ids: List[str], output_indices: List[int]
) -> None:
    """Register a linear chain ``task_ids[0] -> ... -> task_ids[-1]``.

    Every task except the leaf is completed, so the leaf is left in flight for a
    test to fail.
    """
    tracker.register_task_submission(task_ids[0], dependencies=[])
    for parent_task_id, child_task_id in zip(task_ids, task_ids[1:]):
        tracker.register_task_submission(
            child_task_id,
            dependencies=_dependencies(parent_task_id, output_indices),
        )
        tracker.register_task_complete(parent_task_id)


def _assert_pending_children(
    tracker: LineageTracker,
    parent_task_id: str,
    plan_id: str,
    expected: Dict[str, Dict[str, List[int]]],
) -> None:
    """Assert the pending children of ``parent_task_id`` within ``plan_id``.

    Pending children are reported per plan, and each one maps to every parent it
    consumes blocks from -- in a linear chain that is only ``parent_task_id``.
    """
    assert tracker.get_pending_children(parent_task_id, plan_id=plan_id) == expected


def _assert_reuse_status(
    tracker: LineageTracker,
    data_task_id: str,
    output_indices: List[int],
    plan_id: str,
    expected_status: ObjectReuseStatus,
) -> None:
    """Assert every listed output block of ``data_task_id`` has ``expected_status``.

    Reuse status is always scoped to a plan: the same block can be reused by the
    plan reconstructing it and unrelated to every other plan.
    """
    for output_index in output_indices:
        assert (
            tracker.get_object_reuse_status(
                data_task_id, output_index=output_index, plan_id=plan_id
            )
            == expected_status
        )


def _assert_child_pending_on_parent(
    tracker: LineageTracker,
    parent_task_id: str,
    child_task_id: str,
    output_indices: List[int],
    plan_id: str,
) -> None:
    """Assert the child is the parent's only pending child in ``plan_id``."""
    _assert_pending_children(
        tracker,
        parent_task_id,
        plan_id,
        {child_task_id: {parent_task_id: output_indices}},
    )
    _assert_reuse_status(
        tracker,
        parent_task_id,
        output_indices,
        plan_id,
        ObjectReuseStatus.OBJECT_REUSED,
    )


def _assert_parent_discharged(
    tracker: LineageTracker,
    parent_task_id: str,
    output_indices: List[int],
    plan_id: str,
) -> None:
    """Assert ``plan_id`` no longer claims ``parent_task_id``.

    Once a parent completes its re-execution for a plan, the plan stops claiming
    it: it has no pending children left and its outputs are not part of the plan
    at all, so they report OBJECT_UNRELATED rather than reused or pruned.
    """
    _assert_pending_children(tracker, parent_task_id, plan_id, {})
    _assert_reuse_status(
        tracker,
        parent_task_id,
        output_indices,
        plan_id,
        ObjectReuseStatus.OBJECT_UNRELATED,
    )


def _assert_target_produces_new_objects(
    tracker: LineageTracker,
    target_task_id: str,
    output_indices: List[int],
    plan_id: str,
) -> None:
    """Assert the plan's target owes nothing downstream and its outputs are new."""
    _assert_pending_children(tracker, target_task_id, plan_id, {})
    _assert_reuse_status(
        tracker, target_task_id, output_indices, plan_id, ObjectReuseStatus.OBJECT_NEW
    )


def _reconstruct_edge(
    tracker: LineageTracker,
    parent_task_id: str,
    child_task_id: str,
    output_indices: List[int],
    plan_id: str,
) -> None:
    """Re-execute one ``parent -> child`` edge of ``plan_id``.

    The parent completes its re-execution for the plan and the child is
    resubmitted against the parent's fresh outputs, which discharges the plan's
    claim on the parent and hands reconstruction down to the child.
    """
    _assert_child_pending_on_parent(
        tracker, parent_task_id, child_task_id, output_indices, plan_id
    )

    tracker.register_task_complete(parent_task_id, plan_id=plan_id)
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(parent_task_id, output_indices)
    )

    _assert_parent_discharged(tracker, parent_task_id, output_indices, plan_id)


def test_seed_task_fail_resubmit_and_complete():
    """Fail a dependency-less seed task, resubmit it, then complete it.

    Verifies that:
      - Failing the seed task returns the seed task itself for retry, along with
        the plan id every step of the recovery is keyed by.
      - After resubmission it has no pending children.
      - Its output object reuse status is OBJECT_NEW (no child depends on it).
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"

    # Register task submission for the seed task with no dependency.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Fail the task; the failed seed task should be returned for retry.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(seed_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Register the seed task for submission again to emulate resubmission.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # A seed task with no children has no pending children, and with no child
    # depending on its outputs they are newly produced.
    _assert_target_produces_new_objects(tracker, seed_task_id, [0], plan_id)

    # The seed's own re-execution closes out the plan.
    tracker.register_task_complete(seed_task_id, plan_id=plan_id)


def test_unregistered_task_raises():
    """Every entry point rejects task ids the tracker has never seen.

    The tracker's invariant is that a task is registered before it reaches a
    terminal state or is queried, so unknown ids are a caller error.
    """
    tracker = LineageTracker()

    with pytest.raises(ValueError):
        tracker.register_task_complete("unknown_task")
    with pytest.raises(ValueError):
        tracker.register_failed_task("unknown_task")
    with pytest.raises(ValueError):
        tracker.get_pending_children("unknown_task", plan_id="unknown_plan")
    with pytest.raises(ValueError):
        tracker.get_object_reuse_status(
            "unknown_task", output_index=0, plan_id="unknown_plan"
        )


def test_complete_with_unregistered_plan_raises():
    """Completing a task for a plan that never claimed it is a caller error.

    A task only carries a plan while it is reconstructing for it, so completing
    against an unknown plan id means either the plan was never traced through
    this task or it already completed for it -- there is never more than one
    reconstruction of the same task in flight.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"

    tracker.register_task_submission(seed_task_id, dependencies=[])
    with pytest.raises(ValueError):
        tracker.register_task_complete(seed_task_id, plan_id="unknown_plan")

    # Completing for the plan that does claim the task is fine, but only once:
    # the completion discharges the plan's claim on the task.
    _, plan_id = tracker.register_failed_task(seed_task_id)
    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_complete(seed_task_id, plan_id=plan_id)
    with pytest.raises(ValueError):
        tracker.register_task_complete(seed_task_id, plan_id=plan_id)


def test_submission_with_unregistered_parent_raises():
    """A child cannot be submitted before its parent is registered.

    The invariant is that a task is only submitted once its parent has been
    registered, so a dependency on an unknown parent is a caller error.
    """
    tracker = LineageTracker()

    with pytest.raises(ValueError):
        tracker.register_task_submission(
            "child_task", dependencies=_dependencies("unregistered_parent", [0])
        )


def test_target_consumed_output_pruned_and_unconsumed_output_new():
    """The reconstruction target re-emits only the outputs nothing took.

    The child consumes only output 0, so when the seed is the target of
    reconstruction that block is OBJECT_PRUNED -- the child already fetched a
    copy of those rows, and re-emitting them would duplicate. Output 1, which
    nothing downstream consumed, is OBJECT_NEW and free to be handed to anyone.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_id = "child_task"

    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(seed_task_id, [0])
    )
    tracker.register_task_complete(seed_task_id)

    # Fail the seed itself, so the seed is the target of the resulting plan.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(seed_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    tracker.register_task_submission(seed_task_id, dependencies=[])

    _assert_reuse_status(
        tracker, seed_task_id, [0], plan_id, ObjectReuseStatus.OBJECT_PRUNED
    )
    _assert_reuse_status(
        tracker, seed_task_id, [1], plan_id, ObjectReuseStatus.OBJECT_NEW
    )


def test_reuse_status_of_task_outside_the_plan_is_unrelated():
    """A task no plan claims reports OBJECT_UNRELATED for every output.

    Reuse status is only meaningful relative to a reconstruction, so querying a
    task that is not part of the given plan -- here a child queried against a
    plan that only traces through its parent -- yields neither reused nor pruned.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_id = "child_task"

    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(seed_task_id, [0])
    )
    tracker.register_task_complete(seed_task_id)

    # The plan targets the seed, so it never traces down into the child.
    _, plan_id = tracker.register_failed_task(seed_task_id)

    _assert_reuse_status(
        tracker, child_task_id, [0, 1], plan_id, ObjectReuseStatus.OBJECT_UNRELATED
    )


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_child_task_fail_recovers_seed_and_reuse_status(output_indices: List[int]):
    """Fail a child that depends on the seed's outputs, then recover.

    Verifies that:
      - Failing the child returns the seed task for retry (the chain root) along
        with the plan id every step of the recovery is keyed by.
      - After the seed is resubmitted, the still-pending child shows up as a
        pending child of the seed, reporting every depended-on output index.
      - Each of the seed's depended-on outputs is OBJECT_REUSED.
      - Completing the seed's re-execution for the plan discharges the plan's
        claim on it, so it has no pending children left and its outputs report
        OBJECT_UNRELATED, while the child -- the plan's target -- produces new
        objects since nothing consumes them.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_id = "child_task"

    # Register the seed task (no dependencies).
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Register a child task depending on the seed's output blocks, then complete
    # the seed now that every consumer of its outputs has been submitted.
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(seed_task_id, output_indices)
    )
    tracker.register_task_complete(seed_task_id)

    # Fail the child; reconstruction traces back to the seed as the retry root.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(child_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit the seed to emulate recovery. The child is still pending
    # reconstruction, so it is a pending child of the seed depending on every
    # output index it consumes, and each of those outputs is reused.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _assert_child_pending_on_parent(
        tracker, seed_task_id, child_task_id, output_indices, plan_id
    )

    # Re-executing the seed and resubmitting the child hands reconstruction down
    # the edge and retires the plan's claim on the seed.
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices, plan_id)

    # The child is the leaf of this chain, so nothing depends on its outputs.
    _assert_target_produces_new_objects(tracker, child_task_id, [0], plan_id)

    # The child's re-execution closes out the plan.
    tracker.register_task_complete(child_task_id, plan_id=plan_id)


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_child_task_fail_recovers_seed_when_submitted_after_parent_complete(
    output_indices: List[int],
):
    """Recover a failed child that was submitted after its parent completed.

    The variant of ``test_child_task_fail_recovers_seed_and_reuse_status`` in
    which the seed completes *before* the child is submitted against its outputs,
    modeling an operator that only submits a downstream task once the upstream
    task has finished producing blocks.

    Recovery is expected to be indistinguishable from the in-flight ordering: the
    lineage edge is recorded when the child is submitted, and a parent that has
    already completed still reports the child as pending and its consumed outputs
    as OBJECT_REUSED.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_id = "child_task"

    # Register and complete the seed task (no dependencies) before any child
    # consumes its outputs.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_complete(seed_task_id)

    # Only now register a child task depending on the completed seed's outputs.
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(seed_task_id, output_indices)
    )

    # Fail the child; reconstruction traces back to the seed as the retry root.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(child_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit the seed to emulate recovery. The child is still pending
    # reconstruction, so it is a pending child of the seed depending on every
    # output index it consumes, and each of those outputs is reused.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _assert_child_pending_on_parent(
        tracker, seed_task_id, child_task_id, output_indices, plan_id
    )

    # Re-executing the seed and resubmitting the child hands reconstruction down
    # the edge and retires the plan's claim on the seed.
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices, plan_id)

    # The child is the leaf of this chain, so nothing depends on its outputs.
    _assert_target_produces_new_objects(tracker, child_task_id, [0], plan_id)

    # The child's re-execution closes out the plan.
    tracker.register_task_complete(child_task_id, plan_id=plan_id)


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_child_task_fail_twice_recovers_seed_and_reuse_status(
    output_indices: List[int],
):
    """Fail the child, recover the seed, then fail the child's retry as well.

    Verifies that a second failure of the same child is handled identically to
    the first:
      - The retry of the child traces back to the same seed task, and failing a
        re-execution keeps the same plan id rather than opening a second plan.
      - After the seed is resubmitted a second time, the child is again a pending
        child of the seed depending on the same output indices.
      - The seed's depended-on outputs are OBJECT_REUSED again, so the lineage
        edge survives a full failure/recovery round trip.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_id = "child_task"

    # Register the seed task (no dependencies).
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Register a child task depending on the seed's output blocks, then complete
    # the seed now that every consumer of its outputs has been submitted.
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(seed_task_id, output_indices)
    )
    tracker.register_task_complete(seed_task_id)

    # Fail the child; reconstruction traces back to the seed as the retry root.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(child_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit and complete the seed to emulate the first recovery, then resubmit
    # the child as part of reconstruction.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices, plan_id)

    # Fail the child again. The retry belongs to the same plan, so the failure is
    # reported against it and traces back to the same seed task.
    seed_tasks_to_retry, retry_plan_id = tracker.register_failed_task(
        child_task_id, plan_id=plan_id
    )
    assert seed_tasks_to_retry == [seed_task_id]
    assert retry_plan_id == plan_id

    # Resubmit the seed again to emulate the second recovery. The child is still
    # pending reconstruction, so the lineage edge survives the second round trip.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _assert_child_pending_on_parent(
        tracker, seed_task_id, child_task_id, output_indices, plan_id
    )

    # The child's second retry finally completes, closing out the plan.
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices, plan_id)
    _assert_target_produces_new_objects(tracker, child_task_id, [0], plan_id)
    tracker.register_task_complete(child_task_id, plan_id=plan_id)


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_deep_chain_leaf_fail_recovers_seed_and_reuse_status(output_indices: List[int]):
    """Fail the leaf of a 5-deep linear chain, then recover the whole chain.

    Chain: ``task_0 -> task_1 -> task_2 -> task_3 -> task_4``, each task
    consuming ``output_indices`` of its parent's outputs. Verifies that:
      - Failing the leaf traces all the way back to the seed as the single retry
        root and yields the plan id the whole recovery is keyed by.
      - Every task below the seed is pending, so each parent reports exactly its
        one pending child and each depended-on output is OBJECT_REUSED.
      - Reconstruction walks down the chain one edge at a time, each parent's
        outputs flipping to OBJECT_UNRELATED once it completes for the plan, and
        the leaf -- which nothing consumes -- produces new objects.
    """
    tracker = LineageTracker()
    task_ids = [f"task_{i}" for i in range(5)]
    seed_task_id, leaf_task_id = task_ids[0], task_ids[-1]

    _register_linear_chain(tracker, task_ids, output_indices=output_indices)

    # Fail the leaf; reconstruction traces up the whole chain to the seed.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(leaf_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit the seed to emulate recovery of the chain root.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Walk reconstruction down the chain one edge at a time.
    for parent_task_id, child_task_id in zip(task_ids, task_ids[1:]):
        _reconstruct_edge(
            tracker, parent_task_id, child_task_id, output_indices, plan_id
        )

    # The leaf has no children of its own.
    _assert_target_produces_new_objects(tracker, leaf_task_id, [0], plan_id)

    # The leaf's own re-execution closes out the reconstruction.
    tracker.register_task_complete(leaf_task_id, plan_id=plan_id)


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
@pytest.mark.parametrize("retry_failure_index", [2, 4])
def test_deep_chain_retry_fail_recovers_seed_and_reuse_status(
    retry_failure_index: int,
    output_indices: List[int],
):
    """Fail leaf task of a 5-deep chain, then fail again at
    retry_failure_index task during reconstruction.

    Chain: ``task_0 -> task_1 -> task_2 -> task_3 -> task_4``, each task
    consuming ``output_indices`` of its parent's outputs.

    Verifies that:
      - Failing the leaf traces all the way back to the seed as the retry root.
      - Reconstruction restarts from the seed and re-executes every task above the
        second failure point successfully.
      - The second failure is reported against the same plan and traces back to
        the same seed regardless of how deep in the chain it happens, re-claiming
        the tasks that had already completed for the plan: they are pending again
        with their parents' outputs OBJECT_REUSED once more.
      - After the second recovery the whole chain reconstructs as before.
    """
    tracker = LineageTracker()
    task_ids = [f"task_{i}" for i in range(5)]
    seed_task_id, leaf_task_id = task_ids[0], task_ids[-1]

    _register_linear_chain(tracker, task_ids, output_indices=output_indices)

    # Fail the leaf; reconstruction traces up the whole chain to the seed.
    seed_tasks_to_retry, plan_id = tracker.register_failed_task(leaf_task_id)
    assert seed_tasks_to_retry == [seed_task_id]
    assert plan_id is not None

    # Resubmit the seed to emulate the first recovery.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Walk reconstruction down to the second failure point: every task above it
    # re-executes successfully, and the last edge resubmits the task that is about
    # to fail against its parent's fresh outputs.
    for parent_task_id, child_task_id in zip(
        task_ids[:retry_failure_index], task_ids[1 : retry_failure_index + 1]
    ):
        _reconstruct_edge(
            tracker, parent_task_id, child_task_id, output_indices, plan_id
        )

    # The retry fails. It is part of the same plan, so the failure is reported
    # against it and reconstruction traces back to the same seed.
    seed_tasks_to_retry, retry_plan_id = tracker.register_failed_task(
        task_ids[retry_failure_index], plan_id=plan_id
    )
    assert seed_tasks_to_retry == [seed_task_id]
    assert retry_plan_id == plan_id

    # Resubmit the seed again to emulate the second recovery, then walk the full
    # reconstruction down the chain to completion.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    for parent_task_id, child_task_id in zip(task_ids, task_ids[1:]):
        _reconstruct_edge(
            tracker, parent_task_id, child_task_id, output_indices, plan_id
        )

    # The leaf has no children of its own.
    _assert_target_produces_new_objects(tracker, leaf_task_id, [0], plan_id)

    # The leaf's own re-execution closes out the reconstruction.
    tracker.register_task_complete(leaf_task_id, plan_id=plan_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
