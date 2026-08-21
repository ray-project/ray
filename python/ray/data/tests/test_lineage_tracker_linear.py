import sys
from typing import Dict, List

import pytest

from ray.data._internal.execution.lineage_tracker import (
    LineageTracker,
    ObjectReuseStatus,
    ParentBlockOutput,
)

# The linear tracker models one edge per parent, so the only axis that varies
# between otherwise identical scenarios is how many of the parent's output
# blocks the child consumes.
OUTPUT_INDICES = [[0], [0, 1]]


def _register_linear_chain(
    tracker: LineageTracker, task_ids: List[str], output_indices: List[int]
) -> None:
    """Register a linear chain ``task_ids[0] -> ... -> task_ids[-1]``.

    Every task except the leaf is completed,
    """
    tracker.register_task_submission(task_ids[0], dependencies=[])
    for parent_task_id, child_task_id in zip(task_ids, task_ids[1:]):
        tracker.register_task_submission(
            child_task_id,
            dependencies=[
                ParentBlockOutput(
                    parent_data_task_id=parent_task_id, output_index=output_index
                )
                for output_index in output_indices
            ],
        )
        tracker.register_task_complete(parent_task_id)


def _dependencies(
    parent_task_id: str, output_indices: List[int]
) -> List[ParentBlockOutput]:
    """Build the dependency edges from ``parent_task_id``'s output blocks."""
    return [
        ParentBlockOutput(parent_data_task_id=parent_task_id, output_index=output_index)
        for output_index in output_indices
    ]


def _assert_pending_children(
    tracker: LineageTracker,
    parent_task_id: str,
    expected: Dict[str, List[int]],
) -> None:
    """Assert the pending children of ``parent_task_id``."""
    assert tracker.get_pending_children(parent_task_id) == expected


def _assert_reuse_status(
    tracker: LineageTracker,
    data_task_id: str,
    output_indices: List[int],
    expected_status: ObjectReuseStatus,
) -> None:
    """Assert every listed output block of ``data_task_id`` has ``expected_status``."""
    for output_index in output_indices:
        assert (
            tracker.get_object_reuse_status(data_task_id, output_index=output_index)
            == expected_status
        )


def _assert_child_pending_on_parent(
    tracker: LineageTracker,
    parent_task_id: str,
    child_task_id: str,
    output_indices: List[int],
) -> None:
    """Assert the child is the parent's only pending child."""
    _assert_pending_children(tracker, parent_task_id, {child_task_id: output_indices})
    _assert_reuse_status(
        tracker, parent_task_id, output_indices, ObjectReuseStatus.OBJECT_REUSED
    )


def _assert_leaf_produces_new_objects(
    tracker: LineageTracker, leaf_task_id: str, output_indices: List[int]
) -> None:
    """Assert the leaf owes nothing downstream and its outputs are newly produced."""
    _assert_pending_children(tracker, leaf_task_id, {})
    _assert_reuse_status(
        tracker, leaf_task_id, output_indices, ObjectReuseStatus.OBJECT_NEW
    )


def _reconstruct_edge(
    tracker: LineageTracker,
    parent_task_id: str,
    child_task_id: str,
    output_indices: List[int],
) -> None:
    """Re-execute one ``parent -> child`` edge during reconstruction.

    The child is resubmitted against the parent's fresh outputs, and only then
    does the parent complete its re-execution.
    """
    _assert_child_pending_on_parent(
        tracker, parent_task_id, child_task_id, output_indices
    )

    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(parent_task_id, output_indices)
    )
    tracker.register_task_complete(parent_task_id)

    _assert_child_pending_on_parent(
        tracker, parent_task_id, child_task_id, output_indices
    )


def test_seed_task_fail_resubmit_and_complete():
    """Fail a dependency-less seed task, resubmit it, then complete it.

    Verifies that:
      - Failing the seed task returns the seed task itself for retry.
      - After resubmission and completion, it has no pending children.
      - Its output object reuse status is OBJECT_NEW (no child depends on it).
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"

    # Register task submission for the seed task with no dependency.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Fail the task; the failed seed task should be returned for retry.
    assert tracker.register_failed_task(seed_task_id) == seed_task_id

    # Register the seed task for submission again to emulate resubmission.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Complete the task.
    tracker.register_task_complete(seed_task_id)

    # A seed task with no children has no pending children, and with no child
    # depending on its outputs they are newly produced.
    _assert_leaf_produces_new_objects(tracker, seed_task_id, [0])


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
        tracker.get_pending_children("unknown_task")
    with pytest.raises(ValueError):
        tracker.get_object_reuse_status("unknown_task", output_index=0)


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


def test_unconsumed_parent_output_is_new():
    """Outputs no child consumes are newly produced, even on a non-leaf task.

    The child consumes only output 0, so output 0 is reused while output 1 -- which
    nothing downstream depends on -- is free to be handed to anyone.
    """
    tracker = LineageTracker()
    seed_task_id = "seed_task"
    child_task_id = "child_task"

    tracker.register_task_submission(seed_task_id, dependencies=[])
    tracker.register_task_submission(
        child_task_id, dependencies=_dependencies(seed_task_id, [0])
    )
    tracker.register_task_complete(seed_task_id)

    _assert_reuse_status(tracker, seed_task_id, [0], ObjectReuseStatus.OBJECT_REUSED)
    _assert_reuse_status(tracker, seed_task_id, [1], ObjectReuseStatus.OBJECT_NEW)


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_child_task_fail_recovers_seed_and_reuse_status(output_indices: List[int]):
    """Fail a child that depends on the seed's outputs, then recover.

    Verifies that:
      - Failing the child returns the seed task for retry (the chain root).
      - After the seed is resubmitted, the still-pending child shows up as a
        pending child of the seed, reporting every depended-on output index.
      - Each of the seed's depended-on outputs is OBJECT_REUSED.
      - Completing the seed's re-execution leaves the lineage edge intact, so the
        child stays pending on it and the outputs stay reused.
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
    assert tracker.register_failed_task(child_task_id) == seed_task_id

    # Resubmit the seed to emulate recovery. The child is still pending
    # reconstruction, so it is a pending child of the seed depending on every
    # output index it consumes, and each of those outputs is reused.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _assert_child_pending_on_parent(
        tracker, seed_task_id, child_task_id, output_indices
    )

    # Re-executing the seed and resubmitting the child does not retire the edge.
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices)

    # The child is the leaf of this chain, so nothing depends on its outputs.
    tracker.register_task_complete(child_task_id)
    _assert_leaf_produces_new_objects(tracker, child_task_id, [0])


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
    assert tracker.register_failed_task(child_task_id) == seed_task_id

    # Resubmit the seed to emulate recovery. The child is still pending
    # reconstruction, so it is a pending child of the seed depending on every
    # output index it consumes, and each of those outputs is reused.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _assert_child_pending_on_parent(
        tracker, seed_task_id, child_task_id, output_indices
    )

    # Re-executing the seed and resubmitting the child does not retire the edge.
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices)

    # The child is the leaf of this chain, so nothing depends on its outputs.
    tracker.register_task_complete(child_task_id)
    _assert_leaf_produces_new_objects(tracker, child_task_id, [0])


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_child_task_fail_twice_recovers_seed_and_reuse_status(
    output_indices: List[int],
):
    """Fail the child, recover the seed, then fail the child's retry as well.

    Verifies that a second failure of the same child is handled identically to
    the first:
      - The retry of the child traces back to the same seed task.
      - After the seed is resubmitted a second time, the child is still a pending
        child of the seed depending on the same output indices.
      - The seed's depended-on outputs are still OBJECT_REUSED, so the lineage
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
    assert tracker.register_failed_task(child_task_id) == seed_task_id

    # Resubmit and complete the seed to emulate the first recovery, then resubmit
    # the child as part of reconstruction.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices)

    # Fail the child again. The second failure traces back to the same seed task.
    assert tracker.register_failed_task(child_task_id) == seed_task_id

    # Resubmit the seed again to emulate the second recovery. The child is still
    # pending reconstruction, so the lineage edge survives the second round trip.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    _assert_child_pending_on_parent(
        tracker, seed_task_id, child_task_id, output_indices
    )

    # The child's second retry finally completes.
    _reconstruct_edge(tracker, seed_task_id, child_task_id, output_indices)
    tracker.register_task_complete(child_task_id)


@pytest.mark.parametrize("output_indices", OUTPUT_INDICES)
def test_deep_chain_leaf_fail_recovers_seed_and_reuse_status(output_indices: List[int]):
    """Fail the leaf of a 5-deep linear chain, then recover the whole chain.

    Chain: ``task_0 -> task_1 -> task_2 -> task_3 -> task_4``, each task
    consuming ``output_indices`` of its parent's outputs. Verifies that:
      - Failing the leaf traces all the way back to the seed as the retry root.
      - Every task below the seed is pending, so each parent reports exactly its
        one pending child and each depended-on output is OBJECT_REUSED.
      - Reconstruction walks down the chain one edge at a time, and the leaf --
        which nothing consumes -- produces new objects.
    """
    tracker = LineageTracker()
    task_ids = [f"task_{i}" for i in range(5)]
    seed_task_id, leaf_task_id = task_ids[0], task_ids[-1]

    _register_linear_chain(tracker, task_ids, output_indices=output_indices)

    # Fail the leaf; reconstruction traces up the whole chain to the seed.
    assert tracker.register_failed_task(leaf_task_id) == seed_task_id

    # Resubmit the seed to emulate recovery of the chain root.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Walk reconstruction down the chain one edge at a time.
    for parent_task_id, child_task_id in zip(task_ids, task_ids[1:]):
        _reconstruct_edge(tracker, parent_task_id, child_task_id, output_indices)

    # The leaf has no children of its own.
    _assert_leaf_produces_new_objects(tracker, leaf_task_id, [0])

    # The leaf's own re-execution closes out the reconstruction.
    tracker.register_task_complete(leaf_task_id)


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
      - The second failure traces back to the same seed regardless of how deep in
        the chain it happens, and the tasks above it are pending again with their
        parents' outputs still OBJECT_REUSED.
      - After the second recovery the whole chain reconstructs as before.
    """
    tracker = LineageTracker()
    task_ids = [f"task_{i}" for i in range(5)]
    seed_task_id, leaf_task_id = task_ids[0], task_ids[-1]

    _register_linear_chain(tracker, task_ids, output_indices=output_indices)

    # Fail the leaf; reconstruction traces up the whole chain to the seed.
    assert tracker.register_failed_task(leaf_task_id) == seed_task_id

    # Resubmit the seed to emulate the first recovery.
    tracker.register_task_submission(seed_task_id, dependencies=[])

    # Walk reconstruction down to the second failure point: every task above it
    # re-executes successfully, and the last edge resubmits the task that is about
    # to fail against its parent's fresh outputs.
    for parent_task_id, child_task_id in zip(
        task_ids[:retry_failure_index], task_ids[1 : retry_failure_index + 1]
    ):
        _reconstruct_edge(tracker, parent_task_id, child_task_id, output_indices)

    # The retry fails; reconstruction traces back to the same seed.
    assert tracker.register_failed_task(task_ids[retry_failure_index]) == seed_task_id

    # Resubmit the seed again to emulate the second recovery, then walk the full
    # reconstruction down the chain to completion.
    tracker.register_task_submission(seed_task_id, dependencies=[])
    for parent_task_id, child_task_id in zip(task_ids, task_ids[1:]):
        _reconstruct_edge(tracker, parent_task_id, child_task_id, output_indices)

    # The leaf has no children of its own.
    _assert_leaf_produces_new_objects(tracker, leaf_task_id, [0])

    # The leaf's own re-execution closes out the reconstruction.
    tracker.register_task_complete(leaf_task_id)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
