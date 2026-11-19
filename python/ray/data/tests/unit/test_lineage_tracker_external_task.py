"""PROTOTYPE. Tracker semantics for an external task, such as the push-based
split's delivery of one output block to a train worker.

The executor registers each push as a task that consumes one output block of the
final operator. When the push's input is lost, the push is the target of the plan,
so the producer re-runs as an ordinary parent and only the block that push owed is
re-produced.
"""

import sys
from typing import List

import pytest

from ray.data._internal.execution.lineage_tracker import (
    LineageTaskId,
    LineageTracker,
    ObjectReuseStatus,
    ParentBlockOutput,
)

REUSED = ObjectReuseStatus.OBJECT_REUSED
PRUNED = ObjectReuseStatus.OBJECT_PRUNED


def _emit_and_push(
    tracker: LineageTracker, producer: LineageTaskId, num_outputs: int
) -> List[LineageTaskId]:
    """Queue each of ``producer``'s outputs and push it as an external task, the
    way the executor and the push coordinator do. Returns the push task IDs."""
    push_ids = []
    for output_index in range(num_outputs):
        block_id = f"{producer}/block{output_index}"
        tracker.register_block_output(producer, block_id, output_index)
        tracker.register_block_queued(producer, output_index)
        push_id = f"push:{producer}:{output_index}"
        tracker.register_task_submission(
            push_id, tracker.resolve_dependencies([block_id])
        )
        push_ids.append(push_id)
    tracker.register_task_complete(producer, num_outputs=num_outputs)
    return push_ids


def _statuses(tracker, task_id, num_outputs, plan_id):
    return [
        tracker.get_object_reuse_status(task_id, output_index, plan_id)
        for output_index in range(num_outputs)
    ]


def test_lost_push_reproduces_only_its_block():
    tracker = LineageTracker()
    tracker.register_task_submission("seed", [])
    push_ids = _emit_and_push(tracker, "seed", num_outputs=3)
    tracker.register_task_complete(push_ids[0], num_outputs=0)

    seeds, plan_id = tracker.register_task_failed(push_ids[1])

    assert seeds == ["seed"]
    assert plan_id == push_ids[1]
    # The delivered block 0 and the in-flight block 2 are not re-emitted.
    assert _statuses(tracker, "seed", 3, plan_id) == [PRUNED, REUSED, PRUNED]
    assert tracker.get_pending_children("seed", plan_id) == {
        push_ids[1]: [ParentBlockOutput(parent_lineage_task_id="seed", output_index=1)]
    }

    # The re-produced block is released for the push, which re-registers under
    # the same ID and plan.
    tracker.register_block_queued("seed", 1, plan_id)
    tracker.register_task_submission(
        push_ids[1],
        [ParentBlockOutput(parent_lineage_task_id="seed", output_index=1)],
        plan_id,
    )
    assert tracker.get_pending_children("seed", plan_id) == {}


def test_lost_push_through_a_map_reruns_only_the_owed_branch():
    tracker = LineageTracker()
    tracker.register_task_submission("seed", [])
    for output_index in range(2):
        tracker.register_block_output("seed", f"seed/block{output_index}", output_index)
        tracker.register_block_queued("seed", output_index)
    tracker.register_task_complete("seed", num_outputs=2)
    # One map task consumes both seed blocks and emits two blocks.
    tracker.register_task_submission(
        "map", tracker.resolve_dependencies(["seed/block0", "seed/block1"])
    )
    push_ids = _emit_and_push(tracker, "map", num_outputs=2)

    seeds, plan_id = tracker.register_task_failed(push_ids[0])

    assert seeds == ["seed"]
    assert _statuses(tracker, "seed", 2, plan_id) == [REUSED, REUSED]
    assert _statuses(tracker, "map", 2, plan_id) == [REUSED, PRUNED]


def test_two_lost_pushes_of_one_producer_get_separate_plans():
    tracker = LineageTracker()
    tracker.register_task_submission("seed", [])
    push_ids = _emit_and_push(tracker, "seed", num_outputs=3)

    seeds_a, plan_a = tracker.register_task_failed(push_ids[0])
    seeds_b, plan_b = tracker.register_task_failed(push_ids[2])

    # Each plan re-runs the seed and owes only its own block, so neither drops
    # nor duplicates the other's. Batching them onto one seed re-run is the
    # seed-batching follow-up.
    assert seeds_a == seeds_b == ["seed"]
    assert plan_a != plan_b
    assert _statuses(tracker, "seed", 3, plan_a) == [REUSED, PRUNED, PRUNED]
    assert _statuses(tracker, "seed", 3, plan_b) == [PRUNED, PRUNED, REUSED]


def test_lost_re_push_retries_the_same_plan():
    tracker = LineageTracker()
    tracker.register_task_submission("seed", [])
    push_ids = _emit_and_push(tracker, "seed", num_outputs=2)
    _, plan_id = tracker.register_task_failed(push_ids[1])
    tracker.register_block_queued("seed", 1, plan_id)
    tracker.register_task_submission(
        push_ids[1],
        [ParentBlockOutput(parent_lineage_task_id="seed", output_index=1)],
        plan_id,
    )

    # The re-pushed block is lost too.
    seeds, retry_plan_id = tracker.register_task_failed(push_ids[1], plan_id)

    assert seeds == ["seed"]
    assert retry_plan_id == plan_id
    assert _statuses(tracker, "seed", 2, plan_id) == [PRUNED, REUSED]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
