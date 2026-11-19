"""Garbage collection of lineage that is durable in a sink.

A completed write task's rows can never be lost again, so the operator calls
``try_garbage_collect_lineage``: the tracker drops the task's node and releases the
outputs it consumed from its parents. A parent that has completed and had every output
released is dropped in turn. Once every write task has completed, the graph must be
empty.
"""

import sys
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence

import pytest

from ray.data._internal.execution.lineage_tracker import (
    BlockId,
    LineageTaskId,
    LineageTracker,
    ObjectReuseStatus,
)
from ray.data.tests.unit.lineage_tracker_util import (
    Action,
    ActionRunner,
    Complete,
    ExpectObjectReuseStatus,
    ExpectPendingChildren,
    ExpectResolvedDependencies,
    ExpectSeedInput,
    Fail,
    GarbageCollect,
    OutputBlock,
    PlanRef,
    QueueBlock,
    Raises,
    RegisterSeedInput,
    Submit,
    SubmitBlocks,
    run_actions,
)


@dataclass(frozen=True)
class ExpectTaskIds(Action):
    """The tasks whose nodes are still in the graph."""

    expected: Sequence[LineageTaskId]

    def apply(self, runner: ActionRunner) -> None:
        assert sorted(runner.tracker._lineage_task_id_to_task_node) == sorted(
            self.expected
        )


@dataclass(frozen=True)
class ExpectBlockIds(Action):
    """The blocks the tracker still attributes to a producer."""

    expected: Sequence[BlockId]

    def apply(self, runner: ActionRunner) -> None:
        assert sorted(runner.tracker._block_id_to_parent_output) == sorted(
            self.expected
        )


@dataclass(frozen=True)
class ExpectNodeAttribute(Action):
    """One attribute of a task's node."""

    lineage_task_id: LineageTaskId
    attribute: str
    expected: Any

    def apply(self, runner: ActionRunner) -> None:
        node = runner.tracker._lineage_task_id_to_task_node[self.lineage_task_id]
        assert getattr(node, self.attribute) == self.expected


def _complete_write(
    lineage_task_id: LineageTaskId,
    num_outputs: int = 1,
    plan: Optional[PlanRef] = None,
) -> List[Action]:
    """Complete a write task the way the operator does: report the completion,
    then mark its outputs durable."""
    return [
        Complete(lineage_task_id, num_outputs=num_outputs, plan=plan),
        GarbageCollect(lineage_task_id),
    ]


def _assert_all_state_is_owned(tracker: LineageTracker) -> None:
    """Every piece of state the tracker keeps belongs to a registered node.

    That is what lets removing a node remove all of its state: nothing outside a
    node may refer to a task that is no longer registered.
    """
    nodes = tracker._lineage_task_id_to_task_node
    owned_block_ids = {}
    for task_id, node in nodes.items():
        assert node.lineage_task_id == task_id
        for block_id in node.output_block_ids:
            owned_block_ids[block_id] = task_id
        for parent in node.parent_tasks:
            assert nodes.get(parent.lineage_task_id) is parent
        for child in node.child_tasks:
            assert nodes.get(child.lineage_task_id) is child
        assert set(node.child_task_block_dependencies) == {
            child.lineage_task_id for child in node.child_tasks
        }
        # A plan is keyed by its target task, which outlives the plan.
        for plan_id in node.plan_to_child_block_lineages:
            assert plan_id in nodes
    assert {
        block_id: parent_output.parent_lineage_task_id
        for block_id, parent_output in tracker._block_id_to_parent_output.items()
    } == owned_block_ids


@pytest.fixture
def tracker():
    tracker = LineageTracker()
    yield tracker
    _assert_all_state_is_owned(tracker)


_PLAN = PlanRef("plan")


def test_completed_chain_is_collected_by_its_write(tracker):
    """read -> map -> write: the write's completion collects the whole chain."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=1),
            Submit("map:0", {"read:0": [0]}),
            Complete("map:0", num_outputs=1),
            Submit("write:0", {"map:0": [0]}),
            ExpectTaskIds(["map:0", "read:0", "write:0"]),
            *_complete_write("write:0"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_parent_still_streaming_is_collected_at_its_own_completion(tracker):
    """A write can finish before the task that fed it does.

    Outputs stream, so the write may consume the parent's first output and complete
    while the parent is still producing. The parent is collected once it completes,
    not before.
    """
    run_actions(
        [
            Submit("read:0"),
            Submit("write:0", {"read:0": [0]}),
            *_complete_write("write:0"),
            ExpectTaskIds(["read:0"]),
            Complete("read:0", num_outputs=1),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_parent_waits_for_every_output_to_be_written(tracker):
    """Fan-out: the parent stays until each of its outputs has been written."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=3),
            Submit("write:0", {"read:0": [0]}),
            Submit("write:1", {"read:0": [1, 2]}),
            *_complete_write("write:0"),
            ExpectTaskIds(["read:0", "write:1"]),
            ExpectNodeAttribute("read:0", "durable_output_indices", {0}),
            *_complete_write("write:1"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_unconsumed_output_keeps_the_parent(tracker):
    """An output still in flight (no consumer registered yet) pins its producer."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=2),
            Submit("write:0", {"read:0": [0]}),
            *_complete_write("write:0"),
            ExpectTaskIds(["read:0"]),
            Submit("write:1", {"read:0": [1]}),
            *_complete_write("write:1"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_written_output_stays_pruned_when_its_producer_is_reconstructed(tracker):
    """Outputs consumed by a collected write must not be re-emitted.

    The write is gone from ``child_tasks``, so the target must still treat the
    output it consumed as delivered, or the written rows would be written twice.
    """
    run_actions(
        [
            Submit("read:0"),
            QueueBlock("read:0", 0),
            Complete("read:0", num_outputs=1),
            Submit("map:0", {"read:0": [0]}),
            # map:0 streams output 0 to a write that completes, then fails.
            QueueBlock("map:0", 0),
            Submit("write:0", {"map:0": [0]}),
            *_complete_write("write:0"),
            ExpectTaskIds(["map:0", "read:0"]),
            Fail("map:0", expected_seed_task_ids=["read:0"], plan=_PLAN),
            # The seed re-runs and releases map:0's input, then map:0 re-runs as
            # the plan's target.
            Submit("read:0", plan=_PLAN),
            ExpectPendingChildren("read:0", _PLAN, {"map:0": {"read:0": [0]}}),
            ExpectObjectReuseStatus(
                "read:0", 0, _PLAN, ObjectReuseStatus.OBJECT_REUSED
            ),
            Complete("read:0", num_outputs=1, plan=_PLAN),
            Submit("map:0", {"read:0": [0]}, plan=_PLAN),
            ExpectObjectReuseStatus("map:0", 0, _PLAN, ObjectReuseStatus.OBJECT_PRUNED),
            ExpectObjectReuseStatus("map:0", 1, _PLAN, ObjectReuseStatus.OBJECT_NEW),
            # Only the new output is queued, and once it is written nothing is left.
            QueueBlock("map:0", 1, _PLAN),
            Complete("map:0", num_outputs=2, plan=_PLAN),
            ExpectTaskIds(["map:0", "read:0"]),
            Submit("write:1", {"map:0": [1]}),
            *_complete_write("write:1"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


#: read:0 -> map:0, which fans out its output ``i`` to write:i. Every output has
#: been queued downstream, so a reconstruction must re-emit only what was lost.
_BUILD_FAN_OUT_TO_TWO_WRITES = [
    Submit("read:0"),
    QueueBlock("read:0", 0),
    Complete("read:0", num_outputs=1),
    Submit("map:0", {"read:0": [0]}),
    QueueBlock("map:0", 0),
    QueueBlock("map:0", 1),
    Complete("map:0", num_outputs=2),
    Submit("write:0", {"map:0": [0]}),
    Submit("write:1", {"map:0": [1]}),
]

#: The plan opened by the failure of write:i, indexed by ``i``.
_WRITE_PLANS = [PlanRef("write_0_plan"), PlanRef("write_1_plan")]


def _reconstruct_write(output_index: int) -> List[Action]:
    """Re-run read:0 -> map:0 -> write:``output_index`` under that write's plan,
    then complete the write."""
    write_task_id, plan = f"write:{output_index}", _WRITE_PLANS[output_index]
    return [
        Submit("read:0", plan=plan),
        ExpectPendingChildren("read:0", plan, {"map:0": {"read:0": [0]}}),
        Complete("read:0", num_outputs=1, plan=plan),
        Submit("map:0", {"read:0": [0]}, plan=plan),
        # Only the output the failed write consumed is re-emitted.
        ExpectObjectReuseStatus(
            "map:0", output_index, plan, ObjectReuseStatus.OBJECT_REUSED
        ),
        ExpectObjectReuseStatus(
            "map:0", 1 - output_index, plan, ObjectReuseStatus.OBJECT_PRUNED
        ),
        Complete("map:0", num_outputs=2, plan=plan),
        Submit(write_task_id, {"map:0": [output_index]}, plan=plan),
        *_complete_write(write_task_id, plan=plan),
    ]


@pytest.mark.parametrize(
    "first_output_index",
    [
        pytest.param(0, id="write_0_reconstructs_first"),
        pytest.param(1, id="write_1_reconstructs_first"),
    ],
)
def test_fan_out_writes_that_both_fail_are_collected_as_each_reconstructs(
    tracker, first_output_index: int
):
    """Each reconstructed write collects only itself, until the last one collects
    the lineage it shared with the other.
    """
    second_output_index = 1 - first_output_index
    run_actions(
        [
            *_BUILD_FAN_OUT_TO_TWO_WRITES,
            Fail("write:0", expected_seed_task_ids=["read:0"], plan=_WRITE_PLANS[0]),
            Fail("write:1", expected_seed_task_ids=["read:0"], plan=_WRITE_PLANS[1]),
            # Neither write is durable, so nothing is collected.
            ExpectTaskIds(["map:0", "read:0", "write:0", "write:1"]),
            *_reconstruct_write(first_output_index),
            # The parents still owe the other write its input.
            ExpectTaskIds(["map:0", "read:0", f"write:{second_output_index}"]),
            *_reconstruct_write(second_output_index),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_fan_in_write_collects_every_parent(tracker):
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=1),
            Submit("read:1"),
            Complete("read:1", num_outputs=1),
            Submit("write:0", {"read:0": [0], "read:1": [0]}),
            *_complete_write("write:0"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


#: read:0 and read:1 fan in to write:0. read:1 streams its output to the write
#: but is still running when the write completes, so only read:0 goes with it.
_WRITE_FANS_IN_FROM_A_STREAMING_PARENT = [
    Submit("read:0"),
    QueueBlock("read:0", 0),
    Complete("read:0", num_outputs=1),
    Submit("read:1"),
    QueueBlock("read:1", 0),
    Submit("write:0", {"read:0": [0], "read:1": [0]}),
    *_complete_write("write:0"),
    ExpectTaskIds(["read:1"]),
]


@pytest.mark.parametrize(
    "actions",
    [
        pytest.param(
            [
                *_WRITE_FANS_IN_FROM_A_STREAMING_PARENT,
                Complete("read:1", num_outputs=1),
                ExpectTaskIds([]),
            ],
            id="streaming_parent_completes",
        ),
        pytest.param(
            [
                *_WRITE_FANS_IN_FROM_A_STREAMING_PARENT,
                Fail("read:1", expected_seed_task_ids=["read:1"], plan=_PLAN),
                Submit("read:1", plan=_PLAN),
                # Its output is already written, so the re-execution must not
                # emit it again.
                ExpectPendingChildren("read:1", _PLAN, {}),
                ExpectObjectReuseStatus(
                    "read:1", 0, _PLAN, ObjectReuseStatus.OBJECT_PRUNED
                ),
                # The reconstruction still runs, so the node stays.
                ExpectTaskIds(["read:1"]),
                Complete("read:1", num_outputs=1, plan=_PLAN),
                ExpectTaskIds([]),
            ],
            id="streaming_parent_fails_and_reconstructs",
        ),
    ],
)
def test_fan_in_write_collects_each_parent_once_it_completes(
    tracker, actions: List[Action]
):
    """A write that fans in collects only the parents that have completed. The
    rest are collected once they complete, whether by their first attempt or by
    a reconstruction.
    """
    run_actions(actions, tracker)


def test_diamond_is_collected_once(tracker):
    """Two paths to one ancestor must not trip over the already-collected node."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=2),
            Submit("map:0", {"read:0": [0]}),
            Complete("map:0", num_outputs=1),
            Submit("map:1", {"read:0": [1]}),
            Complete("map:1", num_outputs=1),
            Submit("write:0", {"map:0": [0], "map:1": [0]}),
            *_complete_write("write:0"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_task_with_no_outputs_is_collected(tracker):
    """A task that emitted nothing (say, everything filtered out) owes nobody."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=0),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_non_sink_leaf_is_never_collected(tracker):
    """Without a write, outputs go to the driver and are never durable."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=1),
            Submit("map:0", {"read:0": [0]}),
            Complete("map:0", num_outputs=1),
            ExpectTaskIds(["map:0", "read:0"]),
        ],
        tracker,
    )


def test_incomplete_sink_is_collected_at_its_completion(tracker):
    """Outputs marked durable before completion are collected once it completes."""
    run_actions(
        [
            Submit("read:0"),
            Complete("read:0", num_outputs=1),
            Submit("write:0", {"read:0": [0]}),
            GarbageCollect("write:0", expected_collected=False),
            ExpectTaskIds(["read:0", "write:0"]),
            Complete("write:0", num_outputs=1),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_collecting_an_unregistered_task_is_rejected(tracker):
    run_actions([Raises(GarbageCollect("write:0"))], tracker)


def test_collecting_an_already_collected_task_is_rejected(tracker):
    run_actions(
        [
            Submit("write:0"),
            *_complete_write("write:0"),
            ExpectTaskIds([]),
            Raises(GarbageCollect("write:0")),
        ],
        tracker,
    )


#: read:0 streams output 0 to write:0, then fails before completing. The read is
#: the target of its own plan and the only seed to re-run.
_FAIL_READ_AFTER_WRITE_IS_SUBMITTED = [
    Submit("read:0"),
    QueueBlock("read:0", 0),
    Submit("write:0", {"read:0": [0]}),
    Fail("read:0", expected_seed_task_ids=["read:0"], plan=_PLAN),
]

#: Re-run the failed read as the target of the plan. Its output 0 is already
#: downstream with the write, so the re-execution must not queue it again.
_RESUBMIT_READ = [
    Submit("read:0", plan=_PLAN),
    ExpectPendingChildren("read:0", _PLAN, {}),
    ExpectObjectReuseStatus("read:0", 0, _PLAN, ObjectReuseStatus.OBJECT_PRUNED),
]


def test_running_attempt_keeps_a_node(tracker):
    """A reconstruction attempt in flight still reports outputs against the node."""
    run_actions(
        [
            *_FAIL_READ_AFTER_WRITE_IS_SUBMITTED,
            *_RESUBMIT_READ,
            ExpectNodeAttribute("read:0", "num_running_attempts", 1),
            # Resubmitting the target should not queue any children.
            ExpectNodeAttribute("read:0", "plan_to_child_block_lineages", {}),
            *_complete_write("write:0"),
            ExpectTaskIds(["read:0"]),
            Complete("read:0", num_outputs=1, plan=_PLAN),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_reconstructed_lineage_is_collected_once_written(tracker):
    """After a recovery completes and its target is written, nothing is left."""
    run_actions(
        [
            Submit("read:0"),
            QueueBlock("read:0", 0),
            Complete("read:0", num_outputs=1),
            Submit("map:0", {"read:0": [0]}),
            Fail("map:0", expected_seed_task_ids=["read:0"], plan=_PLAN),
            # The seed re-runs and releases map:0's input, then map:0 re-runs, all
            # under the plan.
            Submit("read:0", plan=_PLAN),
            ExpectPendingChildren("read:0", _PLAN, {"map:0": {"read:0": [0]}}),
            Submit("map:0", {"read:0": [0]}, plan=_PLAN),
            Complete("read:0", num_outputs=1, plan=_PLAN),
            QueueBlock("map:0", 0, _PLAN),
            Complete("map:0", num_outputs=1, plan=_PLAN),
            ExpectTaskIds(["map:0", "read:0"]),
            Submit("write:0", {"map:0": [0]}),
            *_complete_write("write:0"),
            ExpectTaskIds([]),
        ],
        tracker,
    )


def test_collected_seed_input_is_released(tracker):
    """A collected seed can never be resubmitted, so its input goes with it."""
    seed_input = object()
    run_actions(
        [
            Submit("read:0"),
            RegisterSeedInput("read:0", seed_input),
            ExpectSeedInput("read:0", seed_input),
        ],
        tracker,
    )
    read_node = tracker._lineage_task_id_to_task_node["read:0"]

    run_actions(
        [
            *_complete_write("read:0"),
            Raises(ExpectSeedInput("read:0", seed_input)),
        ],
        tracker,
    )
    # Released even though the node object is still referenced here.
    assert read_node.seed_input is None


_FIRST_PLAN = PlanRef("first_plan")
_SECOND_PLAN = PlanRef("second_plan")


def test_unused_registered_blocks_are_freed_on_garbage_collection(tracker):
    """A block that is recorded but dropped instead of consumed leaves no entry.

    Two plans that trace back to one seed each re-run it, so each reproduces its
    own copy of the same output. If the child's reconstruction task is already
    queued when the second copy is released, that copy is dropped and never
    resolved. Its entry must go once the producer is collected.

    read:0 -> map:0 fans out to write:0 and write:1, which fail one after the
    other. Each failure traces back through map:0 to read:0.
    """
    plans = [_FIRST_PLAN, _SECOND_PLAN]
    actions: List[Action] = [
        Submit("read:0"),
        OutputBlock("read:0", "read_block", 0),
        QueueBlock("read:0", 0),
        Complete("read:0", num_outputs=1),
        SubmitBlocks("map:0", ["read_block"]),
    ]
    for output_index in range(2):
        actions += [
            OutputBlock("map:0", f"map_block_{output_index}", output_index),
            QueueBlock("map:0", output_index),
        ]
    actions.append(Complete("map:0", num_outputs=2))
    for output_index in range(2):
        actions.append(
            SubmitBlocks(f"write:{output_index}", [f"map_block_{output_index}"])
        )

    actions += [
        # write:0 fails. The seed re-runs for its plan and releases its copy of
        # map:0's input, which queues map:0's reconstruction task.
        Fail("write:0", expected_seed_task_ids=["read:0"], plan=_FIRST_PLAN),
        Submit("read:0", plan=_FIRST_PLAN),
        ExpectObjectReuseStatus(
            "read:0", 0, _FIRST_PLAN, ObjectReuseStatus.OBJECT_REUSED
        ),
        OutputBlock("read:0", "consumed_copy", 0),
        QueueBlock("read:0", 0, _FIRST_PLAN),
        Complete("read:0", num_outputs=1, plan=_FIRST_PLAN),
        # write:1 fails after the seed's first re-run was submitted, so the seed
        # re-runs again for the second plan. map:0's reconstruction task is still
        # queued, so the second plan is batched into it and this copy is dropped.
        Fail("write:1", expected_seed_task_ids=["read:0"], plan=_SECOND_PLAN),
        Submit("read:0", plan=_SECOND_PLAN),
        ExpectObjectReuseStatus(
            "read:0", 0, _SECOND_PLAN, ObjectReuseStatus.OBJECT_REUSED
        ),
        OutputBlock("read:0", "dropped_copy", 0),
        QueueBlock("read:0", 0, _SECOND_PLAN),
        Complete("read:0", num_outputs=1, plan=_SECOND_PLAN),
        # map:0's reconstruction task serves both plans on the first copy alone.
        SubmitBlocks("map:0", ["consumed_copy"], plans=plans),
    ]
    # Each plan withholds only the output its failed write consumes.
    for output_index, plan in enumerate(plans):
        actions += [
            ExpectObjectReuseStatus(
                "map:0", output_index, plan, ObjectReuseStatus.OBJECT_REUSED
            ),
            ExpectObjectReuseStatus(
                "map:0", 1 - output_index, plan, ObjectReuseStatus.OBJECT_PRUNED
            ),
            OutputBlock("map:0", f"map_copy_{output_index}", output_index),
            QueueBlock("map:0", output_index, plan),
        ]
    for plan in plans:
        actions.append(Complete("map:0", num_outputs=2, plan=plan))

    # Each write re-runs as its plan's target.
    for output_index, plan in enumerate(plans):
        actions.append(
            SubmitBlocks(
                f"write:{output_index}", [f"map_copy_{output_index}"], plans=[plan]
            )
        )
    actions += [
        *_complete_write("write:0", plan=_FIRST_PLAN),
        # The producer is still in the graph, so the entry may yet be resolved.
        ExpectTaskIds(["map:0", "read:0", "write:1"]),
        ExpectBlockIds(["dropped_copy"]),
        *_complete_write("write:1", plan=_SECOND_PLAN),
        ExpectTaskIds([]),
        ExpectBlockIds([]),
    ]
    run_actions(actions, tracker)


def test_sink_outputs_are_removed_with_their_task(tracker):
    """A sink's outputs go to the driver, so no task ever resolves them.

    They belong to the sink's node, so collecting the sink removes them.
    """
    run_actions(
        [
            Submit("write:0"),
            OutputBlock("write:0", "stats_block", 0),
            ExpectBlockIds(["stats_block"]),
            *_complete_write("write:0"),
            ExpectTaskIds([]),
            ExpectBlockIds([]),
        ],
        tracker,
    )


def test_resolving_a_block_releases_it_from_its_producer(tracker):
    """Verify we eagerly free the registered blocks
    of a run as soon as they are resolved"""
    run_actions(
        [
            Submit("read:0"),
            OutputBlock("read:0", "block", 0),
            ExpectNodeAttribute("read:0", "output_block_ids", {"block"}),
            ExpectResolvedDependencies(["block"], {"read:0": [0]}),
            ExpectNodeAttribute("read:0", "output_block_ids", set()),
            ExpectBlockIds([]),
        ],
        tracker,
    )


def test_output_of_an_unregistered_task_is_rejected(tracker):
    """An entry must have an owner, or nothing would ever remove it."""
    run_actions(
        [
            Raises(OutputBlock("read:0", "block", 0)),
            ExpectBlockIds([]),
        ],
        tracker,
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
