"""PROTOTYPE. How ``ExternalConsumerTasks`` applies the external tasks that
external_tasks report to the lineage tracker on the executor thread."""

import sys
from dataclasses import dataclass
from typing import List, Optional

import pytest

from ray.data._internal.execution.external_consumer_tasks import (
    ExternalConsumerTasks,
    ExternalTaskLostInput,
)
from ray.data._internal.execution.interfaces.ref_bundle import ReconstructionStamp
from ray.data._internal.execution.lineage_tracker import LineageTracker
from ray.exceptions import ObjectLostError


@dataclass(frozen=True)
class _FakeRef:
    id: str

    def hex(self) -> str:
        return self.id


@dataclass
class _FakeEntry:
    ref: _FakeRef


@dataclass
class _FakeBundle:
    blocks: List[_FakeEntry]
    reconstruction_stamp: Optional[ReconstructionStamp] = None


def _produce(tracker: LineageTracker, producer: str, num_outputs: int):
    """Register ``producer`` and queue its outputs, the way the executor does.
    Returns the refs of the outputs."""
    tracker.register_task_submission(producer, [])
    refs = []
    for output_index in range(num_outputs):
        ref = _FakeRef(f"{producer}/block{output_index}")
        tracker.register_block_output(producer, ref.hex(), output_index)
        tracker.register_block_queued(producer, output_index)
        refs.append(ref)
    tracker.register_task_complete(producer, num_outputs=num_outputs)
    return refs


def _task_ids(tracker: LineageTracker) -> List[str]:
    """The tasks whose lineage the tracker still keeps."""
    return sorted(tracker._lineage_task_id_to_task_node)


def _lost_error() -> ObjectLostError:
    return ObjectLostError("00" * 28, None, "callsite")


def _update_and_pop(external_tasks: ExternalConsumerTasks, tracker):
    """Apply the reports the way the executor does once per step."""
    external_tasks.update_lineage(tracker)
    return external_tasks.pop_lost_inputs()


def _take_and_submit(external_tasks: ExternalConsumerTasks, ref, stamp=None) -> int:
    external_tasks.on_output_taken(_FakeBundle([_FakeEntry(ref)], stamp))
    return external_tasks.submit(ref, stamp)


def test_submit_and_lost_input_in_one_drain_registers_first():
    tracker = LineageTracker()
    [ref] = _produce(tracker, "seed", 1)
    external_tasks = ExternalConsumerTasks()
    error = _lost_error()

    handle = _take_and_submit(external_tasks, ref)
    external_tasks.report_lost_input(handle, error)
    failures = _update_and_pop(external_tasks, tracker)

    assert failures == [ExternalTaskLostInput(f"consumer:{handle}", frozenset(), error)]
    # The external task was registered as the seed's child, so it can be planned.
    seeds, plan_id = tracker.register_task_failed(f"consumer:{handle}")
    assert seeds == ["seed"]
    assert plan_id == f"consumer:{handle}"


def test_lost_input_without_lineage_tracker_is_returned():
    external_tasks = ExternalConsumerTasks()
    error = _lost_error()

    handle = _take_and_submit(external_tasks, _FakeRef("block"))
    external_tasks.report_lost_input(handle, error)

    assert _update_and_pop(external_tasks, None) == [
        ExternalTaskLostInput(f"consumer:{handle}", frozenset(), error)
    ]


def test_reproduced_block_resubmits_under_its_stamp():
    tracker = LineageTracker()
    [ref] = _produce(tracker, "seed", 1)
    external_tasks = ExternalConsumerTasks()
    handle = _take_and_submit(external_tasks, ref)
    external_tasks.report_lost_input(handle, _lost_error())
    [lost] = _update_and_pop(external_tasks, tracker)
    _, plan_id = tracker.register_task_failed(lost.lineage_task_id)
    tracker.register_block_queued("seed", 0, plan_id)

    reproduced = _FakeRef("seed/block0-retry")
    stamp = ReconstructionStamp(lost.lineage_task_id, frozenset({plan_id}))
    retry = _take_and_submit(external_tasks, reproduced, stamp)
    external_tasks.complete(retry)

    assert _update_and_pop(external_tasks, tracker) == []
    assert not external_tasks.has_unresolved()
    # The retry completing collects the lineage the lost input kept.
    assert _task_ids(tracker) == []


def test_unresolved_until_the_drain_applies_the_outcome():
    external_tasks = ExternalConsumerTasks()
    ref = _FakeRef("block")

    external_tasks.on_output_taken(_FakeBundle([_FakeEntry(ref)]))
    assert external_tasks.has_unresolved()
    handle = external_tasks.submit(ref, None)
    external_tasks.complete(handle)
    assert external_tasks.has_unresolved()

    external_tasks.update_lineage(None)
    assert not external_tasks.has_unresolved()


def test_abandoned_task_is_not_reconstructed():
    tracker = LineageTracker()
    [ref] = _produce(tracker, "seed", 1)
    external_tasks = ExternalConsumerTasks()

    handle = _take_and_submit(external_tasks, ref)
    external_tasks.abandon(handle)

    assert _update_and_pop(external_tasks, tracker) == []
    assert not external_tasks.has_unresolved()
    # An abandoned task never completes, so its lineage is kept.
    assert _task_ids(tracker) == ["consumer:0", "seed"]


def test_lost_input_reported_after_update_waits_for_the_next_update():
    tracker = LineageTracker()
    [ref] = _produce(tracker, "seed", 1)
    external_tasks = ExternalConsumerTasks()
    external_tasks.update_lineage(tracker)
    error = _lost_error()

    # Submitted and lost between the two calls of one executor step.
    handle = _take_and_submit(external_tasks, ref)
    external_tasks.report_lost_input(handle, error)
    assert external_tasks.pop_lost_inputs() == []

    # The next step registers the task before returning its lost input.
    assert _update_and_pop(external_tasks, tracker) == [
        ExternalTaskLostInput(f"consumer:{handle}", frozenset(), error)
    ]
    seeds, _ = tracker.register_task_failed(f"consumer:{handle}")
    assert seeds == ["seed"]


def test_completed_external_tasks_garbage_collect_their_lineage():
    """Once every output of a task is consumed by a completed external task,
    its lineage is collected, through to the seed and its retained input."""
    tracker = LineageTracker()
    [seed_ref] = _produce(tracker, "seed", 1)
    tracker.register_seed_input("seed", object())
    tracker.register_task_submission(
        "map", tracker.resolve_dependencies([seed_ref.hex()])
    )
    map_refs = []
    for output_index in range(2):
        ref = _FakeRef(f"map/block{output_index}")
        tracker.register_block_output("map", ref.hex(), output_index)
        tracker.register_block_queued("map", output_index)
        map_refs.append(ref)
    tracker.register_task_complete("map", num_outputs=2)
    external_tasks = ExternalConsumerTasks()
    handles = [_take_and_submit(external_tasks, ref) for ref in map_refs]

    external_tasks.complete(handles[0])
    _update_and_pop(external_tasks, tracker)
    # The map's other output can still be lost, so its lineage is kept.
    assert _task_ids(tracker) == [f"consumer:{handles[1]}", "map", "seed"]

    external_tasks.complete(handles[1])
    _update_and_pop(external_tasks, tracker)
    assert _task_ids(tracker) == []


def test_output_awaiting_submission_keeps_its_lineage():
    tracker = LineageTracker()
    refs = _produce(tracker, "seed", 2)
    external_tasks = ExternalConsumerTasks()
    external_tasks.on_output_taken(_FakeBundle([_FakeEntry(ref) for ref in refs]))

    external_tasks.complete(external_tasks.submit(refs[0], None))
    _update_and_pop(external_tasks, tracker)
    assert _task_ids(tracker) == ["seed"]

    # The task for the other output resolves its producer, which is still
    # registered, so its input can be reconstructed.
    handle = external_tasks.submit(refs[1], None)
    external_tasks.report_lost_input(handle, _lost_error())
    [lost] = _update_and_pop(external_tasks, tracker)
    seeds, _ = tracker.register_task_failed(lost.lineage_task_id)
    assert seeds == ["seed"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))


def test_finished_split_releases_outputs_it_never_submitted():
    """A split that stops early can take an output and never submit a task for
    it. Finishing the split stops that output from holding the executor, without
    releasing the other split's."""
    external_tasks = ExternalConsumerTasks()
    stopped_ref, running_ref = _FakeRef("stopped"), _FakeRef("running")
    external_tasks.on_output_taken(_FakeBundle([_FakeEntry(stopped_ref)]), 0)
    external_tasks.on_output_taken(_FakeBundle([_FakeEntry(running_ref)]), 1)

    external_tasks.finish_split(0)
    assert external_tasks.has_unresolved()

    external_tasks.update_lineage(None)
    external_tasks.complete(external_tasks.submit(running_ref, None))
    external_tasks.update_lineage(None)
    assert not external_tasks.has_unresolved()

    # An output a finished split takes afterwards never holds the executor.
    external_tasks.on_output_taken(_FakeBundle([_FakeEntry(_FakeRef("late"))]), 0)
    assert not external_tasks.has_unresolved()
