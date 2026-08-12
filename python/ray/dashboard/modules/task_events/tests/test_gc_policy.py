import sys

import pytest

from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import TaskStatus, TaskType
from ray.dashboard.modules.task_events.gc_policy import FinishedTaskActorTaskGcPolicy


def _finished_event() -> gcs_pb2.TaskEvents:
    event = gcs_pb2.TaskEvents()
    event.state_updates.state_ts_ns[TaskStatus.FINISHED] = 1
    return event


def _typed_event(task_type) -> gcs_pb2.TaskEvents:
    event = gcs_pb2.TaskEvents()
    event.task_info.type = task_type
    return event


def test_max_priority():
    assert FinishedTaskActorTaskGcPolicy().max_priority == 3


def test_priority_tiers():
    policy = FinishedTaskActorTaskGcPolicy()
    assert policy.get_task_list_priority(_finished_event()) == 0
    assert policy.get_task_list_priority(_typed_event(TaskType.ACTOR_TASK)) == 1
    assert (
        policy.get_task_list_priority(_typed_event(TaskType.ACTOR_CREATION_TASK)) == 1
    )
    assert policy.get_task_list_priority(_typed_event(TaskType.NORMAL_TASK)) == 2
    # Empty event: neither finished nor an actor task -> lowest priority.
    assert policy.get_task_list_priority(gcs_pb2.TaskEvents()) == 2


def test_finished_wins_over_actor():
    policy = FinishedTaskActorTaskGcPolicy()
    event = _typed_event(TaskType.ACTOR_TASK)
    event.state_updates.state_ts_ns[TaskStatus.FINISHED] = 1
    assert policy.get_task_list_priority(event) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
