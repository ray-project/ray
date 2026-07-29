import sys

import pytest

from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import TaskStatus, TaskType
from ray.dashboard.modules.task_events.gc_policy import (
    FinishedTaskActorTaskGcPolicy,
    is_actor_task,
    is_task_finished,
)


def _finished_event() -> gcs_pb2.TaskEvents:
    event = gcs_pb2.TaskEvents()
    event.state_updates.state_ts_ns[TaskStatus.FINISHED] = 1
    return event


def _typed_event(task_type) -> gcs_pb2.TaskEvents:
    event = gcs_pb2.TaskEvents()
    event.task_info.type = task_type
    return event


def test_is_task_finished():
    assert is_task_finished(_finished_event())
    assert not is_task_finished(gcs_pb2.TaskEvents())


def test_is_actor_task():
    assert is_actor_task(_typed_event(TaskType.ACTOR_TASK))
    assert is_actor_task(_typed_event(TaskType.ACTOR_CREATION_TASK))
    assert not is_actor_task(_typed_event(TaskType.NORMAL_TASK))
    assert not is_actor_task(gcs_pb2.TaskEvents())


def test_priority_tiers():
    policy = FinishedTaskActorTaskGcPolicy()
    assert policy.MAX_PRIORITY == 3
    assert policy.get_task_list_priority(_finished_event()) == 0
    assert policy.get_task_list_priority(_typed_event(TaskType.ACTOR_TASK)) == 1
    assert policy.get_task_list_priority(_typed_event(TaskType.NORMAL_TASK)) == 2


def test_finished_wins_over_actor():
    policy = FinishedTaskActorTaskGcPolicy()
    event = _typed_event(TaskType.ACTOR_TASK)
    event.state_updates.state_ts_ns[TaskStatus.FINISHED] = 1
    assert policy.get_task_list_priority(event) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
