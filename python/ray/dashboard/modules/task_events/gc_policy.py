"""Garbage-collection priority policy for the task-event store.

Each task event is assigned a priority tier. When the store is over capacity, events in
lower-priority tiers are evicted first: finished tasks first, then actor tasks, then
everything else.
"""
from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import TaskStatus, TaskType


def is_task_finished(task_event: gcs_pb2.TaskEvents) -> bool:
    """Whether the task attempt has reported a FINISHED state."""
    if not task_event.HasField("state_updates"):
        return False
    return TaskStatus.FINISHED in task_event.state_updates.state_ts_ns


def is_actor_task(task_event: gcs_pb2.TaskEvents) -> bool:
    """Whether the task attempt is an actor task or an actor creation task."""
    if not task_event.HasField("task_info"):
        return False
    return task_event.task_info.type in (
        TaskType.ACTOR_TASK,
        TaskType.ACTOR_CREATION_TASK,
    )


class FinishedTaskActorTaskGcPolicy:
    """Buckets task events into priority tiers; a higher tier is evicted later."""

    # Number of priority tiers, i.e. valid tiers are 0 .. MAX_PRIORITY - 1.
    MAX_PRIORITY = 3

    def get_task_list_priority(self, task_event: gcs_pb2.TaskEvents) -> int:
        if is_task_finished(task_event):
            return 0
        if is_actor_task(task_event):
            return 1
        return 2
