"""Garbage-collection priority policy for the task-event store.

Each task event is assigned a priority tier. When the store is over capacity, events in
lower-priority tiers are evicted first: finished tasks first, then actor tasks, then
everything else.
"""
from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import TaskStatus, TaskType


def is_task_terminated(task_event: gcs_pb2.TaskEvents) -> bool:
    """Whether the task attempt has reported a FINISHED or FAILED state."""
    if not task_event.HasField("state_updates"):
        return False
    state_ts_ns = task_event.state_updates.state_ts_ns
    return TaskStatus.FINISHED in state_ts_ns or TaskStatus.FAILED in state_ts_ns


class FinishedTaskActorTaskGcPolicy:
    """Buckets task events into priority tiers; a higher tier is evicted later."""

    # Number of priority tiers, i.e. valid tiers are 0 .. max_priority - 1.
    _MAX_PRIORITY = 3

    @property
    def max_priority(self) -> int:
        """Number of priority tiers; valid tiers are 0 .. max_priority - 1."""
        return self._MAX_PRIORITY

    def get_task_list_priority(self, task_event: gcs_pb2.TaskEvents) -> int:
        if self._is_task_finished(task_event):
            return 0
        if self._is_actor_task(task_event):
            return 1
        return 2

    @staticmethod
    def _is_task_finished(task_event: gcs_pb2.TaskEvents) -> bool:
        """Whether the task attempt has reported a FINISHED state."""
        if not task_event.HasField("state_updates"):
            return False
        return TaskStatus.FINISHED in task_event.state_updates.state_ts_ns

    @staticmethod
    def _is_actor_task(task_event: gcs_pb2.TaskEvents) -> bool:
        """Whether the task attempt is an actor task or an actor creation task."""
        if not task_event.HasField("task_info"):
            return False
        return task_event.task_info.type in (
            TaskType.ACTOR_TASK,
            TaskType.ACTOR_CREATION_TASK,
        )
