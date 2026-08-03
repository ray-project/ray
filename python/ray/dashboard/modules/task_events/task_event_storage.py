"""In-memory store for task events on the dashboard head.

Events from the same task attempt (same task id + attempt number) are merged into a single
``TaskEvents`` entry. The store is bounded by ``MAX_NUM_TASK_EVENTS``; when full, entries
in lower-priority tiers are evicted first (see ``gc_policy``). Per-job bookkeeping tracks
which task attempts have been dropped so that late events for a dropped attempt are not
partially re-stored.
"""
import collections
import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from ray._private import ray_constants
from ray._raylet import JobID, TaskID
from ray.core.generated import gcs_pb2
from ray.core.generated.common_pb2 import ErrorType, RayErrorInfo, TaskStatus, TaskType
from ray.dashboard.modules.task_events.gc_policy import FinishedTaskActorTaskGcPolicy

logger = logging.getLogger(__name__)

# Max number of task events kept before eviction kicks in.
MAX_NUM_TASK_EVENTS = ray_constants.env_integer(
    "RAY_DASHBOARD_TASK_EVENTS_MAX_NUM_TASK_EVENTS", 100_000
)
# Max profile events kept per task attempt; older ones are dropped past this.
MAX_NUM_PROFILE_EVENTS_PER_TASK = ray_constants.env_integer(
    "RAY_DASHBOARD_TASK_EVENTS_MAX_NUM_PROFILE_EVENTS_PER_TASK", 1000
)
# Max dropped task attempts tracked per job before the tracking set is trimmed.
MAX_DROPPED_TASK_ATTEMPTS_PER_JOB = ray_constants.env_integer(
    "RAY_DASHBOARD_TASK_EVENTS_MAX_DROPPED_TASK_ATTEMPTS_PER_JOB", 1_000_000
)
# How often the per-job dropped-attempt tracking is trimmed.
GC_JOB_SUMMARY_INTERVAL_S = ray_constants.env_float(
    "RAY_DASHBOARD_TASK_EVENTS_GC_JOB_SUMMARY_INTERVAL_S", 5.0
)
# Minimum seconds between "at capacity" warnings, to avoid log spam on every eviction.
_CAPACITY_WARNING_INTERVAL_S = 10.0

# Stat counter keys.
STAT_NUM_STORED = "num_task_events_stored"
STAT_TOTAL_REPORTED = "total_num_task_events_reported"
STAT_TOTAL_ATTEMPTS_DROPPED = "total_num_task_attempts_dropped"
STAT_TOTAL_PROFILE_DROPPED = "total_num_profile_task_events_dropped"
_TASK_TYPE_STAT = {
    TaskType.NORMAL_TASK: "total_num_normal_task",
    TaskType.ACTOR_CREATION_TASK: "total_num_actor_creation_task",
    TaskType.ACTOR_TASK: "total_num_actor_task",
    TaskType.DRIVER_TASK: "total_num_driver_task",
}

# A task attempt is identified by (task_id bytes, attempt number).
TaskAttempt = Tuple[bytes, int]

_NIL_TASK_ID = TaskID.nil().binary()
_NIL_JOB_ID = JobID.nil().binary()


def _is_nil_id(id_bytes: bytes, nil_bytes: bytes) -> bool:
    return not id_bytes or id_bytes == nil_bytes


def _task_attempt(task_event: gcs_pb2.TaskEvents) -> TaskAttempt:
    return (task_event.task_id, task_event.attempt_number)


def _worker_id(task_event: gcs_pb2.TaskEvents) -> bytes:
    return task_event.state_updates.worker_id


def _num_profile_events(task_event: gcs_pb2.TaskEvents) -> int:
    return len(task_event.profile_events.events)


class JobTaskSummary:
    """Per-job tracking of dropped task attempts and dropped profile events."""

    def __init__(self):
        self._num_profile_events_dropped = 0
        self._num_task_attempts_dropped_tracked = 0
        self._num_dropped_task_attempts_evicted = 0
        self._dropped_task_attempts: Set[TaskAttempt] = set()

    def record_task_attempt_dropped(self, task_attempt: TaskAttempt) -> None:
        self._dropped_task_attempts.add(task_attempt)
        self._num_task_attempts_dropped_tracked = len(self._dropped_task_attempts)

    def record_profile_events_dropped(self, count: int) -> None:
        self._num_profile_events_dropped += count

    def should_drop_task_attempt(self, task_attempt: TaskAttempt) -> bool:
        """A task attempt is dropped once any of its events have been dropped."""
        return task_attempt in self._dropped_task_attempts

    @property
    def num_profile_events_dropped(self) -> int:
        return self._num_profile_events_dropped

    @property
    def num_task_attempts_dropped(self) -> int:
        return (
            self._num_task_attempts_dropped_tracked
            + self._num_dropped_task_attempts_evicted
        )

    def on_job_ends(self) -> None:
        """No more events arrive for a finished job, so stop tracking its drops."""
        self._dropped_task_attempts.clear()

    def gc_old_dropped_task_attempts(self, job_id: bytes) -> None:
        """Trim the dropped-attempt set when it grows past the per-job cap."""
        max_tracked = MAX_DROPPED_TASK_ATTEMPTS_PER_JOB
        if len(self._dropped_task_attempts) <= max_tracked:
            return
        logger.info(
            "Evicting extra tracked dropped task attempts (%d > %d) for job %s. Set "
            "RAY_DASHBOARD_TASK_EVENTS_MAX_DROPPED_TASK_ATTEMPTS_PER_JOB to a higher "
            "value to track more.",
            len(self._dropped_task_attempts),
            max_tracked,
            job_id.hex(),
        )
        num_to_evict = len(self._dropped_task_attempts) - max_tracked
        # Evict an extra 10% to avoid trimming on every pass.
        num_to_evict = min(
            len(self._dropped_task_attempts), num_to_evict + int(0.1 * num_to_evict)
        )
        self._num_task_attempts_dropped_tracked = len(self._dropped_task_attempts)
        if num_to_evict == 0:
            return
        self._num_dropped_task_attempts_evicted += num_to_evict
        to_evict = list(self._dropped_task_attempts)[:num_to_evict]
        self._dropped_task_attempts.difference_update(to_evict)
        self._num_task_attempts_dropped_tracked = len(self._dropped_task_attempts)


class TaskEventStorage:
    """Bounded, deduplicated in-memory store of ``TaskEvents`` keyed by task attempt.

    - ``_tiers``: one insertion-ordered dict per GC priority tier (task attempt ->
      ``TaskEvents``); the first key is the oldest, so eviction drops the oldest entry in
      the lowest-priority non-empty tier.
    - ``_primary_index``: task attempt -> the tier holding it, so an entry is found (and
      moved between tiers when its priority changes) via
      ``_tiers[_primary_index[attempt]][attempt]``.
    - ``_task_index`` / ``_job_index`` / ``_worker_index``: id -> set of task attempts,
      for lookups by task / job / worker.
    """

    def __init__(
        self,
        max_num_task_events: int = MAX_NUM_TASK_EVENTS,
        gc_policy: Optional[FinishedTaskActorTaskGcPolicy] = None,
    ):
        self._max_num_task_events = max_num_task_events
        self._gc_policy = gc_policy or FinishedTaskActorTaskGcPolicy()
        # One insertion-ordered map per priority tier; oldest key first.
        self._tiers: List[Dict[TaskAttempt, gcs_pb2.TaskEvents]] = [
            {} for _ in range(self._gc_policy.max_priority)
        ]
        # Primary index: task attempt -> which tier holds it.
        self._primary_index: Dict[TaskAttempt, int] = {}
        # Secondary indices: id -> set of task attempts.
        self._task_index: Dict[bytes, Set[TaskAttempt]] = {}
        self._job_index: Dict[bytes, Set[TaskAttempt]] = {}
        self._worker_index: Dict[bytes, Set[TaskAttempt]] = {}
        self._job_task_summary: Dict[bytes, JobTaskSummary] = {}
        self._stats: collections.Counter = collections.Counter()
        self._last_capacity_warning_time = float("-inf")

    def add_or_replace_task_event(self, task_event: gcs_pb2.TaskEvents) -> None:
        """Add a new task attempt or merge into an existing one, evicting if over capacity."""
        self._stats[STAT_TOTAL_REPORTED] += 1
        job_id = task_event.job_id
        task_id = task_event.task_id
        if _is_nil_id(job_id, _NIL_JOB_ID) or _is_nil_id(task_id, _NIL_TASK_ID):
            # Missing task/job id, e.g. profile events created without a task id.
            logger.debug(
                "Skipping invalid task event with missing job or task id: %s",
                task_event,
            )
            return

        attempt = _task_attempt(task_event)
        if self._summary(job_id).should_drop_task_attempt(attempt):
            logger.debug(
                "Already dropping task %s attempt %d of job %s",
                task_id.hex(),
                task_event.attempt_number,
                job_id.hex(),
            )
            return

        if attempt in self._primary_index:
            self._update_existing(attempt, task_event)
        else:
            self._add_new(attempt, task_event)

        if (
            self._max_num_task_events > 0
            and self._stats[STAT_NUM_STORED] > self._max_num_task_events
        ):
            self._warn_capacity_reached()
            self._evict_task_event()

    def _warn_capacity_reached(self) -> None:
        now = time.monotonic()
        if now - self._last_capacity_warning_time < _CAPACITY_WARNING_INTERVAL_S:
            return
        self._last_capacity_warning_time = now
        logger.warning(
            "Max number of task events (%d) reached; old task events will be "
            "overwritten. Set RAY_DASHBOARD_TASK_EVENTS_MAX_NUM_TASK_EVENTS to a higher "
            "value to store more.",
            self._max_num_task_events,
        )

    def record_data_loss_from_worker(self, dropped_task_attempts) -> None:
        """Honor drops reported upstream: mark the attempts dropped and evict any partial
        copy already stored, so data loss is at task-attempt granularity."""
        for dropped in dropped_task_attempts:
            task_id = dropped.task_id
            attempt = (task_id, dropped.attempt_number)
            job_id = TaskID(task_id).job_id().binary()
            self._summary(job_id).record_task_attempt_dropped(attempt)
            self._stats[STAT_TOTAL_ATTEMPTS_DROPPED] += 1
            if attempt in self._primary_index:
                self._remove_task_attempt(attempt)

    def mark_tasks_failed_on_worker_dead(
        self, worker_id: bytes, worker_table_data: gcs_pb2.WorkerTableData
    ) -> None:
        """Mark all non-terminal task attempts run by a dead worker as failed."""
        attempts = self._worker_index.get(worker_id)
        if attempts is None:
            return
        error_info = RayErrorInfo(error_type=ErrorType.WORKER_DIED)
        error_info.error_message = (
            f"Worker running the task ({worker_id.hex()}) died with exit_type: "
            f"{worker_table_data.exit_type} with error_message: "
            f"{worker_table_data.exit_detail}"
        )
        failed_ts_ns = worker_table_data.end_time_ms * 10**6
        for attempt in list(attempts):
            self._mark_task_attempt_failed_if_needed(attempt, failed_ts_ns, error_info)

    def mark_tasks_failed_on_job_ends(
        self, job_id: bytes, job_finish_time_ns: int
    ) -> None:
        """Mark all non-terminal task attempts of a finished job as failed."""
        attempts = self._job_index.get(job_id)
        if attempts is None:
            return
        error_info = RayErrorInfo(error_type=ErrorType.WORKER_DIED)
        error_info.error_message = (
            f"Job finishes ({job_id.hex()}) as driver exits. "
            "Marking all non-terminal tasks as failed."
        )
        for attempt in list(attempts):
            self._mark_task_attempt_failed_if_needed(
                attempt, job_finish_time_ns, error_info
            )

    def update_job_summary_on_job_done(self, job_id: bytes) -> None:
        """Clear a finished job's dropped-attempt tracking (no more events will arrive)."""
        summary = self._job_task_summary.get(job_id)
        if summary is not None:
            summary.on_job_ends()

    def gc_job_summary(self) -> None:
        for job_id, summary in self._job_task_summary.items():
            summary.gc_old_dropped_task_attempts(job_id)

    @property
    def stats(self) -> collections.Counter:
        return self._stats

    @property
    def num_task_events_stored(self) -> int:
        return self._stats[STAT_NUM_STORED]

    def get_task_event(self, task_attempt: TaskAttempt) -> Optional[gcs_pb2.TaskEvents]:
        tier = self._primary_index.get(task_attempt)
        return None if tier is None else self._tiers[tier][task_attempt]

    def job_summary(self, job_id: bytes) -> Optional[JobTaskSummary]:
        return self._job_task_summary.get(job_id)

    def _summary(self, job_id: bytes) -> JobTaskSummary:
        summary = self._job_task_summary.get(job_id)
        if summary is None:
            summary = JobTaskSummary()
            self._job_task_summary[job_id] = summary
        return summary

    def _add_new(self, attempt: TaskAttempt, task_event: gcs_pb2.TaskEvents) -> None:
        tier = self._gc_policy.get_task_list_priority(task_event)
        self._tiers[tier][attempt] = task_event
        self._primary_index[attempt] = tier
        self._stats[STAT_NUM_STORED] += 1
        if task_event.HasField("task_info") and task_event.attempt_number == 0:
            self._increment_task_type(task_event.task_info.type)
        self._update_index(attempt, task_event)

    def _update_existing(
        self, attempt: TaskAttempt, task_event: gcs_pb2.TaskEvents
    ) -> None:
        tier = self._primary_index[attempt]
        existing = self._tiers[tier][attempt]
        if task_event.HasField("task_info") and not existing.HasField("task_info"):
            self._increment_task_type(task_event.task_info.type)

        existing.MergeFrom(task_event)

        num_profile = len(existing.profile_events.events)
        if num_profile > MAX_NUM_PROFILE_EVENTS_PER_TASK:
            to_drop = num_profile - MAX_NUM_PROFILE_EVENTS_PER_TASK
            del existing.profile_events.events[:to_drop]
            self._summary(existing.job_id).record_profile_events_dropped(to_drop)
            self._stats[STAT_TOTAL_PROFILE_DROPPED] += to_drop

        new_tier = self._gc_policy.get_task_list_priority(existing)
        if new_tier != tier:
            del self._tiers[tier][attempt]
            self._tiers[new_tier][attempt] = existing
            self._primary_index[attempt] = new_tier

        self._update_index(attempt, existing)

    def _increment_task_type(self, task_type) -> None:
        stat = _TASK_TYPE_STAT.get(task_type)
        if stat is not None:
            self._stats[stat] += 1

    def _update_index(
        self, attempt: TaskAttempt, task_event: gcs_pb2.TaskEvents
    ) -> None:
        self._task_index.setdefault(task_event.task_id, set()).add(attempt)
        self._job_index.setdefault(task_event.job_id, set()).add(attempt)
        worker_id = _worker_id(task_event)
        if worker_id:
            self._worker_index.setdefault(worker_id, set()).add(attempt)

    def _remove_from_index(
        self, attempt: TaskAttempt, task_event: gcs_pb2.TaskEvents
    ) -> None:
        self._discard(self._job_index, task_event.job_id, attempt)
        self._discard(self._task_index, task_event.task_id, attempt)
        worker_id = _worker_id(task_event)
        if worker_id:
            self._discard(self._worker_index, worker_id, attempt)
        del self._primary_index[attempt]

    @staticmethod
    def _discard(
        index: Dict[bytes, Set[TaskAttempt]], key: bytes, attempt: TaskAttempt
    ) -> None:
        attempts = index.get(key)
        if attempts is None:
            return
        attempts.discard(attempt)
        if not attempts:
            del index[key]

    def _remove_task_attempt(self, attempt: TaskAttempt) -> None:
        tier = self._primary_index[attempt]
        task_event = self._tiers[tier][attempt]
        num_profile = _num_profile_events(task_event)

        summary = self._summary(task_event.job_id)
        summary.record_profile_events_dropped(num_profile)
        summary.record_task_attempt_dropped(attempt)
        self._stats[STAT_NUM_STORED] -= 1
        self._stats[STAT_TOTAL_ATTEMPTS_DROPPED] += 1
        self._stats[STAT_TOTAL_PROFILE_DROPPED] += num_profile

        self._remove_from_index(attempt, task_event)
        del self._tiers[tier][attempt]

    def _evict_task_event(self) -> None:
        for tier in range(self._gc_policy.max_priority):
            if self._tiers[tier]:
                # The first key is the oldest (least recently inserted) in this tier.
                oldest = next(iter(self._tiers[tier]))
                self._remove_task_attempt(oldest)
                return

    @staticmethod
    def _is_task_terminated(task_event: gcs_pb2.TaskEvents) -> bool:
        """Whether the task attempt has reported a FINISHED or FAILED state."""
        if not task_event.HasField("state_updates"):
            return False
        state_ts_ns = task_event.state_updates.state_ts_ns
        return TaskStatus.FINISHED in state_ts_ns or TaskStatus.FAILED in state_ts_ns

    def _mark_task_attempt_failed_if_needed(
        self, attempt: TaskAttempt, failed_ts_ns: int, error_info: RayErrorInfo
    ) -> None:
        task_event = self._tiers[self._primary_index[attempt]][attempt]
        # Don't fail a task attempt that already reached a terminal state.
        if self._is_task_terminated(task_event):
            return
        task_event.state_updates.state_ts_ns[TaskStatus.FAILED] = failed_ts_ns
        task_event.state_updates.error_info.CopyFrom(error_info)
