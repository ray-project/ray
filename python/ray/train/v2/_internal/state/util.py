import time

from ray.train.v2._internal.state.schema import (
    ActorStatus,
    RunAttemptStatus,
    RunStatus,
    TrainRun,
    TrainRunAttempt,
)
from ray.util.state import get_actor

_GRACEFUL_ABORT_STATUS_DETAIL = "Run aborted due to user interrupt (SIGINT)."
_DEAD_CONTROLLER_ABORT_STATUS_DETAIL = (
    "Run aborted because the driver process exited unexpectedly."
)


def update_train_run_aborted(run: TrainRun, graceful: bool) -> None:
    run.status = RunStatus.ABORTED
    if graceful:
        run.status_detail = _GRACEFUL_ABORT_STATUS_DETAIL
    else:
        run.status_detail = _DEAD_CONTROLLER_ABORT_STATUS_DETAIL
    run.end_time_ns = current_time_ns()


def update_train_run_attempt_aborted(
    run_attempt: TrainRunAttempt, graceful: bool
) -> None:
    if graceful:
        run_attempt.status_detail = _GRACEFUL_ABORT_STATUS_DETAIL
    else:
        run_attempt.status_detail = _DEAD_CONTROLLER_ABORT_STATUS_DETAIL
    run_attempt.status = RunAttemptStatus.ABORTED
    run_attempt.end_time_ns = current_time_ns()
    mark_workers_dead(run_attempt)


def mark_workers_dead(run_attempt: TrainRunAttempt) -> None:
    for worker in run_attempt.workers:
        worker.status = ActorStatus.DEAD


def current_time_ns() -> int:
    return time.time_ns()


def is_actor_alive(actor_id: str, timeout: int) -> bool:
    """Returns whether actor is alive."""
    actor_state = get_actor(actor_id, timeout=timeout)
    return actor_state and actor_state.state != "DEAD"
