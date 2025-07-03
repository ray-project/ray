import time

from ray.train.v2._internal.state.schema import (
    ActorStatus,
    RunAttemptStatus,
    RunStatus,
    TrainRun,
    TrainRunAttempt,
)

_GRACEFUL_ABORT_STATUS_DETAIL = "User gracefully aborted run with SIGINT."
_DEAD_CONTROLLER_ABORT_STATUS_DETAIL = (
    "State actor aborted live run with dead controller."
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
