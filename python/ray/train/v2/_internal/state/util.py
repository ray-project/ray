import time

from ray.train.v2._internal.state.schema import (
    ActorStatus,
    RunAttemptStatus,
    RunStatus,
    TrainRun,
    TrainRunAttempt,
)


def update_train_run_aborted(run: TrainRun) -> None:
    run.status = RunStatus.ABORTED
    run.status_detail = None  # TODO: Add status detail.
    run.end_time_ns = current_time_ns()


def update_train_run_attempt_aborted(run_attempt: TrainRunAttempt) -> None:
    run_attempt.status_detail = None  # TODO: Add status detail.
    run_attempt.status = RunAttemptStatus.ABORTED
    run_attempt.end_time_ns = current_time_ns()
    mark_workers_dead(run_attempt)


def mark_workers_dead(run_attempt: TrainRunAttempt) -> None:
    for worker in run_attempt.workers:
        worker.status = ActorStatus.DEAD


def current_time_ns() -> int:
    return time.time_ns()
