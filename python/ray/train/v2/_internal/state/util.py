import time
from typing import Any, Dict

from ray.train._internal.data_config import DataConfig
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


def construct_data_config_dict(data_config: DataConfig) -> Dict[str, Any]:
    exec_options = data_config._execution_options
    if isinstance(exec_options, dict):
        execution_options = {
            ds_name: options.to_dict() for ds_name, options in exec_options.items()
        }
    else:
        execution_options = exec_options.to_dict() if exec_options is not None else None

    return {
        "datasets_to_split": data_config._datasets_to_split,
        "execution_options": execution_options,
        "enable_shard_locality": data_config._enable_shard_locality,
    }
