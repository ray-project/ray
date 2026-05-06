import time

from ray.data._internal.execution.interfaces.execution_options import ExecutionOptions
from ray.train._internal.data_config import DataConfig
from ray.train.v2._internal.state.schema import (
    ActorStatus,
    DataConfig as DataConfigSchema,
    DataExecutionOptions,
    ExecutionOptions as ExecutionOptionsSchema,
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


def construct_data_config(data_config: DataConfig) -> DataConfigSchema:
    """Serialize a user-facing DataConfig into the exportable schema.

    Note: This function assumes data_config._execution_options (a defaultdict)
    hasn't been read between initialization of the field and this function call.
    Any read materializes a dataset key and affects the data config shape,
    wrongly capturing a per dataset execution options even if the user only
    provided a default.
    """
    exec_options = data_config._execution_options

    per_dataset_execution_options = {}
    if exec_options:
        per_dataset_execution_options = {
            ds_name: execution_options_to_model(opts)
            for ds_name, opts in exec_options.items()
        }

    return DataConfigSchema(
        datasets_to_split=data_config._datasets_to_split,
        data_execution_options=DataExecutionOptions(
            default=execution_options_to_model(exec_options.default_factory()),
            per_dataset_execution_options=per_dataset_execution_options,
        ),
        enable_shard_locality=data_config._enable_shard_locality,
    )


def execution_options_to_model(
    execution_options: ExecutionOptions,
) -> ExecutionOptionsSchema:
    """Convert a ray.data ExecutionOptions object into the export schema model."""
    return ExecutionOptionsSchema(
        resource_limits=execution_options.resource_limits.to_resource_dict(),
        exclude_resources=execution_options.exclude_resources.to_resource_dict(),
        preserve_order=execution_options.preserve_order,
        actor_locality_enabled=execution_options.actor_locality_enabled,
        verbose_progress=execution_options.verbose_progress,
    )
