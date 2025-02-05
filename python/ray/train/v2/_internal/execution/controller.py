import logging
import os
import time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ray._private.auto_init_hook import wrap_auto_init
from ray.train import Checkpoint
from ray.train.v2._internal.constants import (
    DEFAULT_HEALTH_CHECK_INTERVAL_S,
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
)
from ray.train.v2._internal.exceptions import (
    TrainingFailedError,
    WorkerGroupStartupFailedError,
    WorkerGroupStartupTimeoutError,
)
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    ReportCallback,
    TrainContextCallback,
    WorkerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
)
from ray.train.v2._internal.execution.checkpoint.report_handler import (
    ReportCallbackHandler,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import (
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.storage import StorageContext, get_fs_and_path
from ray.train.v2._internal.execution.worker_group import WorkerGroup, WorkerGroupStatus
from ray.train.v2._internal.logging.logging import configure_controller_logger
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.result import Result
from ray.train.v2.api.callback import RayTrainCallback

logger = logging.getLogger(__name__)


class TrainControllerState(Enum):
    """The possible states that the training controller can be in
    while running the main execution control loop.

    States:
        RUNNING: The training controller is actively running training tasks.
        RECOVERING: The training controller is in the process of recovering
            from an error.
        INITIALIZING: The train controller is starting up.
            This is always the initial state of the controller.
        ERRORED: A terminal state indicating that training has encountered
            an error and cannot continue.
        FINISHED: A terminal state indicating that training has completed.
    """

    RUNNING = "RUNNING"
    INITIALIZING = "INITIALIZING"
    RECOVERING = "RECOVERING"
    ERRORED = "ERRORED"
    FINISHED = "FINISHED"


class TrainController:
    """Manages the execution of a distributed training job.

    Responsibilities include:
    * Triggering the training function to run on the worker group.
    * Monitoring the status of the worker group.
    * Handling scaling decisions by restarting the worker group.
    * Handling failure decisions by restarting the worker group or terminating training.
    * Running callback logic on different hooks in the control loop.
    """

    worker_group_cls = WorkerGroup

    def __init__(
        self,
        train_fn: Callable[[Dict[str, Any]], None],
        train_run_context: TrainRunContext,
        scaling_policy: ScalingPolicy,
        failure_policy: FailurePolicy,
        callbacks: Optional[List[RayTrainCallback]] = None,
        # TODO: [Deprecation]
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        self._train_run_context = train_run_context
        configure_controller_logger(self._train_run_context)
        self._train_fn = train_fn
        self._scaling_policy = scaling_policy
        self._failure_policy = failure_policy
        self._run_config = self._train_run_context.run_config
        self._callbacks = callbacks or []
        self._resume_from_checkpoint = resume_from_checkpoint
        self._storage_context = StorageContext(
            storage_path=self._run_config.storage_path,
            experiment_dir_name=self._run_config.name,
            storage_filesystem=self._run_config.storage_filesystem,
        )

        self._checkpoint_manager = CheckpointManager(
            checkpoint_config=self._run_config.checkpoint_config,
            storage_context=self._storage_context,
        )
        report_handler = ReportCallbackHandler(
            report_callbacks=(
                [self._checkpoint_manager]
                + [c for c in self._callbacks if isinstance(c, ReportCallback)]
            )
        )

        # Group callbacks by the hooks they're subscribed to.
        self._controller_callbacks = [self._scaling_policy] + [
            c for c in self._callbacks if isinstance(c, ControllerCallback)
        ]
        # Group callbacks that will be propagated to the worker group,
        # train worker and the train context.
        worker_group_callbacks_to_propagate = [report_handler] + [
            c
            for c in self._callbacks
            if isinstance(
                c, (WorkerGroupCallback, WorkerCallback, TrainContextCallback)
            )
        ]

        self._worker_group = self.worker_group_cls(
            train_run_context=self._train_run_context,
            callbacks=worker_group_callbacks_to_propagate,
        )
        self._state = TrainControllerState.INITIALIZING

        self._latest_poll_time = float("-inf")
        self._health_check_interval_s = float(
            os.getenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, DEFAULT_HEALTH_CHECK_INTERVAL_S)
        )
        self._training_failed_error: Optional[TrainingFailedError] = None

    def _execute_scaling_decision(
        self, decision: ScalingDecision, worker_group_status: WorkerGroupStatus
    ):
        """Executes scaling decisions."""
        for callback in self._controller_callbacks:
            callback.before_controller_execute_scaling_decision(
                decision, worker_group_status
            )

        if isinstance(decision, ResizeDecision):
            self._restart_worker_group(
                num_workers=decision.num_workers,
                resources_per_worker=decision.resources_per_worker,
            )

    def _execute_failure_decision(
        self, failure_decision: FailureDecision, worker_group_status: WorkerGroupStatus
    ):
        """Executes failure handling decisions (ex: restart, terminate)."""
        assert worker_group_status.errors

        for callback in self._controller_callbacks:
            callback.before_controller_execute_failure_decision(
                failure_decision, worker_group_status
            )

        if failure_decision == FailureDecision.NOOP:
            assert self._state == TrainControllerState.RUNNING
            return

        errors_str = "\n".join(
            [
                f"[Rank {worker_rank}]\n{error}"
                for worker_rank, error in worker_group_status.errors.items()
            ]
        )

        if failure_decision == FailureDecision.RESTART:
            logger.error(
                "Restarting training worker group after encountering "
                f"failures on {len(worker_group_status.errors)} worker(s):\n"
                f"{errors_str}"
            )
            # Shutdown the worker group so that we don't keep polling errored tasks.
            self._worker_group.shutdown()
            self._set_state(TrainControllerState.RECOVERING)
        elif failure_decision == FailureDecision.RAISE:
            logger.error(
                "Terminating training worker group after encountering "
                f"failure(s) on {len(worker_group_status.errors)} worker(s):\n"
                f"{errors_str}"
            )
            self._set_state(TrainControllerState.ERRORED)
            self._training_failed_error = TrainingFailedError(
                worker_failures=worker_group_status.errors
            )
        else:
            raise ValueError(f"Unexpected failure decision: {failure_decision}")

    def _poll_workers(self) -> WorkerGroupStatus:
        # Ensure that the time between polls is at least HEALTH_CHECK_INTERVAL_S.
        time_since_last_poll = time_monotonic() - self._latest_poll_time
        if time_since_last_poll < self._health_check_interval_s:
            remaining_time = max(
                self._health_check_interval_s - time_since_last_poll, 0
            )
            time.sleep(remaining_time)

        status = self._worker_group.poll_status(timeout=self._health_check_interval_s)
        self._latest_poll_time = time_monotonic()
        return status

    def _restart_worker_group(self, num_workers: int, resources_per_worker: dict):
        """Restart the worker group and launch the train function."""
        self._worker_group.shutdown()

        # If there's a latest checkpoint that's been committed,
        # use it to restore the worker group.
        latest_checkpoint_result = self._checkpoint_manager.latest_checkpoint_result
        latest_checkpoint = (
            latest_checkpoint_result.checkpoint if latest_checkpoint_result else None
        )
        placement_strategy = self._scaling_policy.scaling_config.placement_strategy

        # Start the worker group with the latest checkpoint if there is one.
        # Otherwise, start the worker group with the checkpoint set by controller.
        # Finally, if there is no checkpoint, start the worker group with None.
        try:
            self._worker_group.start(
                train_fn=self._train_fn,
                num_workers=num_workers,
                resources_per_worker=resources_per_worker,
                placement_strategy=placement_strategy,
                checkpoint=latest_checkpoint or self._resume_from_checkpoint,
            )
        except (WorkerGroupStartupTimeoutError, WorkerGroupStartupFailedError) as e:
            logger.error(
                "Retrying the launch of the training worker group. "
                f"The previous launch attempt encountered the following failure:\n{e}"
            )

            # TODO: Should this logic go through the failure policy?
            # The current logic will always try recovering unconditionally
            # on startup errors without a retry limit.
            self._set_state(TrainControllerState.RECOVERING)
            return

        # TODO: Consider starting the worker group asynchronously.
        self._set_state(TrainControllerState.RUNNING)

    def _start(self):
        for callback in self._controller_callbacks:
            callback.after_controller_start()

    def _shutdown(self):
        self._worker_group.shutdown()

        for callback in self._controller_callbacks:
            callback.before_controller_shutdown()

    def get_worker_group(self) -> WorkerGroup:
        return self._worker_group

    def get_state(self) -> TrainControllerState:
        return self._state

    def _set_state(self, state: TrainControllerState):
        previous_state = self._state
        self._state = state

        for callback in self._controller_callbacks:
            callback.after_controller_state_update(previous_state, state)

    def _run_control_loop_iteration(self):
        """Run a single iteration of the control loop.

        Steps:
        1. Poll the worker group for status.
        2. If the worker group is initializing or recovering from an error,
            make a scaling decision and execute it.
        3. If the worker group has finished, set the controller state to FINISHED.
        4. If the worker group has errors, make a failure decision and execute it.
        5. Otherwise, the worker group is running healthily.
            Query the scaling policy for a scaling decision and execute it.
        """
        assert self.get_state() in (
            TrainControllerState.RUNNING,
            TrainControllerState.RECOVERING,
            TrainControllerState.INITIALIZING,
        ), self.get_state()

        worker_group_status = self._poll_workers()

        if worker_group_status.finished and not worker_group_status.errors:
            self._set_state(TrainControllerState.FINISHED)
            return

        if self.get_state() in (
            TrainControllerState.INITIALIZING,
            TrainControllerState.RECOVERING,
        ):
            scaling_decision = (
                self._scaling_policy.make_decision_for_non_running_worker_group(
                    worker_group_status
                )
            )
            self._execute_scaling_decision(scaling_decision, worker_group_status)
        elif self.get_state() == TrainControllerState.RUNNING:
            if worker_group_status.errors:
                failure_decision = self._failure_policy.make_decision(
                    worker_group_status
                )
                self._execute_failure_decision(failure_decision, worker_group_status)
            else:
                scaling_decision = (
                    self._scaling_policy.make_decision_for_running_worker_group(
                        worker_group_status
                    )
                )
                self._execute_scaling_decision(scaling_decision, worker_group_status)

    @wrap_auto_init
    def run(self):
        """Run the main control loop. Exits when training is finished or errored."""
        self._start()

        while self.get_state() not in (
            TrainControllerState.ERRORED,
            TrainControllerState.FINISHED,
        ):
            self._run_control_loop_iteration()

        self._shutdown()

    def get_result(self) -> Result:
        """Get the final training result from the TrainController."""

        controller_state = self.get_state()
        if controller_state not in (
            TrainControllerState.FINISHED,
            TrainControllerState.ERRORED,
        ):
            raise ValueError(
                f"Cannot get result when controller is in state {controller_state}"
            )

        latest_checkpoint_result = self._checkpoint_manager.latest_checkpoint_result
        latest_metrics = (
            latest_checkpoint_result.metrics if latest_checkpoint_result else None
        )
        latest_checkpoint = (
            latest_checkpoint_result.checkpoint if latest_checkpoint_result else None
        )
        best_checkpoints = [
            (r.checkpoint, r.metrics)
            for r in self._checkpoint_manager.best_checkpoint_results
        ]
        storage_filesystem, storage_fs_path = get_fs_and_path(
            self._run_config.storage_path, self._run_config.storage_filesystem
        )
        experiment_fs_path = Path(storage_fs_path, self._run_config.name).as_posix()

        return Result(
            metrics=latest_metrics,
            checkpoint=latest_checkpoint,
            error=self._training_failed_error,
            path=experiment_fs_path,
            best_checkpoints=best_checkpoints,
            _storage_filesystem=storage_filesystem,
        )
