import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from ray._private.auto_init_hook import wrap_auto_init
from ray.train.v2._internal.constants import (
    DEFAULT_HEALTH_CHECK_INTERVAL_S,
    HEALTH_CHECK_INTERVAL_S_ENV_VAR,
)
from ray.train.v2._internal.exceptions import (
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
from ray.train.v2._internal.execution.controller.state import (
    ErroredState,
    FinishedState,
    InitializingState,
    ReschedulingState,
    ResizingState,
    RestartingState,
    RunningState,
    SchedulingState,
    TrainControllerState,
)
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import (
    NoopDecision,
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroup,
    WorkerGroupPollStatus,
)
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2._internal.logging.logging import configure_controller_logger
from ray.train.v2._internal.util import time_monotonic
from ray.train.v2.api.callback import RayTrainCallback
from ray.train.v2.api.exceptions import TrainingFailedError
from ray.train.v2.api.result import Result

logger = logging.getLogger(__name__)


@dataclass
class TrainControllerLoopIterationResult:
    """The result of a single iteration of the control loop."""

    run_attempt_id: str
    previous_state: TrainControllerState
    next_state: TrainControllerState
    training_failed_error: Optional[TrainingFailedError] = None

    def __repr__(self) -> str:
        return (
            f"TrainControllerLoopIterationResult(\n"
            f"    run_attempt_id={self.run_attempt_id},\n"
            f"    previous_state={self.previous_state._state_type.state_name},\n"
            f"    next_state={self.next_state._state_type.state_name}\n"
            f"    training_failed_error={self.training_failed_error}\n"
            f")"
        )


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
    ):
        self._train_run_context = train_run_context
        configure_controller_logger(self._train_run_context)
        self._train_fn = train_fn
        self._scaling_policy = scaling_policy
        self._failure_policy = failure_policy
        self._run_config = self._train_run_context.run_config
        self._callbacks = callbacks or []
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
        self._worker_group_callbacks_to_propagate = [report_handler] + [
            c
            for c in self._callbacks
            if isinstance(
                c, (WorkerGroupCallback, WorkerCallback, TrainContextCallback)
            )
        ]

        self._health_check_interval_s = float(
            os.getenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, DEFAULT_HEALTH_CHECK_INTERVAL_S)
        )

        self._worker_group: Optional[WorkerGroup] = None
        self._state = InitializingState()

        # TODO: These can be attributes of a RunAttempt?
        self._latest_poll_time = float("-inf")

    def _execute_scaling_decision(
        self, decision: ScalingDecision
    ) -> TrainControllerState:
        """Executes scaling decisions."""
        for callback in self._controller_callbacks:
            callback.before_controller_execute_scaling_decision(decision)

        if isinstance(decision, ResizeDecision):
            if self._worker_group:
                self._shutdown_worker_group()

            worker_group_started = self._start_worker_group(
                num_workers=decision.num_workers,
                resources_per_worker=decision.resources_per_worker,
            )

            if worker_group_started:
                next_state = RunningState()
            else:
                next_state = ReschedulingState()

            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=self._state,
                next_state=next_state,
            )

    def _execute_failure_decision(
        self,
        failure_decision: FailureDecision,
        worker_group_status: WorkerGroupPollStatus,
    ) -> TrainControllerState:
        """Executes failure handling decisions (ex: restart, terminate)."""
        assert worker_group_status.errors

        controller_state = self.get_state()

        for callback in self._controller_callbacks:
            callback.before_controller_execute_failure_decision(failure_decision)

        # TODO: What should we do here?
        # This currently never happens because there must be errors.
        if failure_decision == FailureDecision.NOOP:
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=RunningState(),
            )

        errors_str = worker_group_status.get_error_string()
        training_failed_error = TrainingFailedError(
            error_message=errors_str, worker_failures=worker_group_status.errors
        )

        if failure_decision == FailureDecision.RESTART:
            logger.error(
                "Restarting training worker group after encountering "
                f"failures on {len(worker_group_status.errors)} worker(s):\n"
                f"{errors_str}"
            )
            next_state = RestartingState(training_failed_error=training_failed_error)
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=next_state,
                training_failed_error=training_failed_error,
            )
        elif failure_decision == FailureDecision.RAISE:
            logger.error(
                "Terminating training worker group after encountering "
                f"failure(s) on {len(worker_group_status.errors)} worker(s):\n"
                f"{errors_str}"
            )
            next_state = ErroredState(training_failed_error=training_failed_error)
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=next_state,
                training_failed_error=training_failed_error,
            )
        else:
            raise ValueError(f"Unexpected failure decision: {failure_decision}")

    def _poll_workers(self) -> WorkerGroupPollStatus:
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

    def _start_worker_group(self, num_workers: int, resources_per_worker: dict) -> bool:
        """Start the worker group and launch the train function.

        Returns:
            True if the worker group was successfully started, False otherwise.
        """

        # If there's a latest checkpoint that's been committed,
        # use it to restore the worker group.
        latest_checkpoint_result = self._checkpoint_manager.latest_checkpoint_result
        latest_checkpoint = (
            latest_checkpoint_result.checkpoint if latest_checkpoint_result else None
        )
        placement_strategy = self._scaling_policy.scaling_config.placement_strategy

        worker_group_context = WorkerGroupContext(
            run_attempt_id=self._get_run_attempt_id(),
            train_fn=self._train_fn,
            num_workers=num_workers,
            resources_per_worker=resources_per_worker,
            placement_strategy=placement_strategy,
            checkpoint=latest_checkpoint,
        )

        # Start the worker group with the latest checkpoint if there is one.
        # Otherwise, start the worker group with the checkpoint set by controller.
        # Finally, if there is no checkpoint, start the worker group with None.
        try:
            self._worker_group = self.worker_group_cls.create(
                train_run_context=self._train_run_context,
                worker_group_context=worker_group_context,
                callbacks=self._worker_group_callbacks_to_propagate,
            )
        except (WorkerGroupStartupTimeoutError, WorkerGroupStartupFailedError) as e:
            logger.error(
                "Retrying the launch of the training worker group. "
                f"The previous launch attempt encountered the following failure:\n{e}"
            )

            # TODO: Should this logic go through the failure policy?
            # The current logic will always try recovering unconditionally
            # on startup errors without a retry limit.
            return False

        # TODO: Consider starting the worker group asynchronously.
        return True

    def _start(self):
        for callback in self._controller_callbacks:
            callback.after_controller_start()

    def _shutdown(self):
        if self._worker_group:
            self._shutdown_worker_group()

        for callback in self._controller_callbacks:
            callback.before_controller_shutdown()

    def _shutdown_worker_group(self):
        """Shutdown the worker group and set the worker group to None."""
        self._worker_group.shutdown()
        self._worker_group = None

    def get_worker_group(self) -> Optional[WorkerGroup]:
        return self._worker_group

    def get_state(self) -> TrainControllerState:
        return self._state

    def _set_state(self, state: TrainControllerState):
        previous_state = self._state
        self._state = state

        for callback in self._controller_callbacks:
            callback.after_controller_state_update(previous_state, state)

    def _step(self) -> TrainControllerLoopIterationResult:
        """Run a single iteration of the control loop.

        Returns:
            The result of the iteration.
        """
        controller_state = self.get_state()

        if isinstance(controller_state, InitializingState):
            scaling_decision = (
                self._scaling_policy.make_decision_for_non_running_worker_group()
            )
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=SchedulingState(scaling_decision),
            )
        elif isinstance(controller_state, SchedulingState):
            return self._execute_scaling_decision(controller_state.scaling_decision)
        elif isinstance(controller_state, ReschedulingState):
            scaling_decision = (
                self._scaling_policy.make_decision_for_non_running_worker_group()
            )
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=SchedulingState(scaling_decision),
            )
        elif isinstance(controller_state, RunningState):
            worker_group_status = self._poll_workers()

            if worker_group_status.finished and not worker_group_status.errors:
                return TrainControllerLoopIterationResult(
                    run_attempt_id=self._get_run_attempt_id(),
                    previous_state=controller_state,
                    next_state=FinishedState(),
                )
            if worker_group_status.errors:
                failure_decision = self._failure_policy.make_decision(
                    worker_group_status
                )
                return self._execute_failure_decision(
                    failure_decision, worker_group_status
                )
            else:
                scaling_decision = self._scaling_policy.make_decision_for_running_worker_group(
                    worker_group_state=self.get_worker_group().get_worker_group_state(),
                    worker_group_status=worker_group_status,
                )

                if isinstance(scaling_decision, ResizeDecision):
                    next_state = ResizingState(
                        scaling_decision=scaling_decision,
                    )
                elif isinstance(scaling_decision, NoopDecision):
                    next_state = RunningState()
                else:
                    raise ValueError(f"Unexpected scaling decision: {scaling_decision}")

                return TrainControllerLoopIterationResult(
                    run_attempt_id=self._get_run_attempt_id(),
                    previous_state=controller_state,
                    next_state=next_state,
                )
        elif isinstance(controller_state, RestartingState):
            scaling_decision = (
                self._scaling_policy.make_decision_for_non_running_worker_group()
            )
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=SchedulingState(scaling_decision=scaling_decision),
            )
        elif isinstance(controller_state, ResizingState):
            return TrainControllerLoopIterationResult(
                run_attempt_id=self._get_run_attempt_id(),
                previous_state=controller_state,
                next_state=SchedulingState(
                    scaling_decision=controller_state.scaling_decision
                ),
            )
        else:
            raise ValueError(f"Unexpected controller state: {controller_state}")

    def _generate_run_attempt_id(self):
        self._run_attempt_id = uuid.uuid4().hex
        return self._run_attempt_id

    def _get_run_attempt_id(self):
        return self._run_attempt_id

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
        controller_state = self.get_state()
        assert not controller_state.is_terminal()

        if controller_state.needs_new_run_attempt():
            self._generate_run_attempt_id()

        result = self._step()

        self._set_state(result.next_state)

    @wrap_auto_init
    def run(self):
        """Run the main control loop. Exits when training is finished or errored."""
        self._start()

        while not self.get_state().is_terminal():
            self._run_control_loop_iteration()

        self._shutdown()

    def _build_result(self) -> Result:
        storage = self._checkpoint_manager._storage_context

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

        # Provide the history of metrics attached to checkpoints as a dataframe.
        metrics_dataframe = None
        if best_checkpoints:
            metrics_dataframe = pd.DataFrame([m for _, m in best_checkpoints])

        return Result(
            metrics=latest_metrics,
            checkpoint=latest_checkpoint,
            error=self.get_training_failed_error(),
            path=storage.experiment_fs_path,
            best_checkpoints=best_checkpoints,
            metrics_dataframe=metrics_dataframe,
            _storage_filesystem=storage.storage_filesystem,
        )

    def get_result(self) -> Result:
        """Get the final training result from the TrainController."""

        controller_state = self.get_state()
        if not controller_state.is_terminal():
            raise ValueError(
                f"Cannot get result when controller is in state {controller_state}"
            )

        return self._build_result()

    def get_training_failed_error(self) -> Optional[TrainingFailedError]:
        """Get the training failed error from the controller state.

        Returns:
            The training failed error if the controller is in an errored state,
            None otherwise.
        """
        controller_state = self.get_state()

        if isinstance(controller_state, ErroredState):
            return controller_state.training_failed_error

        return None
