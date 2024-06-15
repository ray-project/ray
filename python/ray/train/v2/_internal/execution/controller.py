import logging
import os
import time
from enum import Enum
from typing import Any, Callable, Dict

from ray.train.v2._internal.constants import (
    DEFAULT_HEALTH_CHECK_INTERVAL_S,
    RAY_TRAIN_HEALTH_CHECK_INTERVAL_S,
)
from ray.train.v2._internal.execution.failure_handling import (
    FailureDecision,
    FailurePolicy,
)
from ray.train.v2._internal.execution.scaling_policy import (
    ResizeDecision,
    ScalingDecision,
    ScalingPolicy,
)
from ray.train.v2._internal.execution.worker_group import WorkerGroup, WorkerGroupStatus
from ray.train.v2._internal.util import time_monotonic

logger = logging.getLogger(__file__)


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
        scaling_policy: ScalingPolicy,
        failure_policy: FailurePolicy,
    ):
        self._train_fn = train_fn

        self._scaling_policy = scaling_policy
        self._failure_policy = failure_policy
        self._worker_group = self.worker_group_cls()

        self._state = TrainControllerState.INITIALIZING

        self._latest_poll_time = float("-inf")
        self._health_check_interval_s = float(
            os.getenv(
                RAY_TRAIN_HEALTH_CHECK_INTERVAL_S, DEFAULT_HEALTH_CHECK_INTERVAL_S
            )
        )

    def _execute_scaling_decision(self, decision: ScalingDecision):
        """Executes scaling decisions."""
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

        if failure_decision == FailureDecision.RESTART:
            errors = worker_group_status.errors.values()
            logger.error(
                "Restarting workers after encountering "
                f"{len(errors)} worker errors:\n"
                + ("\n".join([str(e) for e in errors]))
            )
            self._set_state(TrainControllerState.RECOVERING)
        elif failure_decision == FailureDecision.RAISE:
            errors = worker_group_status.errors.values()
            logger.error(
                "Terminating training after encountering worker errors:\n"
                + ("\n".join([str(e) for e in errors]))
            )
            self._set_state(TrainControllerState.ERRORED)
        elif failure_decision == FailureDecision.NOOP:
            assert self._state == TrainControllerState.RUNNING
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

        # TODO: Handle failures in starting the worker group.
        self._worker_group.start(
            num_workers=num_workers, resources_per_worker=resources_per_worker
        )
        self._worker_group.run_train_fn(self._train_fn)
        # TODO: Consider starting the worker group asynchronously.
        self._set_state(TrainControllerState.RUNNING)

    def _shutdown(self):
        self._worker_group.shutdown()
        self._scaling_policy.on_controller_shutdown()

    def get_worker_group(self) -> WorkerGroup:
        return self._worker_group

    def get_state(self) -> TrainControllerState:
        return self._state

    def _set_state(self, state: TrainControllerState):
        self._state = state

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

        if worker_group_status.finished:
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
            self._execute_scaling_decision(scaling_decision)
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
                self._execute_scaling_decision(scaling_decision)

    def run(self):
        """Run the main control loop. Exits when training is finished or errored."""
        self._scaling_policy.on_controller_run_start()

        while self.get_state() not in (
            TrainControllerState.ERRORED,
            TrainControllerState.FINISHED,
        ):
            self._run_control_loop_iteration()

        self._shutdown()
