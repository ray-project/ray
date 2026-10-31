from typing import TYPE_CHECKING, Dict, Optional

from ray.air._internal.usage import tag_scheduler
from ray.tune.experiment import Trial
from ray.tune.result import DEFAULT_METRIC
from ray.util.annotations import DeveloperAPI, PublicAPI

if TYPE_CHECKING:
    from ray.tune.execution.tune_controller import TuneController


@DeveloperAPI
class TrialScheduler:
    """Interface for implementing a Trial Scheduler class.

    Note to Tune developers: If a new scheduler is added, please update
    `air/_internal/usage.py`.
    """

    CONTINUE = "CONTINUE"  #: Status for continuing trial execution
    PAUSE = "PAUSE"  #: Status for pausing trial execution
    STOP = "STOP"  #: Status for stopping trial execution
    # Caution: Temporary and anti-pattern! This means Scheduler calls
    # into Executor directly without going through TrialRunner.
    # TODO(xwjiang): Deprecate this after we control the interaction
    #  between schedulers and executor.
    NOOP = "NOOP"

    _metric = None

    _supports_buffered_results = True

    def __init__(self):
        tag_scheduler(self)

    @property
    def metric(self):
        return self._metric

    @property
    def supports_buffered_results(self):
        return self._supports_buffered_results

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], **spec
    ) -> bool:
        """Pass search properties to scheduler.

        This method acts as an alternative to instantiating schedulers
        that react to metrics with their own `metric` and `mode` parameters.

        Args:
            metric: Metric to optimize
            mode: One of ["min", "max"]. Direction to optimize.
            **spec: Any kwargs for forward compatibility.
                Info like Experiment.PUBLIC_KEYS is provided through here.

        Returns:
            True if the search properties were set successfully, False otherwise.
        """
        if self._metric and metric:
            return False
        if metric:
            self._metric = metric

        if self._metric is None:
            # Per default, use anonymous metric
            self._metric = DEFAULT_METRIC

        return True

    def on_trial_add(self, tune_controller: "TuneController", trial: Trial):
        """Called when a new trial is added to the trial runner.

        Args:
            tune_controller: The controller running the experiment.
            trial: The trial that was added.
        """

        raise NotImplementedError

    def on_trial_error(self, tune_controller: "TuneController", trial: Trial):
        """Notification for the error of trial.

        This will only be called when the trial is in the RUNNING state.

        Args:
            tune_controller: The controller running the experiment.
            trial: The trial that errored.
        """

        raise NotImplementedError

    def on_trial_result(
        self, tune_controller: "TuneController", trial: Trial, result: Dict
    ) -> str:
        """Called on each intermediate result returned by a trial.

        At this point, the trial scheduler can make a decision by returning
        one of CONTINUE, PAUSE, and STOP. This will only be called when the
        trial is in the RUNNING state.

        Args:
            tune_controller: The controller running the experiment.
            trial: The trial that reported the result.
            result: The result the trial reported.

        Returns:
            The scheduling decision for the trial: ``TrialScheduler.CONTINUE``,
            ``TrialScheduler.PAUSE``, or ``TrialScheduler.STOP``. A scheduler
            that has already changed the trial's state itself returns
            ``TrialScheduler.NOOP``.
        """

        raise NotImplementedError

    def on_trial_complete(
        self, tune_controller: "TuneController", trial: Trial, result: Dict
    ):
        """Notification for the completion of trial.

        This will only be called when the trial is in the RUNNING state and
        either completes naturally or by manual termination.

        Args:
            tune_controller: The controller running the experiment.
            trial: The trial that completed.
            result: The last result the trial reported.
        """

        raise NotImplementedError

    def on_trial_remove(self, tune_controller: "TuneController", trial: Trial):
        """Called to remove trial.

        This is called when the trial is in PAUSED or PENDING state. Otherwise,
        call `on_trial_complete`.

        Args:
            tune_controller: The controller running the experiment.
            trial: The trial to remove.
        """

        raise NotImplementedError

    def choose_trial_to_run(self, tune_controller: "TuneController") -> Optional[Trial]:
        """Called to choose a new trial to run.

        This should return one of the trials in tune_controller that is in
        the PENDING or PAUSED state. This function must be idempotent.

        If no trial is ready, return None.

        Args:
            tune_controller: The controller running the experiment.

        Returns:
            The trial to run next, or ``None`` if no trial is ready.
        """

        raise NotImplementedError

    def debug_string(self) -> str:
        """Returns a human readable message for printing to the console.

        Returns:
            The message describing the scheduler's state.
        """

        raise NotImplementedError

    def save(self, checkpoint_path: str):
        """Save trial scheduler to a checkpoint

        Args:
            checkpoint_path: Path of the file to write the scheduler state to.
        """
        raise NotImplementedError

    def restore(self, checkpoint_path: str):
        """Restore trial scheduler from checkpoint.

        Args:
            checkpoint_path: Path of the file to read the scheduler state from.
        """
        raise NotImplementedError


@PublicAPI
class FIFOScheduler(TrialScheduler):
    """Simple scheduler that just runs trials in submission order."""

    def __init__(self):
        super().__init__()

    def on_trial_add(self, tune_controller: "TuneController", trial: Trial):
        pass

    def on_trial_error(self, tune_controller: "TuneController", trial: Trial):
        pass

    def on_trial_result(
        self, tune_controller: "TuneController", trial: Trial, result: Dict
    ) -> str:
        return TrialScheduler.CONTINUE

    def on_trial_complete(
        self, tune_controller: "TuneController", trial: Trial, result: Dict
    ):
        pass

    def on_trial_remove(self, tune_controller: "TuneController", trial: Trial):
        pass

    def choose_trial_to_run(self, tune_controller: "TuneController") -> Optional[Trial]:
        for trial in tune_controller.get_trials():
            if trial.status == Trial.PENDING:
                return trial
        for trial in tune_controller.get_trials():
            if trial.status == Trial.PAUSED:
                return trial
        return None

    def debug_string(self) -> str:
        return "Using FIFO scheduling algorithm."
