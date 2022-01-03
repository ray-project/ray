# coding: utf-8
from abc import abstractmethod
import logging
from typing import Dict, List, Optional
import warnings

from ray.tune.resources import Resources
from ray.util.annotations import DeveloperAPI
from ray.tune.trial import Trial, Checkpoint

logger = logging.getLogger(__name__)


# Signals when a class is directly inherited from TrialExecutor.
# A warning is printed to inform users of TrialExecutor deprecation.
class _WarnOnDirectInheritanceMeta(type):
    def __new__(mcls, name, bases, module, **kwargs):
        if name not in ("RayTrialExecutor", "_MockTrialExecutor",
                        "TrialExecutor") and "TrialExecutor" in tuple(
                            base.__name__ for base in bases):
            deprecation_msg = (
                f"{name} inherits from TrialExecutor, which is being "
                "deprecated. "
                "RFC: https://github.com/ray-project/ray/issues/17593. "
                "Please reach out on the Ray Github if you have any concerns.")
            warnings.warn(deprecation_msg, DeprecationWarning)
        cls = super().__new__(mcls, name, bases, module, **kwargs)
        return cls


@DeveloperAPI
class TrialExecutor(metaclass=_WarnOnDirectInheritanceMeta):
    """Module for interacting with remote trainables.

    Manages platform-specific details such as resource handling
    and starting/stopping trials.
    """

    def __init__(self):
        """Initializes a new TrialExecutor.
        """
        self._cached_trial_state = {}
        self._trials_to_cache = set()

    def set_status(self, trial: Trial, status: str) -> None:
        """Sets status and checkpoints metadata if needed.

        Only checkpoints metadata if trial status is a terminal condition.
        PENDING, PAUSED, and RUNNING switches have checkpoints taken care of
        in the TrialRunner.

        Args:
            trial (Trial): Trial to checkpoint.
            status (Trial.status): Status to set trial to.
        """
        if trial.status == status:
            logger.debug("Trial %s: Status %s unchanged.", trial, trial.status)
        else:
            logger.debug("Trial %s: Changing status from %s to %s.", trial,
                         trial.status, status)
        trial.set_status(status)
        if status in [Trial.TERMINATED, Trial.ERROR]:
            self._trials_to_cache.add(trial)

    def mark_trial_to_checkpoint(self, trial: Trial) -> None:
        self._trials_to_cache.add(trial)

    def get_checkpoints(self) -> Dict[str, str]:
        """Returns a copy of mapping of the trial ID to pickled metadata."""
        for trial in self._trials_to_cache:
            self._cached_trial_state[trial.trial_id] = trial.get_json_state()
        self._trials_to_cache.clear()
        return self._cached_trial_state

    @abstractmethod
    def has_resources(self, resources: Resources) -> bool:
        """Returns whether this runner has at least the specified resources."""
        pass

    @abstractmethod
    def start_trial(self, trial: Trial) -> bool:
        """Starts the trial restoring from checkpoint if checkpoint is provided.

        Args:
            trial (Trial): Trial to be started.

        Returns:
            True if trial started successfully, False otherwise.
        """
        pass

    @abstractmethod
    def stop_trial(self,
                   trial: Trial,
                   error: bool = False,
                   error_msg: Optional[str] = None) -> None:
        """Stops the trial.

        Stops this trial, releasing all allocating resources.
        If stopping the trial fails, the run will be marked as terminated
        in error, but no exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.

        """
        pass

    def continue_training(self, trial: Trial) -> None:
        """Continues the training of this trial."""
        pass

    def pause_trial(self, trial: Trial) -> None:
        """Pauses the trial.

        We want to release resources (specifically GPUs) when pausing an
        experiment. This results in PAUSED state that similar to TERMINATED.
        """
        assert trial.status == Trial.RUNNING, trial.status
        try:
            self.save(trial, Checkpoint.MEMORY)
            self.stop_trial(trial)
            self.set_status(trial, Trial.PAUSED)
        except Exception:
            logger.exception("Error pausing runner.")
            self.set_status(trial, Trial.ERROR)

    @abstractmethod
    def reset_trial(self, trial: Trial, new_config: Dict,
                    new_experiment_tag: str) -> bool:
        """Tries to invoke `Trainable.reset()` to reset trial.

        Args:
            trial (Trial): Trial to be reset.
            new_config (dict): New configuration for Trial
                trainable.
            new_experiment_tag (str): New experiment name
                for trial.

        Returns:
            True if `reset` is successful else False.
        """
        pass

    @abstractmethod
    def get_running_trials(self) -> List[Trial]:
        """Returns all running trials."""
        pass

    def on_step_begin(self, trials: List[Trial]) -> None:
        """A hook called before running one step of the trial event loop.

        Args:
            trials (List[Trial]): The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        pass

    def on_step_end(self, trials: List[Trial]) -> None:
        """A hook called after running one step of the trial event loop.

        Args:
            trials (List[Trial]): The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        pass

    def force_reconcilation_on_next_step_end(self) -> None:
        pass

    @abstractmethod
    def get_next_available_trial(self) -> Optional[Trial]:
        """Blocking call that waits until one result is ready.

        Returns:
            Trial object that is ready for intermediate processing.
        """
        pass

    @abstractmethod
    def get_next_failed_trial(self) -> Optional[Trial]:
        """Non-blocking call that detects and returns one failed trial.

        Returns:
            A Trial object that is ready for failure processing. None if
            no failure detected.
        """
        pass

    @abstractmethod
    def fetch_result(self, trial: Trial) -> List[Trial]:
        """Fetches one result for the trial.

        Assumes the trial is running.

        Returns:
            Result object for the trial.
        """
        pass

    @abstractmethod
    def debug_string(self) -> str:
        """Returns a human readable message for printing to the console."""
        pass

    @abstractmethod
    def restore(self, trial: Trial) -> None:
        """Restores training state from a checkpoint.

        If checkpoint is None, try to restore from trial.checkpoint.
        If restoring fails, the trial status will be set to ERROR.

        Args:
            trial (Trial): Trial to be restored.

        Returns:
            False if error occurred, otherwise return True.
        """
        pass

    @abstractmethod
    def save(self,
             trial,
             storage: str = Checkpoint.PERSISTENT,
             result: Optional[Dict] = None) -> Checkpoint:
        """Saves training state of this trial to a checkpoint.

        If result is None, this trial's last result will be used.

        Args:
            trial (Trial): The state of this trial to be saved.
            storage (str): Where to store the checkpoint. Defaults to
                PERSISTENT.
            result (dict): The state of this trial as a dictionary to be saved.

        Returns:
            A Checkpoint object.
        """
        pass

    @abstractmethod
    def export_trial_if_needed(self, trial: Trial) -> Dict:
        """Exports model of this trial based on trial.export_formats.

        Args:
            trial (Trial): The state of this trial to be saved.

        Returns:
            A dict that maps ExportFormats to successfully exported models.
        """
        pass

    def has_gpus(self) -> bool:
        """Returns True if GPUs are detected on the cluster."""
        return False

    def cleanup(self, trials: List[Trial]) -> None:
        """Ensures that trials are cleaned up after stopping.

        Args:
            trials (List[Trial]): The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        pass

    def in_staging_grace_period(self) -> bool:
        """Returns True if trials have recently been staged."""
        return False

    def set_max_pending_trials(self, max_pending: int) -> None:
        """Set the maximum number of allowed pending trials."""
        pass
