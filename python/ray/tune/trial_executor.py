# coding: utf-8
from abc import abstractmethod
import logging
from typing import Dict, List, Optional, Union

from ray.exceptions import RayTaskError
from ray.tune import TuneError
from ray.util.annotations import DeveloperAPI
from ray.tune.trial import Trial, _TuneCheckpoint

logger = logging.getLogger(__name__)


# Signals when a class is directly inherited from TrialExecutor.
# A warning is printed to inform users of TrialExecutor deprecation.
class _WarnOnDirectInheritanceMeta(type):
    def __new__(mcls, name, bases, module, **kwargs):
        if (
            name
            not in (
                "RayTrialExecutor",
                "_MockTrialExecutor",
                "TrialExecutor",
            )
            and "TrialExecutor" in tuple(base.__name__ for base in bases)
        ):
            raise DeprecationWarning(
                f"{name} inherits from TrialExecutor, which is being "
                "deprecated. "
                "RFC: https://github.com/ray-project/ray/issues/17593. "
                "Please reach out on the Ray Github if you have any concerns."
            )

        cls = super().__new__(mcls, name, bases, module, **kwargs)
        return cls


@DeveloperAPI
class TrialExecutor(metaclass=_WarnOnDirectInheritanceMeta):
    """Module for interacting with remote trainables.

    Manages platform-specific details such as resource handling
    and starting/stopping trials.
    """

    def __init__(self):
        """Initializes a new TrialExecutor."""
        self._cached_trial_state = {}
        self._trials_to_cache = set()

    def set_status(self, trial: Trial, status: str) -> None:
        """Sets status and checkpoints metadata if needed.

        Only checkpoints metadata if trial status is a terminal condition.
        PENDING, PAUSED, and RUNNING switches have checkpoints taken care of
        in the TrialRunner.

        Args:
            trial: Trial to checkpoint.
            status: Status to set trial to.
        """
        if trial.status == status:
            logger.debug("Trial %s: Status %s unchanged.", trial, trial.status)
        else:
            logger.debug(
                "Trial %s: Changing status from %s to %s.", trial, trial.status, status
            )
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
    def start_trial(self, trial: Trial) -> bool:
        """Starts the trial restoring from checkpoint if checkpoint is provided.

        Args:
            trial: Trial to be started.

        Returns:
            True if trial started successfully, False otherwise.
        """
        pass

    @abstractmethod
    def stop_trial(
        self,
        trial: Trial,
        error: bool = False,
        exc: Optional[Union[TuneError, RayTaskError]] = None,
    ) -> None:
        """Stops the trial.

        Stops this trial, releasing all allocating resources.
        If stopping the trial fails, the run will be marked as terminated
        in error, but no exception will be thrown.

        Args:
            error: Whether to mark this trial as terminated in error.
            exc: Optional exception.

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
            self.save(trial, _TuneCheckpoint.MEMORY)
            self.stop_trial(trial)
            self.set_status(trial, Trial.PAUSED)
        except Exception:
            logger.exception("Error pausing runner.")
            self.set_status(trial, Trial.ERROR)

    @abstractmethod
    def reset_trial(
        self, trial: Trial, new_config: Dict, new_experiment_tag: str
    ) -> bool:
        """Tries to invoke `Trainable.reset()` to reset trial.

        Args:
            trial: Trial to be reset.
            new_config: New configuration for Trial
                trainable.
            new_experiment_tag: New experiment name
                for trial.

        Returns:
            True if `reset` is successful else False.
        """
        pass

    def on_step_begin(self, trials: List[Trial]) -> None:
        """A hook called before running one step of the trial event loop.

        Args:
            trials: The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        pass

    def on_step_end(self, trials: List[Trial]) -> None:
        """A hook called after running one step of the trial event loop.

        Args:
            trials: The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        pass

    def force_reconcilation_on_next_step_end(self) -> None:
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
            trial: Trial to be restored.

        Returns:
            False if error occurred, otherwise return True.
        """
        pass

    @abstractmethod
    def save(
        self,
        trial: Trial,
        storage: str = _TuneCheckpoint.PERSISTENT,
        result: Optional[Dict] = None,
    ) -> _TuneCheckpoint:
        """Saves training state of this trial to a checkpoint.

        If result is None, this trial's last result will be used.

        Args:
            trial: The state of this trial to be saved.
            storage: Where to store the checkpoint. Defaults to
                PERSISTENT.
            result: The state of this trial as a dictionary to be saved.

        Returns:
            A Checkpoint object.
        """
        pass

    @abstractmethod
    def export_trial_if_needed(self, trial: Trial) -> Dict:
        """Exports model of this trial based on trial.export_formats.

        Args:
            trial: The state of this trial to be saved.

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
            trials: The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        pass

    def set_max_pending_trials(self, max_pending: int) -> None:
        """Set the maximum number of allowed pending trials."""
        pass
