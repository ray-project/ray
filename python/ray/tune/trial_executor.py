# coding: utf-8
from abc import abstractmethod
from functools import lru_cache
import logging
import os
import time
from typing import Dict, List, Optional
import warnings

import ray
from ray.tune.resources import Resources
from ray.util.annotations import DeveloperAPI
from ray.tune.trial import Trial, Checkpoint
from ray.tune.cluster_info import is_ray_cluster

logger = logging.getLogger(__name__)


# Ideally we want to use @cache; but it's only available for python 3.9.
# Caching is only helpful/correct for no autoscaler case.
@lru_cache()
def _get_cluster_resources_no_autoscaler() -> Dict:
    return ray.cluster_resources()


def _get_trial_cpu_and_gpu(trial: Trial) -> Dict:
    cpu = trial.placement_group_factory.required_resources.get("CPU", 0)
    gpu = trial.placement_group_factory.required_resources.get("GPU", 0)
    return {"CPU": cpu, "GPU": gpu}


def _can_fulfill_no_autoscaler(trial: Trial) -> bool:
    """Calculates if there is enough resources for a PENDING trial.

    For no autoscaler case.
    """
    assert trial.status == Trial.PENDING
    trial_cpu_gpu = _get_trial_cpu_and_gpu(trial)

    return trial_cpu_gpu["CPU"] <= _get_cluster_resources_no_autoscaler().get(
        "CPU", 0
    ) and trial_cpu_gpu["GPU"] <= _get_cluster_resources_no_autoscaler().get(
        "GPU", 0)


@lru_cache()
def _get_insufficient_resources_warning_threshold() -> float:
    if is_ray_cluster():
        return float(
            os.environ.get(
                "TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S_AUTOSCALER", "60"))
    else:
        # Set the default to 10s so that we don't prematurely determine that
        # a cluster cannot fulfill the resources requirements.
        # TODO(xwjiang): Change it back once #18608 is resolved.
        return float(
            os.environ.get("TUNE_WARN_INSUFFICENT_RESOURCE_THRESHOLD_S", "60"))


# TODO(xwjiang): Consider having a help page with more detailed instructions.
@lru_cache()
def _get_insufficient_resources_warning_msg() -> str:
    msg = (
        f"No trial is running and no new trial has been started within"
        f" at least the last "
        f"{_get_insufficient_resources_warning_threshold()} seconds. "
        f"This could be due to the cluster not having enough "
        f"resources available to start the next trial. "
        f"Stop the tuning job and adjust the resources requested per trial "
        f"(possibly via `resources_per_trial` or via `num_workers` for rllib) "
        f"and/or add more resources to your Ray runtime.")
    if is_ray_cluster():
        return "Ignore this message if the cluster is autoscaling. " + msg
    else:
        return msg


# A beefed up version when Tune Error is raised.
def _get_insufficient_resources_error_msg(trial: Trial) -> str:
    trial_cpu_gpu = _get_trial_cpu_and_gpu(trial)
    return (
        f"Ignore this message if the cluster is autoscaling. "
        f"You asked for {trial_cpu_gpu['CPU']} cpu and "
        f"{trial_cpu_gpu['GPU']} gpu per trial, but the cluster only has "
        f"{_get_cluster_resources_no_autoscaler().get('CPU', 0)} cpu and "
        f"{_get_cluster_resources_no_autoscaler().get('GPU', 0)} gpu. "
        f"Stop the tuning job and adjust the resources requested per trial "
        f"(possibly via `resources_per_trial` or via `num_workers` for rllib) "
        f"and/or add more resources to your Ray runtime.")


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
        # The next two variables are used to keep track of if there is any
        # "progress" made between subsequent calls to `on_no_available_trials`.
        # TODO(xwjiang): Clean this up once figuring out who should have a
        #  holistic view of trials - runner or executor.
        #  Also iterating over list of trials every time is very inefficient.
        #  Need better visibility APIs into trials.
        # The start time since when all active trials have been in PENDING
        # state, or since last time we output a resource insufficent
        # warning message, whichever comes later.
        # -1 means either the TrialExecutor is just initialized without any
        # trials yet, or there are some trials in RUNNING state.
        self._no_running_trials_since = -1
        self._all_trials_size = -1

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
            self.try_checkpoint_metadata(trial)

    def try_checkpoint_metadata(self, trial: Trial) -> None:
        """Checkpoints trial metadata.

        Args:
            trial (Trial): Trial to checkpoint.
        """
        if trial.checkpoint.storage == Checkpoint.MEMORY:
            logger.debug("Trial %s: Not saving data for memory checkpoint.",
                         trial)
            return
        try:
            logger.debug("Trial %s: Saving trial metadata.", trial)
            # Lazy cache trials
            self._trials_to_cache.add(trial)
        except Exception:
            logger.exception("Trial %s: Error checkpointing trial metadata.",
                             trial)

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
    def start_trial(self,
                    trial: Trial,
                    checkpoint: Optional[Checkpoint] = None,
                    train: bool = True) -> bool:
        """Starts the trial restoring from checkpoint if checkpoint is provided.

        Args:
            trial (Trial): Trial to be started.
            checkpoint (Checkpoint): A Python object or path storing the state
            of trial.
            train (bool): Whether or not to start training.

        Returns:
            True if trial started successfully, False otherwise.
        """
        pass

    @abstractmethod
    def stop_trial(self,
                   trial: Trial,
                   error: bool = False,
                   error_msg: Optional[str] = None,
                   destroy_pg_if_cannot_replace: bool = True) -> None:
        """Stops the trial.

        Stops this trial, releasing all allocating resources.
        If stopping the trial fails, the run will be marked as terminated
        in error, but no exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.
            destroy_pg_if_cannot_replace (bool): Whether the trial's placement
            group should be destroyed if it cannot replace any staged ones.

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

    def unpause_trial(self, trial: Trial) -> None:
        """Sets PAUSED trial to pending to allow scheduler to start."""
        assert trial.status == Trial.PAUSED, trial.status
        self.set_status(trial, Trial.PENDING)

    def resume_trial(self, trial: Trial) -> None:
        """Resumes PAUSED trials. This is a blocking call."""
        assert trial.status == Trial.PAUSED, trial.status
        self.start_trial(trial)

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

    def _may_warn_insufficient_resources(self, all_trials):
        # This is approximately saying we are not making progress.
        if len(all_trials) == self._all_trials_size:
            if self._no_running_trials_since == -1:
                self._no_running_trials_since = time.monotonic()
            elif (time.monotonic() - self._no_running_trials_since >
                  _get_insufficient_resources_warning_threshold()):
                if not is_ray_cluster():  # autoscaler not enabled
                    # If any of the pending trial cannot be fulfilled,
                    # that's a good enough hint of trial resources not enough.
                    for trial in all_trials:
                        if (trial.status is Trial.PENDING
                                and not _can_fulfill_no_autoscaler(trial)):
                            # TODO(xwjiang):
                            #  Raise an Error once #18608 is resolved.
                            logger.warning(
                                _get_insufficient_resources_error_msg(trial))
                            break
                else:
                    # TODO(xwjiang): #17799.
                    #  Output a more helpful msg for autoscaler.
                    logger.warning(_get_insufficient_resources_warning_msg())
                self._no_running_trials_since = time.monotonic()
        else:
            self._no_running_trials_since = -1
        self._all_trials_size = len(all_trials)

    def on_no_available_trials(self, trials: List[Trial]) -> None:
        """
        Args:
            trials (List[Trial]): The list of trials. Note, refrain from
                providing TrialRunner directly here.
        """
        self._may_warn_insufficient_resources(trials)

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
    def restore(self,
                trial: Trial,
                checkpoint: Optional[Checkpoint] = None,
                block: bool = False) -> None:
        """Restores training state from a checkpoint.

        If checkpoint is None, try to restore from trial.checkpoint.
        If restoring fails, the trial status will be set to ERROR.

        Args:
            trial (Trial): Trial to be restored.
            checkpoint (Checkpoint): Checkpoint to restore from.
            block (bool): Whether or not to block on restore before returning.

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
