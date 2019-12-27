# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.tune.trial import Trial, Checkpoint
from ray.tune.error import TuneError

logger = logging.getLogger(__name__)


class TrialExecutor(object):
    """Manages platform-specific details such as resource handling
    and starting/stopping trials.
    """

    def __init__(self, queue_trials=False):
        """Initializes a new TrialExecutor.

        Args:
            queue_trials (bool): Whether to queue trials when the cluster does
                not currently have enough resources to launch one. This should
                be set to True when running on an autoscaling cluster to enable
                automatic scale-up.
        """
        self._queue_trials = queue_trials
        self._cached_trial_state = {}

    def set_status(self, trial, status):
        """Sets status and checkpoints metadata if needed.

        Only checkpoints metadata if trial status is a terminal condition.
        PENDING, PAUSED, and RUNNING switches have checkpoints taken care of
        in the TrialRunner.

        Args:
            trial (Trial): Trial to checkpoint.
            status (Trial.status): Status to set trial to.
        """
        logger.debug("Trial %s: Changing status from %s to %s.", trial,
                     trial.status, status)
        trial.set_status(status)
        if status in [Trial.TERMINATED, Trial.ERROR]:
            self.try_checkpoint_metadata(trial)

    def try_checkpoint_metadata(self, trial):
        """Checkpoints metadata.

        Args:
            trial (Trial): Trial to checkpoint.
        """
        if trial.checkpoint.storage == Checkpoint.MEMORY:
            logger.debug("Trial %s: Not saving data for memory checkpoint.",
                         trial)
            return
        try:
            logger.debug("Trial %s: Saving trial metadata.", trial)
            self._cached_trial_state[trial.trial_id] = trial.__getstate__()
        except Exception:
            logger.exception("Trial %s: Error checkpointing trial metadata.",
                             trial)

    def get_checkpoints(self):
        """Returns a copy of mapping of the trial ID to pickled metadata."""
        return self._cached_trial_state.copy()

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "has_resources() method")

    def start_trial(self, trial, checkpoint=None):
        """Starts the trial restoring from checkpoint if checkpoint is provided.

        Args:
            trial (Trial): Trial to be started.
            checkpoint(Checkpoint): A Python object or path storing the state
            of trial.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "start_trial() method")

    def stop_trial(self, trial, error=False, error_msg=None, stop_logger=True):
        """Stops the trial.

        Stops this trial, releasing all allocating resources.
        If stopping the trial fails, the run will be marked as terminated
        in error, but no exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.
            stop_logger (bool): Whether to shut down the trial logger.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "stop_trial() method")

    def continue_training(self, trial):
        """Continues the training of this trial."""
        pass

    def pause_trial(self, trial):
        """Pauses the trial.

        We want to release resources (specifically GPUs) when pausing an
        experiment. This results in PAUSED state that similar to TERMINATED.
        """
        assert trial.status == Trial.RUNNING, trial.status
        try:
            self.save(trial, Checkpoint.MEMORY)
            self.stop_trial(trial, stop_logger=False)
            self.set_status(trial, Trial.PAUSED)
        except Exception:
            logger.exception("Error pausing runner.")
            self.set_status(trial, Trial.ERROR)

    def unpause_trial(self, trial):
        """Sets PAUSED trial to pending to allow scheduler to start."""
        assert trial.status == Trial.PAUSED, trial.status
        self.set_status(trial, Trial.PENDING)

    def resume_trial(self, trial):
        """Resumes PAUSED trials. This is a blocking call."""
        assert trial.status == Trial.PAUSED, trial.status
        self.start_trial(trial)

    def reset_trial(self, trial, new_config, new_experiment_tag):
        """Tries to invoke `Trainable.reset_config()` to reset trial.

        Args:
            trial (Trial): Trial to be reset.
            new_config (dict): New configuration for Trial
                trainable.
            new_experiment_tag (str): New experiment name
                for trial.

        Returns:
            True if `reset_config` is successful else False.
        """
        raise NotImplementedError

    def get_running_trials(self):
        """Returns all running trials."""
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "get_running_trials() method")

    def on_step_begin(self, trial_runner):
        """A hook called before running one step of the trial event loop."""
        pass

    def on_step_end(self, trial_runner):
        """A hook called after running one step of the trial event loop."""
        pass

    def on_no_available_trials(self, trial_runner):
        if self._queue_trials:
            return
        for trial in trial_runner.get_trials():
            if trial.status == Trial.PENDING:
                if not self.has_resources(trial.resources):
                    raise TuneError(
                        ("Insufficient cluster resources to launch trial: "
                         "trial requested {} but the cluster has only {}. "
                         "Pass `queue_trials=True` in "
                         "ray.tune.run() or on the command "
                         "line to queue trials until the cluster scales "
                         "up or resources become available. {}").format(
                             trial.resources.summary_string(),
                             self.resource_string(),
                             trial.get_trainable_cls().resource_help(
                                 trial.config)))
            elif trial.status == Trial.PAUSED:
                raise TuneError("There are paused trials, but no more pending "
                                "trials with sufficient resources.")

    def get_next_available_trial(self):
        """Blocking call that waits until one result is ready.

        Returns:
            Trial object that is ready for intermediate processing.
        """
        raise NotImplementedError

    def get_next_failed_trial(self):
        """Non-blocking call that detects and returns one failed trial.

        Returns:
            A Trial object that is ready for failure processing. None if
            no failure detected.
        """
        raise NotImplementedError

    def fetch_result(self, trial):
        """Fetches one result for the trial.

        Assumes the trial is running.

        Return:
            Result object for the trial.
        """
        raise NotImplementedError

    def debug_string(self):
        """Returns a human readable message for printing to the console."""
        raise NotImplementedError

    def resource_string(self):
        """Returns a string describing the total resources available."""
        raise NotImplementedError

    def restore(self, trial, checkpoint=None):
        """Restores training state from a checkpoint.

        If checkpoint is None, try to restore from trial.checkpoint.
        If restoring fails, the trial status will be set to ERROR.

        Args:
            trial (Trial): Trial to be restored.
            checkpoint (Checkpoint): Checkpoint to restore from.

        Return:
            False if error occurred, otherwise return True.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "restore() method")

    def save(self, trial, storage=Checkpoint.DISK, result=None):
        """Saves training state of this trial to a checkpoint.

        If result is None, this trial's last result will be used.

        Args:
            trial (Trial): The state of this trial to be saved.
            storage (str): Where to store the checkpoint. Defaults to DISK.
            result (dict): The state of this trial as a dictionary to be saved.

        Return:
            A Python object if storage==Checkpoint.MEMORY otherwise
            a path to the checkpoint.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "save() method")

    def export_trial_if_needed(self, trial):
        """Exports model of this trial based on trial.export_formats.

        Args:
            trial (Trial): The state of this trial to be saved.

        Return:
            A dict that maps ExportFormats to successfully exported models.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "export_trial_if_needed() method")

    def has_gpus(self):
        """Returns True if GPUs are detected on the cluster."""
        return None
