# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

from ray.tune.trial import Trial, Checkpoint

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
        trial.status = status
        if status in [Trial.TERMINATED, Trial.ERROR]:
            self.try_checkpoint_metadata(trial)

    def try_checkpoint_metadata(self, trial):
        """Checkpoints metadata.

        Args:
            trial (Trial): Trial to checkpoint.
        """
        if trial._checkpoint.storage == Checkpoint.MEMORY:
            logger.debug("Not saving data for trial w/ memory checkpoint.")
            return
        try:
            logger.debug("Saving trial metadata.")
            self._cached_trial_state[trial.trial_id] = trial.__getstate__()
        except Exception:
            logger.exception("Error checkpointing trial metadata.")

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

    def on_step_begin(self):
        """A hook called before running one step of the trial event loop."""
        pass

    def on_step_end(self):
        """A hook called after running one step of the trial event loop."""
        pass

    def get_next_available_trial(self):
        """Blocking call that waits until one result is ready.

        Returns:
            Trial object that is ready for intermediate processing.
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

        If checkpoint is None, try to restore from trial._checkpoint.
        If restoring fails, the trial status will be set to ERROR.

        Args:
            trial (Trial): Trial to be restored.
            checkpoint (Checkpoint): Checkpoint to restore from.

        Return:
            False if error occurred, otherwise return True.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "restore() method")

    def save(self, trial, storage=Checkpoint.DISK):
        """Saves training state of this trial to a checkpoint.

        Args:
            trial (Trial): The state of this trial to be saved.
            storage (str): Where to store the checkpoint. Defaults to DISK.

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
