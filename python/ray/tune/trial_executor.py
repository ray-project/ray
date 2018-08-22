# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import traceback

from ray.tune.trial import Trial, Checkpoint


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

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "has_resources() method")

    def start_trial(self, trial, checkpoint=None):
        """Starts the trial restoring from checkpoint if checkpoint != None.

        If an error is encountered when starting the trial, an exception will
        be thrown.

        Args:
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

    def restart_trial(self, trial, error_msg=None):
        """Restarts the trial.

        The state of the trial should restore from the last checkpoint.

        Args:
            error_msg (str): Optional error message.
        """
        try:
            print("Attempting to recover trial state from last checkpoint")
            self.stop_trial(
                trial, error=True, error_msg=error_msg, stop_logger=False)
            trial.result_logger.flush()
            self.start_trial(trial)
        except Exception:
            error_msg = traceback.format_exc()
            print("Error recovering trial from checkpoint, abort:", error_msg)
            self.stop_trial(trial, error=True, error_msg=error_msg)

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
            trial.status = Trial.PAUSED
        except Exception:
            print("Error pausing runner:", traceback.format_exc())
            trial.status = Trial.ERROR

    def unpause_trial(self, trial):
        """Sets PAUSED trial to pending to allow scheduler to start."""
        assert trial.status == Trial.PAUSED, trial.status
        trial.status = Trial.PENDING

    def resume_trial(self, trial):
        """Resumes PAUSED trials. This is a blocking call."""

        assert trial.status == Trial.PAUSED, trial.status
        self.start_trial(trial)

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

    def fetch_one_result(self):
        """Fetches one result from running trials.

        It's a blocking call waits until one result is ready.

        Return:
            A tuple of (trial, result). If fetch result failed,
            return (trial, None) other than raise Exception.
        """
        raise NotImplementedError("Subclasses of TrialExecutor must provide "
                                  "fetch_one_result() method")

    def debug_string(self):
        """Returns a human readable message for printing to the console."""
        pass

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
