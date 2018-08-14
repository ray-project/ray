# coding: utf-8
import traceback

from ray.tune.trial import Trial


class TrialExecutor(object):
    def __init__(self, queue_trials=False):
        """
        Initializes a new TrailExecutor.
        Args:
            queue_trials (bool): Whether to queue trials when the cluster does
                not currently have enough resources to launch one. This should
                be set to True when running on an autoscaling cluster to enable
                automatic scale-up.
        """
        self._queue_trials = queue_trials

    def has_resources(self, resources):
        """Returns whether this runner has at least the specified resources."""
        raise NotImplementedError('subclasses of TrialExecutor must provide ant has_resources() method')

    def start_trial(self, trial, checkpoint_obj=None):
        """ Starts the trial.

        If an error is encountered when starting the trial, an exception will
        be thrown.

        """
        raise NotImplementedError('subclasses of TrialExecutor must provide ant start_trial() method')

    def stop_trial(self, trial, error=False, error_msg=None, stop_logger=True):
        """ Stops the trial.

        Stops this trial, releasing all allocating resources.
        If stopping the trial fails, the run will be marked as terminated in error, but no
        exception will be thrown.

        Args:
            error (bool): Whether to mark this trial as terminated in error.
            error_msg (str): Optional error message.
            stop_logger (bool): Whether to shut down the trial logger.
        """
        raise NotImplementedError('subclasses of TrialExecutor must provide ant stop_trial() method')

    def restart_trial(self, trial, error_msg=None):
        """ Restarts the trial.

        The state of the trial should restore from the last checkpoint.

        Args:
            error_msg (str): Optional error message.
        """
        try:
            print("Attempting to recover trial state from last checkpoint")
            self.stop_trial(trial, error=True, error_msg=error_msg, stop_logger=False)
            trial.result_logger.flush()
            self.start_trial(trial)
        except Exception:
            error_msg = traceback.format_exc()
            print("Error recovering trial from checkpoint, abort:", error_msg)
            self.stop_trial(trial, error=True, error_msg=error_msg)

    def continue_trial(self, trial):
        """ mark that this trial should continue to run. """
        pass

    def pause_trial(self, trial):
        """Pauses the trial.

        We want to release resources (specifically GPUs) when pausing an
        experiment. This results in PAUSED state that similar to TERMINATED.
        """
        assert trial.status == Trial.RUNNING, trial.status
        try:
            trial.checkpoint(to_object_store=True)  # save the state of trial.
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
        """Resume PAUSED trials. This is a blocking call."""
        assert trial.status == Trial.PAUSED, trial.status
        self.start_trial(trial)

    def get_running_trials(self):
        """Gets running trials. """
        raise NotImplementedError('subclasses of TrialExecutor must provide ant get_running_trials() method')

    def on_step_begin(self):
        """
        a hook before running one step of the trial event loop (Optional).
        """
        pass

    def on_step_end(self):
        """
        a hook after running one step of the trial event loop (Optional).
        """
        pass

    def fetch_one_result(self):
        """fetch one result from running trials.

        :return: a tuple of trial and its result.
        """
        raise NotImplementedError('subclasses of TrialExecutor must provide ant fetch_one_result() method')

    def debug_string(self):
        pass
