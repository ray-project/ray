from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.tune.trial import Trial


class TrialScheduler(object):
    """Interface for implementing a Trial Scheduler class."""

    CONTINUE = "CONTINUE"  #: Status for continuing trial execution
    PAUSE = "PAUSE"  #: Status for pausing trial execution
    STOP = "STOP"  #: Status for stopping trial execution

    def on_trial_add(self, trial_runner, trial):
        """Called when a new trial is added to the trial runner."""

        raise NotImplementedError

    def on_trial_error(self, trial_runner, trial):
        """Notification for the error of trial.

        This will only be called when the trial is in the RUNNING state."""

        raise NotImplementedError

    def on_trial_result(self, trial_runner, trial, result):
        """Called on each intermediate result returned by a trial.

        At this point, the trial scheduler can make a decision by returning
        one of CONTINUE, PAUSE, and STOP. This will only be called when the
        trial is in the RUNNING state."""

        raise NotImplementedError

    def on_trial_complete(self, trial_runner, trial, result):
        """Notification for the completion of trial.

        This will only be called when the trial is in the RUNNING state and
        either completes naturally or by manual termination."""

        raise NotImplementedError

    def on_trial_remove(self, trial_runner, trial):
        """Called to remove trial.

        This is called when the trial is in PAUSED or PENDING state. Otherwise,
        call `on_trial_complete`."""

        raise NotImplementedError

    def choose_trial_to_run(self, trial_runner):
        """Called to choose a new trial to run.

        This should return one of the trials in trial_runner that is in
        the PENDING or PAUSED state. This function must be idempotent.

        If no trial is ready, return None."""

        raise NotImplementedError

    def debug_string(self):
        """Returns a human readable message for printing to the console."""

        raise NotImplementedError


class FIFOScheduler(TrialScheduler):
    """Simple scheduler that just runs trials in submission order."""

    def on_trial_add(self, trial_runner, trial):
        pass

    def on_trial_error(self, trial_runner, trial):
        pass

    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.CONTINUE

    def on_trial_complete(self, trial_runner, trial, result):
        pass

    def on_trial_remove(self, trial_runner, trial):
        pass

    def choose_trial_to_run(self, trial_runner):
        for trial in trial_runner.get_trials():
            if (trial.status == Trial.PENDING
                    and trial_runner.has_resources(trial.resources)):
                return trial
        for trial in trial_runner.get_trials():
            if (trial.status == Trial.PAUSED
                    and trial_runner.has_resources(trial.resources)):
                return trial
        return None

    def debug_string(self):
        return "Using FIFO scheduling algorithm."
