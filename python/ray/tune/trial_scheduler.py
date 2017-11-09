from __future__ import absolute_import
from __future__ import division

import collections
import numpy as np

from ray.tune.trial import Trial


class TrialScheduler(object):
    CONTINUE = "CONTINUE"
    PAUSE = "PAUSE"
    STOP = "STOP"

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
        one of CONTINUE, PAUSE, and STOP."""

        raise NotImplementedError

    def on_trial_complete(self, trial_runner, trial, result):
        """Notification for the completion of trial.

        This will only be called when the trial completes naturally."""

        raise NotImplementedError

    def choose_trial_to_run(self, trial_runner, trials):
        """Called to choose a new trial to run.

        This should return one of the trials in trial_runner that is in
        the PENDING or PAUSED state."""

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

    def choose_trial_to_run(self, trial_runner):
        for trial in trial_runner.get_trials():
            if (trial.status == Trial.PENDING and
                    trial_runner.has_resources(trial.resources)):
                return trial
        return None

    def debug_string(self):
        return "Using FIFO scheduling algorithm."


# TODO(ekl) expose this in the command line API
class MedianStoppingRule(FIFOScheduler):
    """Implements the median stopping rule as described in the Vizier paper:

        https://research.google.com/pubs/pub46180.html

    Args:
        time_attr (str): The TrainingResult attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The TrainingResult objective value attribute. As
            with `time_attr`, this may refer to any objective value that
            is supposed to increase with time.
        grace_period (float): Only stop trials at least this old in time.
            The units are the same as the attribute named by `time_attr`.
        min_samples_required (int): Min samples to compute median over.
    """

    def __init__(
            self, time_attr='time_total_s', reward_attr='episode_reward_mean',
            grace_period=60.0, min_samples_required=3):
        FIFOScheduler.__init__(self)
        self._completed_trials = set()
        self._results = collections.defaultdict(list)
        self._grace_period = grace_period
        self._min_samples_required = min_samples_required
        self._reward_attr = reward_attr
        self._time_attr = time_attr
        self._num_stopped = 0

    def on_trial_result(self, trial_runner, trial, result):
        """Callback for early stopping.

        This stopping rule stops a running trial if the trial's best objective
        value by step `t` is strictly worse than the median of the running
        averages of all completed trials' objectives reported up to step `t`.
        """

        time = getattr(result, self._time_attr)
        self._results[trial].append(result)
        median_result = self._get_median_result(time)
        best_result = self._best_result(trial)
        print("Trial {} best res={} vs median res={} at t={}".format(
            trial, best_result, median_result, time))
        if best_result < median_result and time > self._grace_period:
            print("MedianStoppingRule: early stopping {}".format(trial))
            self._num_stopped += 1
            return TrialScheduler.STOP
        else:
            return TrialScheduler.CONTINUE

    def on_trial_complete(self, trial_runner, trial, result):
        self._results[trial].append(result)
        self._completed_trials.add(trial)

    def debug_string(self):
        return "Using MedianStoppingRule: num_stopped={}.".format(
            self._num_stopped)

    def _get_median_result(self, time):
        scores = []
        for trial in self._completed_trials:
            scores.append(self._running_result(trial, time))
        if len(scores) >= self._min_samples_required:
            return np.median(scores)
        else:
            return float('-inf')

    def _running_result(self, trial, t_max=float('inf')):
        results = self._results[trial]
        # TODO(ekl) we could do interpolation to be more precise, but for now
        # assume len(results) is large and the time diffs are roughly equal
        return np.mean(
            [getattr(r, self._reward_attr)
                for r in results if getattr(r, self._time_attr) <= t_max])

    def _best_result(self, trial):
        results = self._results[trial]
        return max([getattr(r, self._reward_attr) for r in results])
