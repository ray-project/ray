from __future__ import absolute_import
from __future__ import division

import collections
import numpy as np

from ray.tune.trial import Trial


class TrialScheduler(object):
    CONTINUE = "CONTINUE"
    PAUSE = "PAUSE"
    STOP = "STOP"

    def on_trial_result(self, trial_runner, trial, result):
        """Called on each intermediate result returned by a trial.

        At this point, the trial scheduler can make a decision by returning
        one of CONTINUE, PAUSE, and STOP."""

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
    def on_trial_result(self, trial_runner, trial, result):
        return TrialScheduler.CONTINUE

    def choose_trial_to_run(self, trial_runner):
        for trial in trial_runner.get_trials():
            if (trial.status == Trial.PENDING and
                    trial_runner.has_resources(trial.resources)):
                return trial
        return None

    def debug_string(self):
        return "Using FIFO scheduling algorithm."


class MedianStoppingRule(TrialScheduler):
    def __init__(self):
        self._results = collections.defaultdict(list)
        self._num_decisions = 0
        self._num_pauses = 0

    def on_trial_result(self, trial_runner, trial, result):
        self._num_decisions += 1
        self._results[trial].append(result)
        if (self._get_median_result(result.time_total_s) >
                result.episode_reward_mean):
            self._num_pauses += 1
            return TrialScheduler.PAUSE
        else:
            return TrialScheduler.KEEP_RUNNING

    def choose_trial_to_run(self, trial_runner):
        for trial in trial_runner.get_trials():
            if (trial.status == Trial.PENDING and
                    trial_runner.has_resources(trial.resources)):
                return trial
        for trial in trial_runner.get_trials():
            if (trial.status == Trial.PAUSED and
                    trial_runner.has_resources(trial.resources)):
                return trial
        return None

    def debug_string(self):
        return "Using MedianStoppingRule: pauses={}, decisions={}.".format(
            self._num_pauses, self._num_decisions)

    def _get_median_result(self, time):
        best_results = []
        for trial, results in self._results.items():
            best_result = results[0]
            for r in results:
                if (r.episode_reward_mean > best_result.episode_reward_mean and
                        r.time_total_s <= time):
                    best_result = r
            best_results.append(best_result)
        return np.median([r.episode_reward_mean for r in best_results])
