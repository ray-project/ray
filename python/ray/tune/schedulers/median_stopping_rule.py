from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging
import numpy as np

from ray.tune.trial import Trial
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler

logger = logging.getLogger(__name__)


class MedianStoppingRule(FIFOScheduler):
    """Implements the median stopping rule as described in the Vizier paper:

    https://research.google.com/pubs/pub46180.html

    Args:
        time_attr (str): The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        reward_attr (str): The training result objective value attribute. As
            with `time_attr`, this may refer to any objective value that
            is supposed to increase with time.
        grace_period (float): Only stop trials at least this old in time.
            The units are the same as the attribute named by `time_attr`.
        min_samples_required (int): Min samples to compute median over.
        hard_stop (bool): If False, pauses trials instead of stopping
            them. When all other trials are complete, paused trials will be
            resumed and allowed to run FIFO.
        verbose (bool): If True, will output the median and best result each
            time a trial reports. Defaults to True.
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr="episode_reward_mean",
                 grace_period=60.0,
                 min_samples_required=3,
                 hard_stop=True,
                 verbose=True):
        FIFOScheduler.__init__(self)
        self._stopped_trials = set()
        self._completed_trials = set()
        self._results = collections.defaultdict(list)
        self._grace_period = grace_period
        self._min_samples_required = min_samples_required
        self._reward_attr = reward_attr
        self._time_attr = time_attr
        self._hard_stop = hard_stop
        self._verbose = verbose

    def on_trial_result(self, trial_runner, trial, result):
        """Callback for early stopping.

        This stopping rule stops a running trial if the trial's best objective
        value by step `t` is strictly worse than the median of the running
        averages of all completed trials' objectives reported up to step `t`.
        """

        if trial in self._stopped_trials:
            assert not self._hard_stop
            return TrialScheduler.CONTINUE  # fall back to FIFO

        time = result[self._time_attr]
        self._results[trial].append(result)
        median_result = self._get_median_result(time)
        best_result = self._best_result(trial)
        if self._verbose:
            logger.info("Trial {} best res={} vs median res={} at t={}".format(
                trial, best_result, median_result, time))
        if best_result < median_result and time > self._grace_period:
            if self._verbose:
                logger.info("MedianStoppingRule: "
                            "early stopping {}".format(trial))
            self._stopped_trials.add(trial)
            if self._hard_stop:
                return TrialScheduler.STOP
            else:
                return TrialScheduler.PAUSE
        else:
            return TrialScheduler.CONTINUE

    def on_trial_complete(self, trial_runner, trial, result):
        self._results[trial].append(result)
        self._completed_trials.add(trial)

    def on_trial_remove(self, trial_runner, trial):
        """Marks trial as completed if it is paused and has previously ran."""
        if trial.status is Trial.PAUSED and trial in self._results:
            self._completed_trials.add(trial)

    def debug_string(self):
        return "Using MedianStoppingRule: num_stopped={}.".format(
            len(self._stopped_trials))

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
        return np.mean([
            r[self._reward_attr] for r in results
            if r[self._time_attr] <= t_max
        ])

    def _best_result(self, trial):
        results = self._results[trial]
        return max(r[self._reward_attr] for r in results)
