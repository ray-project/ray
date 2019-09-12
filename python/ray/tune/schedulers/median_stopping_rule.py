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
        metric (str): The training result objective value attribute. Stopping
            procedures will use this attribute.
        mode (str): One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        grace_period (float): Only stop trials at least this old in time.
            The units are the same as the attribute named by `time_attr`.
        eval_interval (float): Compare median metric at this time interval.
            The units are the same as the attribute named by `time_attr`.
        min_samples_required (int): Min number of trials to compute median over.
        hard_stop (bool): If False, pauses trials instead of stopping
            them. When all other trials are complete, paused trials will be
            resumed and allowed to run FIFO.
        verbose (bool): If True, will output the median and best result each
            time a trial reports. Defaults to True.
        running_window_size (float): Median is constructed from means of trial metric,
            the mean is constructed from the last running_window_size results. None
            implies use all results.
            The units are the same as the attribute named by `time_attr`.
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 grace_period=60.0,
                 eval_interval=0.0,
                 min_samples_required=3,
                 hard_stop=True,
                 verbose=True,
                 running_window_size=None):
        assert mode in ["min", "max"], "`mode` must be 'min' or 'max'!"
        if reward_attr is not None:
            mode = "max"
            metric = reward_attr
            logger.warning(
                "`reward_attr` is deprecated and will be removed in a future "
                "version of Tune. "
                "Setting `metric={}` and `mode=max`.".format(reward_attr))
        FIFOScheduler.__init__(self)
        self._stopped_trials = set()
        self._grace_period = grace_period
        self._eval_interval = eval_interval
        self._min_samples_required = min_samples_required
        self._last_eval_times = collections.defaultdict(int)
        self._metric = metric
        assert mode in {"min", "max"}, "`mode` must be 'min' or 'max'."
        self._mode = mode
        self._time_attr = time_attr
        self._hard_stop = hard_stop
        self._verbose = verbose
        self._running_window_size = running_window_size
        self._trial_state = {}
        self._results = collections.defaultdict(list)

    @property
    def _trials_beyond_grace_period(self):
        trials = [
            trial for trial, result_log in self._results.items() if (
                result_log[-1].get(self._time_attr, float("-inf")) > self._grace_period)
        ]
        return trials

    @property
    def worst(self):
        return float("-inf") if self._mode == "max" else float("inf")

    def on_trial_result(self, trial_runner, trial, result):
        """Callback for early stopping.

        This stopping rule stops a running trial if the trial's best objective
        value by step `t` is strictly worse than the median of the running
        averages of all completed trials' objectives reported up to step `t`.
        """
        if trial in self._stopped_trials:
            assert not self._hard_stop
            return TrialScheduler.CONTINUE  # fall back to FIFO

        result_time = result[self._time_attr]
        self._results[trial].append(result)
        if result_time - self._last_eval_times[trial] < self._eval_interval:
            return TrialScheduler.CONTINUE  # avoid overhead
        self._last_eval_times[trial] = result_time
        median_result = self._get_median_result(t_max=result_time)
        best_result = self._best_result(trial)
        if self._verbose:
            logger.info("Trial {} best res={} vs median res={} at t={}".format(
                trial, best_result, median_result, result_time))

        if self._mode == "max":
            comparison = best_result < median_result
        else:
            comparison = best_result > median_result
        if comparison and result_time > self._grace_period:
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

    def debug_string(self):
        return "Using MedianStoppingRule: num_stopped={}.".format(
            len(self._stopped_trials))

    def _get_median_result(self, t_max=float("inf")):
        scores = [
            self._running_result(trial, t_max)
                for trial in self._trials_beyond_grace_period
        ]
        if len(scores) >= self._min_samples_required:
            return np.median(scores)
        else:
            return self.worst

    def _running_result(self, trial, t_max=float("inf")):
        results = self._results[trial]
        scoped_results = [r for r in results if r[self._time_attr] <= t_max]
        if self._running_window_size is not None:
            scoped_results = scoped_results[-self._running_window_size:]
        return np.mean([r[self._metric] for r in scoped_results])

    def _best_result(self, trial):
        results = self._results[trial]
        compare_op = max if self._mode == "max" else min
        return compare_op([r[self._metric] for r in results])
