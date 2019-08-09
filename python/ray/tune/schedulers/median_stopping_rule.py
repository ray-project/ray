from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import logging
import numpy as np

from ray.tune.trial import Trial
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler

logger = logging.getLogger(__name__)


class MedianTrialState(object):
    def __init__(self, trial):
        self.orig_tag = trial.experiment_tag
        self.last_eval_time = 0

    def __repr__(self):
        return str((self.last_eval_time, ))


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
        eval_interval (float): Compare trial median metric at this interval of time.
            The units are the same as the attribute named by `time_attr`.
        min_samples_required (int): Min samples to compute median over.
        hard_stop (bool): If False, pauses trials instead of stopping
            them. When all other trials are complete, paused trials will be
            resumed and allowed to run FIFO.
        verbose (bool): If True, will output the median and best result each
            time a trial reports. Defaults to True.
        tail_length (float): Median is constructed from means of trial metric,
            the mean is constructed from the last tail_length results. None
            implies use all results.
            The units are the same as the attribute named by `time_attr`.
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 grace_period=60.0,
                 eval_interval=600.0,
                 min_samples_required=3,
                 hard_stop=True,
                 verbose=True,
                 tail_length=None):
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
        self._completed_trials = set()
        self._grace_period = grace_period
        self._eval_interval = eval_interval
        self._min_samples_required = min_samples_required
        self._metric = metric
        if mode == "max":
            self._metric_op = 1.
        elif mode == "min":
            self._metric_op = -1.
        self._time_attr = time_attr
        self._hard_stop = hard_stop
        self._verbose = verbose
        self._tail_length = tail_length
        self._trial_state = {}
        self._results = collections.defaultdict(list)

    @property
    def _trials_beyond_grace_period(self):
        trials = [
            trial for trial in self._results if (trial.last_result.get(
                self._time_attr, -float('inf')) > self._grace_period)
        ]
        return trials

    def on_trial_add(self, trial_runner, trial):
        self._trial_state[trial] = MedianTrialState(trial)

    def on_trial_result(self, trial_runner, trial, result):
        """Callback for early stopping.
        This stopping rule stops a running trial if the trial's best objective
        value by step `t` is strictly worse than the median of the running tail_length
        averages of all running trials' objectives reported up to step `t`.
        """
        if trial in self._stopped_trials:
            assert not self._hard_stop
            return TrialScheduler.CONTINUE  # fall back to FIFO
        state = self._trial_state[trial]
        time = result[self._time_attr]
        self._results[trial].append(result)
        if time - state.last_eval_time < self._eval_interval:
            return TrialScheduler.CONTINUE  # avoid overhead
        state.last_eval_time = time
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
            for _trial in trial_runner.get_trials():
                if _trial.status in [Trial.PENDING, Trial.PAUSED]:
                    return TrialScheduler.PAUSE  # yield time to other trials
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
        for trial in self._trials_beyond_grace_period:
            scores.append(self._running_result(trial, time))
        if len(scores) >= self._min_samples_required:
            return np.median(scores)
        else:
            return float("-inf")

    def _running_result(self, trial, t_max=float("inf")):
        results = self._results[trial]
        if self._tail_length is not None:
            results = results[-self._tail_length:]
        return self._metric_op * np.mean(
            [r[self._metric] for r in results \
                if r[self._time_attr] <= t_max])

    def _best_result(self, trial):
        results = self._results[trial]
        if self._tail_length is not None:
            results = results[-self._tail_length:]
        return max(
            [self._metric_op * r[self._metric] for r in results])

    def choose_trial_to_run(self, trial_runner):
        """Ensures all trials get fair share of time (as defined by time_attr).
        This enables the scheduler to support a greater number of
        concurrent trials than can fit in the cluster at any given time.
        """
        candidates = []
        for trial in trial_runner.get_trials():
            if trial.status in [Trial.PENDING, Trial.PAUSED] and \
                    trial_runner.has_resources(trial.resources):
                candidates.append(trial)
        candidates.sort(
            key=lambda trial: self._trial_state[trial].last_eval_time)
        return candidates[0] if candidates else None
