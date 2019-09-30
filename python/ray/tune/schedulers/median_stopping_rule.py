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
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 grace_period=60.0,
                 min_samples_required=3,
                 hard_stop=True,
                 verbose=True):
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
        self._min_samples_required = min_samples_required
        self._metric = metric
        assert mode in {"min", "max"}, "`mode` must be 'min' or 'max'."
        self._worst = float("-inf") if mode == "max" else float("inf")
        self._compare_op = max if mode == "max" else min
        self._time_attr = time_attr
        self._hard_stop = hard_stop
        self._verbose = verbose
        self._trial_state = {}
        self._results = collections.defaultdict(list)

    def on_trial_result(self, trial_runner, trial, result):
        """Callback for early stopping.

        This stopping rule stops a running trial if the trial's best objective
        value by step `t` is strictly worse than the median of the running
        averages of all completed trials' objectives reported up to step `t`.
        """
        if trial in self._stopped_trials:
            assert not self._hard_stop
            # Fall back to FIFO
            return TrialScheduler.CONTINUE

        result_time = result[self._time_attr]
        self._results[trial].append(result)

        if result_time < self._grace_period:
            return TrialScheduler.CONTINUE

        trials = self._trials_beyond_time(result_time)
        trials.remove(trial)

        if len(trials) < self._min_samples_required:
            waiting = [
                t for t in trial_runner.get_trials()
                if t.status in (Trial.PENDING, Trial.PAUSED)
            ]
            if self._verbose and waiting:
                logger.info(
                    "MedianStoppingRule: insufficient samples={} to evaluate "
                    "trial {} at t={}. Yielding time to other trials.".format(
                        len(trials), trial.trial_id, result_time))
            return TrialScheduler.PAUSE if waiting else TrialScheduler.CONTINUE

        median_result = self._median_result(trials, result_time)
        best_result = self._best_result(trial)
        if self._verbose:
            logger.info("Trial {} best res={} vs median res={} at t={}".format(
                trial, best_result, median_result, result_time))

        if self._compare_op(median_result, best_result) != best_result:
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

    def _trials_beyond_time(self, time):
        trials = [
            trial for trial in self._results
            if self._results[trial][-1][self._time_attr] >= time
        ]
        return trials

    def _median_result(self, trials, time):
        return np.median([self._running_mean(trial, time) for trial in trials])

    def _running_mean(self, trial, time):
        results = self._results[trial]
        scoped_results = [r for r in results if r[self._time_attr] <= time]
        return np.mean([r[self._metric] for r in scoped_results])

    def _best_result(self, trial):
        results = self._results[trial]
        return self._compare_op([r[self._metric] for r in results])
