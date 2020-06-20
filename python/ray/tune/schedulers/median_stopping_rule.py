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
            The mean will only be computed from this time onwards. The units
            are the same as the attribute named by `time_attr`.
        min_samples_required (int): Minimum number of trials to compute median
            over.
        min_time_slice (float): Each trial runs at least this long before
            yielding (assuming it isn't stopped). Note: trials ONLY yield if
            there are not enough samples to evaluate performance for the
            current result AND there are other trials waiting to run.
            The units are the same as the attribute named by `time_attr`.
        hard_stop (bool): If False, pauses trials instead of stopping
            them. When all other trials are complete, paused trials will be
            resumed and allowed to run FIFO.
    """

    def __init__(self,
                 time_attr="time_total_s",
                 reward_attr=None,
                 metric="episode_reward_mean",
                 mode="max",
                 grace_period=60.0,
                 min_samples_required=3,
                 min_time_slice=0,
                 hard_stop=True):
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
        self._min_time_slice = min_time_slice
        self._metric = metric
        assert mode in {"min", "max"}, "`mode` must be 'min' or 'max'."
        self._worst = float("-inf") if mode == "max" else float("inf")
        self._compare_op = max if mode == "max" else min
        self._time_attr = time_attr
        self._hard_stop = hard_stop
        self._trial_state = {}
        self._last_pause = collections.defaultdict(lambda: float("-inf"))
        self._results = collections.defaultdict(list)

    def on_trial_result(self, trial_runner, trial, result):
        """Callback for early stopping.

        This stopping rule stops a running trial if the trial's best objective
        value by step `t` is strictly worse than the median of the running
        averages of all completed trials' objectives reported up to step `t`.
        """
        if self._time_attr not in result or self._metric not in result:
            return TrialScheduler.CONTINUE

        if trial in self._stopped_trials:
            assert not self._hard_stop
            # Fall back to FIFO
            return TrialScheduler.CONTINUE

        time = result[self._time_attr]
        self._results[trial].append(result)

        if time < self._grace_period:
            return TrialScheduler.CONTINUE

        trials = self._trials_beyond_time(time)
        trials.remove(trial)

        if len(trials) < self._min_samples_required:
            action = self._on_insufficient_samples(trial_runner, trial, time)
            if action == TrialScheduler.PAUSE:
                self._last_pause[trial] = time
                action_str = "Yielding time to other trials."
            else:
                action_str = "Continuing anyways."
            logger.debug(
                "MedianStoppingRule: insufficient samples={} to evaluate "
                "trial {} at t={}. {}".format(
                    len(trials), trial.trial_id, time, action_str))
            return action

        median_result = self._median_result(trials, time)
        best_result = self._best_result(trial)
        logger.debug("Trial {} best res={} vs median res={} at t={}".format(
            trial, best_result, median_result, time))

        if self._compare_op(median_result, best_result) != best_result:
            logger.debug("MedianStoppingRule: early stopping {}".format(trial))
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

    def _on_insufficient_samples(self, trial_runner, trial, time):
        pause = time - self._last_pause[trial] > self._min_time_slice
        pause = pause and [
            t for t in trial_runner.get_trials()
            if t.status in (Trial.PENDING, Trial.PAUSED)
        ]
        return TrialScheduler.PAUSE if pause else TrialScheduler.CONTINUE

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
        # TODO(ekl) we could do interpolation to be more precise, but for now
        # assume len(results) is large and the time diffs are roughly equal
        scoped_results = [
            r for r in results
            if self._grace_period <= r[self._time_attr] <= time
        ]
        return np.mean([r[self._metric] for r in scoped_results])

    def _best_result(self, trial):
        results = self._results[trial]
        return self._compare_op([r[self._metric] for r in results])
