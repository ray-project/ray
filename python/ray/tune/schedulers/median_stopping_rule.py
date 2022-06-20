import collections
import logging
from typing import Dict, List, Optional

import numpy as np

from ray.tune import trial_runner
from ray.tune.result import DEFAULT_METRIC
from ray.tune.trial import Trial
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI
class MedianStoppingRule(FIFOScheduler):
    """Implements the median stopping rule as described in the Vizier paper:

    https://research.google.com/pubs/pub46180.html

    Args:
        time_attr: The training result attr to use for comparing time.
            Note that you can pass in something non-temporal such as
            `training_iteration` as a measure of progress, the only requirement
            is that the attribute should increase monotonically.
        metric: The training result objective value attribute. Stopping
            procedures will use this attribute. If None but a mode was passed,
            the `ray.tune.result.DEFAULT_METRIC` will be used per default.
        mode: One of {min, max}. Determines whether objective is
            minimizing or maximizing the metric attribute.
        grace_period: Only stop trials at least this old in time.
            The mean will only be computed from this time onwards. The units
            are the same as the attribute named by `time_attr`.
        min_samples_required: Minimum number of trials to compute median
            over.
        min_time_slice: Each trial runs at least this long before
            yielding (assuming it isn't stopped). Note: trials ONLY yield if
            there are not enough samples to evaluate performance for the
            current result AND there are other trials waiting to run.
            The units are the same as the attribute named by `time_attr`.
        hard_stop: If False, pauses trials instead of stopping
            them. When all other trials are complete, paused trials will be
            resumed and allowed to run FIFO.
    """

    def __init__(
        self,
        time_attr: str = "time_total_s",
        metric: Optional[str] = None,
        mode: Optional[str] = None,
        grace_period: float = 60.0,
        min_samples_required: int = 3,
        min_time_slice: int = 0,
        hard_stop: bool = True,
    ):
        FIFOScheduler.__init__(self)
        self._stopped_trials = set()
        self._grace_period = grace_period
        self._min_samples_required = min_samples_required
        self._min_time_slice = min_time_slice
        self._metric = metric
        self._worst = None
        self._compare_op = None

        self._mode = mode
        if mode:
            assert mode in ["min", "max"], "`mode` must be 'min' or 'max'."
            self._worst = float("-inf") if self._mode == "max" else float("inf")
            self._compare_op = max if self._mode == "max" else min

        self._time_attr = time_attr
        self._hard_stop = hard_stop
        self._trial_state = {}
        self._last_pause = collections.defaultdict(lambda: float("-inf"))
        self._results = collections.defaultdict(list)

    def set_search_properties(
        self, metric: Optional[str], mode: Optional[str], **spec
    ) -> bool:
        if self._metric and metric:
            return False
        if self._mode and mode:
            return False

        if metric:
            self._metric = metric
        if mode:
            self._mode = mode

        self._worst = float("-inf") if self._mode == "max" else float("inf")
        self._compare_op = max if self._mode == "max" else min

        if self._metric is None and self._mode:
            # If only a mode was passed, use anonymous metric
            self._metric = DEFAULT_METRIC

        return True

    def on_trial_add(self, trial_runner: "trial_runner.TrialRunner", trial: Trial):
        if not self._metric or not self._worst or not self._compare_op:
            raise ValueError(
                "{} has been instantiated without a valid `metric` ({}) or "
                "`mode` ({}) parameter. Either pass these parameters when "
                "instantiating the scheduler, or pass them as parameters "
                "to `tune.run()`".format(
                    self.__class__.__name__, self._metric, self._mode
                )
            )

        super(MedianStoppingRule, self).on_trial_add(trial_runner, trial)

    def on_trial_result(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, result: Dict
    ) -> str:
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
                    len(trials), trial.trial_id, time, action_str
                )
            )
            return action

        median_result = self._median_result(trials, time)
        best_result = self._best_result(trial)
        logger.debug(
            "Trial {} best res={} vs median res={} at t={}".format(
                trial, best_result, median_result, time
            )
        )

        if self._compare_op(median_result, best_result) != best_result:
            logger.debug("MedianStoppingRule: early stopping {}".format(trial))
            self._stopped_trials.add(trial)
            if self._hard_stop:
                return TrialScheduler.STOP
            else:
                return TrialScheduler.PAUSE
        else:
            return TrialScheduler.CONTINUE

    def on_trial_complete(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, result: Dict
    ):
        self._results[trial].append(result)

    def debug_string(self) -> str:
        return "Using MedianStoppingRule: num_stopped={}.".format(
            len(self._stopped_trials)
        )

    def _on_insufficient_samples(
        self, trial_runner: "trial_runner.TrialRunner", trial: Trial, time: float
    ) -> str:
        pause = time - self._last_pause[trial] > self._min_time_slice
        pause = pause and [
            t
            for t in trial_runner.get_live_trials()
            if t.status in (Trial.PENDING, Trial.PAUSED)
        ]
        return TrialScheduler.PAUSE if pause else TrialScheduler.CONTINUE

    def _trials_beyond_time(self, time: float) -> List[Trial]:
        trials = [
            trial
            for trial in self._results
            if self._results[trial][-1][self._time_attr] >= time
        ]
        return trials

    def _median_result(self, trials: List[Trial], time: float):
        return np.median([self._running_mean(trial, time) for trial in trials])

    def _running_mean(self, trial: Trial, time: float) -> np.ndarray:
        results = self._results[trial]
        # TODO(ekl) we could do interpolation to be more precise, but for now
        # assume len(results) is large and the time diffs are roughly equal
        scoped_results = [
            r for r in results if self._grace_period <= r[self._time_attr] <= time
        ]
        return np.mean([r[self._metric] for r in scoped_results])

    def _best_result(self, trial):
        results = self._results[trial]
        return self._compare_op([r[self._metric] for r in results])
