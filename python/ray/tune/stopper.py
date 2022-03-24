import datetime
from typing import Dict, Optional, Callable, Union
import time
from collections import defaultdict, deque
import numpy as np

from ray import logger
from ray.util.annotations import PublicAPI


@PublicAPI
class Stopper:
    """Base class for implementing a Tune experiment stopper.

    Allows users to implement experiment-level stopping via ``stop_all``. By
    default, this class does not stop any trials. Subclasses need to
    implement ``__call__`` and ``stop_all``.

    .. code-block:: python

        import time
        from ray import tune
        from ray.tune import Stopper

        class TimeStopper(Stopper):
            def __init__(self):
                self._start = time.time()
                self._deadline = 300

            def __call__(self, trial_id, result):
                return False

            def stop_all(self):
                return time.time() - self._start > self.deadline

        tune.run(Trainable, num_samples=200, stop=TimeStopper())

    """

    def __call__(self, trial_id, result):
        """Returns true if the trial should be terminated given the result."""
        raise NotImplementedError

    def stop_all(self):
        """Returns true if the experiment should be terminated."""
        raise NotImplementedError


@PublicAPI
class CombinedStopper(Stopper):
    """Combine several stoppers via 'OR'.

    Args:
        *stoppers (Stopper): Stoppers to be combined.

    Example:

    .. code-block:: python

        from ray.tune.stopper import CombinedStopper, \
            MaximumIterationStopper, TrialPlateauStopper

        stopper = CombinedStopper(
            MaximumIterationStopper(max_iter=20),
            TrialPlateauStopper(metric="my_metric")
        )

        tune.run(train, stop=stopper)

    """

    def __init__(self, *stoppers: Stopper):
        self._stoppers = stoppers

    def __call__(self, trial_id, result):
        return any(s(trial_id, result) for s in self._stoppers)

    def stop_all(self):
        return any(s.stop_all() for s in self._stoppers)


@PublicAPI
class NoopStopper(Stopper):
    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        return False


@PublicAPI
class FunctionStopper(Stopper):
    """Provide a custom function to check if trial should be stopped.

    The passed function will be called after each iteration. If it returns
    True, the trial will be stopped.

    Args:
        function: Function that checks if a trial
            should be stopped. Must accept the `trial_id` string  and `result`
            dictionary as arguments. Must return a boolean.

    """

    def __init__(self, function: Callable[[str, Dict], bool]):
        self._fn = function

    def __call__(self, trial_id, result):
        return self._fn(trial_id, result)

    def stop_all(self):
        return False

    @classmethod
    def is_valid_function(cls, fn):
        is_function = callable(fn) and not issubclass(type(fn), Stopper)
        if is_function and hasattr(fn, "stop_all"):
            raise ValueError(
                "Stop object must be ray.tune.Stopper subclass to be detected "
                "correctly."
            )
        return is_function


@PublicAPI
class MaximumIterationStopper(Stopper):
    """Stop trials after reaching a maximum number of iterations

    Args:
        max_iter: Number of iterations before stopping a trial.
    """

    def __init__(self, max_iter: int):
        self._max_iter = max_iter
        self._iter = defaultdict(lambda: 0)

    def __call__(self, trial_id: str, result: Dict):
        self._iter[trial_id] += 1
        return self._iter[trial_id] >= self._max_iter

    def stop_all(self):
        return False


@PublicAPI
class ExperimentPlateauStopper(Stopper):
    """Early stop the experiment when a metric plateaued across trials.

    Stops the entire experiment when the metric has plateaued
    for more than the given amount of iterations specified in
    the patience parameter.

    Args:
        metric: The metric to be monitored.
        std: The minimal standard deviation after which
            the tuning process has to stop.
        top: The number of best models to consider.
        mode: The mode to select the top results.
            Can either be "min" or "max".
        patience: Number of epochs to wait for
            a change in the top models.

    Raises:
        ValueError: If the mode parameter is not "min" nor "max".
        ValueError: If the top parameter is not an integer
            greater than 1.
        ValueError: If the standard deviation parameter is not
            a strictly positive float.
        ValueError: If the patience parameter is not
            a strictly positive integer.
    """

    def __init__(
        self,
        metric: str,
        std: float = 0.001,
        top: int = 10,
        mode: str = "min",
        patience: int = 0,
    ):
        if mode not in ("min", "max"):
            raise ValueError("The mode parameter can only be either min or max.")
        if not isinstance(top, int) or top <= 1:
            raise ValueError(
                "Top results to consider must be"
                " a positive integer greater than one."
            )
        if not isinstance(patience, int) or patience < 0:
            raise ValueError("Patience must be a strictly positive integer.")
        if not isinstance(std, float) or std <= 0:
            raise ValueError(
                "The standard deviation must be a strictly positive float number."
            )
        self._mode = mode
        self._metric = metric
        self._patience = patience
        self._iterations = 0
        self._std = std
        self._top = top
        self._top_values = []

    def __call__(self, trial_id, result):
        """Return a boolean representing if the tuning has to stop."""
        self._top_values.append(result[self._metric])
        if self._mode == "min":
            self._top_values = sorted(self._top_values)[: self._top]
        else:
            self._top_values = sorted(self._top_values)[-self._top :]

        # If the current iteration has to stop
        if self.has_plateaued():
            # we increment the total counter of iterations
            self._iterations += 1
        else:
            # otherwise we reset the counter
            self._iterations = 0

        # and then call the method that re-executes
        # the checks, including the iterations.
        return self.stop_all()

    def has_plateaued(self):
        return (
            len(self._top_values) == self._top and np.std(self._top_values) <= self._std
        )

    def stop_all(self):
        """Return whether to stop and prevent trials from starting."""
        return self.has_plateaued() and self._iterations >= self._patience


@PublicAPI
class TrialPlateauStopper(Stopper):
    """Early stop single trials when they reached a plateau.

    When the standard deviation of the `metric` result of a trial is
    below a threshold `std`, the trial plateaued and will be stopped
    early.

    Args:
        metric: Metric to check for convergence.
        std: Maximum metric standard deviation to decide if a
            trial plateaued. Defaults to 0.01.
        num_results: Number of results to consider for stdev
            calculation.
        grace_period: Minimum number of timesteps before a trial
            can be early stopped
        metric_threshold (Optional[float]):
            Minimum or maximum value the result has to exceed before it can
            be stopped early.
        mode: If a `metric_threshold` argument has been
            passed, this must be one of [min, max]. Specifies if we optimize
            for a large metric (max) or a small metric (min). If max, the
            `metric_threshold` has to be exceeded, if min the value has to
            be lower than `metric_threshold` in order to early stop.
    """

    def __init__(
        self,
        metric: str,
        std: float = 0.01,
        num_results: int = 4,
        grace_period: int = 4,
        metric_threshold: Optional[float] = None,
        mode: Optional[str] = None,
    ):
        self._metric = metric
        self._mode = mode

        self._std = std
        self._num_results = num_results
        self._grace_period = grace_period
        self._metric_threshold = metric_threshold

        if self._metric_threshold:
            if mode not in ["min", "max"]:
                raise ValueError(
                    f"When specifying a `metric_threshold`, the `mode` "
                    f"argument has to be one of [min, max]. "
                    f"Got: {mode}"
                )

        self._iter = defaultdict(lambda: 0)
        self._trial_results = defaultdict(lambda: deque(maxlen=self._num_results))

    def __call__(self, trial_id: str, result: Dict):
        metric_result = result.get(self._metric)
        self._trial_results[trial_id].append(metric_result)
        self._iter[trial_id] += 1

        # If still in grace period, do not stop yet
        if self._iter[trial_id] < self._grace_period:
            return False

        # If not enough results yet, do not stop yet
        if len(self._trial_results[trial_id]) < self._num_results:
            return False

        # If metric threshold value not reached, do not stop yet
        if self._metric_threshold is not None:
            if self._mode == "min" and metric_result > self._metric_threshold:
                return False
            elif self._mode == "max" and metric_result < self._metric_threshold:
                return False

        # Calculate stdev of last `num_results` results
        try:
            current_std = np.std(self._trial_results[trial_id])
        except Exception:
            current_std = float("inf")

        # If stdev is lower than threshold, stop early.
        return current_std < self._std

    def stop_all(self):
        return False


@PublicAPI
class TimeoutStopper(Stopper):
    """Stops all trials after a certain timeout.

    This stopper is automatically created when the `time_budget_s`
    argument is passed to `tune.run()`.

    Args:
        timeout: Either a number specifying the timeout in seconds, or
            a `datetime.timedelta` object.
    """

    def __init__(self, timeout: Union[int, float, datetime.timedelta]):
        from datetime import timedelta

        if isinstance(timeout, timedelta):
            self._timeout_seconds = timeout.total_seconds()
        elif isinstance(timeout, (int, float)):
            self._timeout_seconds = timeout
        else:
            raise ValueError(
                "`timeout` parameter has to be either a number or a "
                "`datetime.timedelta` object. Found: {}".format(type(timeout))
            )

        # To account for setup overhead, set the start time only after
        # the first call to `stop_all()`.
        self._start = None

    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        if not self._start:
            self._start = time.time()
            return False

        now = time.time()
        if now - self._start >= self._timeout_seconds:
            logger.info(
                f"Reached timeout of {self._timeout_seconds} seconds. "
                f"Stopping all trials."
            )
            return True
        return False
