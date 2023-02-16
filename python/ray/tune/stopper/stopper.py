import abc

from ray.util.annotations import PublicAPI


@PublicAPI
class Stopper(abc.ABC):
    """Base class for implementing a Tune experiment stopper.

    Allows users to implement experiment-level stopping via ``stop_all``. By
    default, this class does not stop any trials. Subclasses need to
    implement ``__call__`` and ``stop_all``.

    Examples:

        >>> import time
        >>> from ray import air, tune
        >>> from ray.air import session
        >>> from ray.tune import Stopper
        >>>
        >>> class TimeStopper(Stopper):
        ...     def __init__(self):
        ...         self._start = time.time()
        ...         self._deadline = 2  # Stop all trials after 2 seconds
        ...
        ...     def __call__(self, trial_id, result):
        ...         return False
        ...
        ...     def stop_all(self):
        ...         return time.time() - self._start > self._deadline
        ...
        >>> def train_fn(config):
        ...     for i in range(10):
        ...         time.sleep(1)
        ...         session.report({"iter": i})
        ...
        >>> tuner = tune.Tuner(
        ...     train_fn,
        ...     tune_config=tune.TuneConfig(num_samples=2),
        ...     run_config=air.RunConfig(stop=TimeStopper()),
        ... )
        >>> print("[ignore]"); result_grid = tuner.fit()  # doctest: +ELLIPSIS
        [ignore]...

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
        *stoppers: Stoppers to be combined.

    Examples:

        >>> import numpy as np
        >>> from ray import air, tune
        >>> from ray.air import session
        >>> from ray.tune.stopper import (
        ...     CombinedStopper,
        ...     MaximumIterationStopper,
        ...     TrialPlateauStopper,
        ... )
        >>>
        >>> stopper = CombinedStopper(
        ...     MaximumIterationStopper(max_iter=10),
        ...     TrialPlateauStopper(metric="my_metric"),
        ... )
        >>> def train_fn(config):
        ...     for i in range(15):
        ...         session.report({"my_metric": np.random.normal(0, 1 - i / 15)})
        ...
        >>> tuner = tune.Tuner(
        ...     train_fn,
        ...     run_config=air.RunConfig(stop=stopper),
        ... )
        >>> print("[ignore]"); result_grid = tuner.fit()  # doctest: +ELLIPSIS
        [ignore]...

    """

    def __init__(self, *stoppers: Stopper):
        self._stoppers = stoppers

    def __call__(self, trial_id, result):
        return any(s(trial_id, result) for s in self._stoppers)

    def stop_all(self):
        return any(s.stop_all() for s in self._stoppers)
