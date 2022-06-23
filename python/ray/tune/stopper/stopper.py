import abc

from ray.util.annotations import PublicAPI


@PublicAPI
class Stopper(abc.ABC):
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
        *stoppers: Stoppers to be combined.

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
