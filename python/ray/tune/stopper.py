import numpy as np


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


class NoopStopper(Stopper):
    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        return False


class FunctionStopper(Stopper):
    def __init__(self, function):
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
                "correctly.")
        return is_function


class EarlyStopping(Stopper):
    def __init__(self, metric: str, std: float = 0.001, top: int = 10):
        """Create the EarlyStopping object.

        Parameters
        ---------------------
        metric: str,
            The metric to be monitored.
        std: float = 0.001,
            The minimal standard deviation after which
            the tuning process has to stop.
        top: int = 10,
            The number of best model to consider.
        """
        self._metric = metric
        self._std = std
        self._top = top
        self._top_values = []

    def __call__(self, trial_id, result):
        """Return a boolean representing if the tuning has to stop."""
        self._top_values.append(result[self._metric])
        self._top_values = sorted(self._top_values)[:self._top]
        return self.stop_all()

    def stop_all(self):
        """Return whether to stop and prevent trials from starting."""
        return (len(self._top_values) == self._top and
                np.std(self._top_values) <= self._std)
