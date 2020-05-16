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
    def __init__(self, metric, std=0.001, top=10, mode="min"):
        """Create the EarlyStopping object.

        Args:
            metric (str): The metric to be monitored.
            std (float): The minimal standard deviation after which
                the tuning process has to stop.
            top (int): The number of best model to consider.
            mode (str): The mode to select the top results.
                Can either be "min" or "max".

        Raises:
            ValueError: If the mode parameter is not "min" nor "max".
            ValueError: If the top parameter is not an integer
                greater than 1.
            ValueError: If the standard deviation parameter is not
                a strictly positive float.
        """
        if mode not in ("min", "max"):
            raise ValueError("The mode parameter can only be"
                             " either min or max.")
        if not isinstance(top, int) or top <= 1:
            raise ValueError("Top results to consider must be"
                             " a positive integer greater than one.")
        if not isinstance(std, float) or std <= 0:
            raise ValueError("The standard deviation must be"
                             " a strictly positive float number.")
        self._mode = mode
        self._metric = metric
        self._std = std
        self._top = top
        self._top_values = []

    def __call__(self, trial_id, result):
        """Return a boolean representing if the tuning has to stop."""
        self._top_values.append(result[self._metric])
        if self._mode == "min":
            self._top_values = sorted(self._top_values)[:self._top]
        else:
            self._top_values = sorted(self._top_values)[-self._top:]
        return self.stop_all()

    def stop_all(self):
        """Return whether to stop and prevent trials from starting."""
        return (len(self._top_values) == self._top
                and np.std(self._top_values) <= self._std)
