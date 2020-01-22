class Stopper:
    """Abstract class for implementing a Tune experiment stopper.

    Allows users to implement experiment-level stopping via ``stop_all``.

    .. code-block:: python

        import time
        from ray import tune
        from ray.tune import Stopper

        class TimeStopper(Stopper):
                def __init__(self):
                    self._start = time.time()
                    self._deadline = 300

                def stop_all(self):
                    return time.time() - self._start > self.deadline

        tune.run(Trainable, num_samples=200, stop=TimeStopper())

    """

    def __call__(self, trial_id, result):
        return False

    def stop_all(self):
        return False


class FunctionStopper(Stopper):
    def __init__(self, function):
        self._fn = function

    def __call__(self, trial_id, result):
        return self._fn(trial_id, result)

    @classmethod
    def is_valid_function(cls, fn):
        is_valid = callable(fn) and not issubclass(type(fn), Stopper)
        if hasattr(fn, "stop_all"):
            raise ValueError(
                "Stop object must be ray.tune.Stopper subclass to be detected "
                "correctly.")
        return is_valid
