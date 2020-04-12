from abc import ABCMeta, abstractmethod

from ray.rllib.utils.framework import check_framework
from ray.rllib.utils.framework import try_import_tf

tf = try_import_tf()


class Schedule(metaclass=ABCMeta):
    """
    Schedule classes implement various time-dependent scheduling schemas, such
    as:
    - Constant behavior.
    - Linear decay.
    - Piecewise decay.

    Useful for backend-agnostic rate/weight changes for learning rates,
    exploration epsilons, beta parameters for prioritized replay, loss weights
    decay, etc..

    Each schedule can be called directly with the `t` (absolute time step)
    value and returns the value dependent on the Schedule and the passed time.
    """

    def __init__(self, framework):
        self.framework = check_framework(framework)

    @abstractmethod
    def _value(self, t):
        """
        Returns the value based on a time step input.

        Args:
            t (int): The time step. This could be a tf.Tensor.

        Returns:
            any: The calculated value depending on the schedule and `t`.
        """
        raise NotImplementedError

    def value(self, t):
        if self.framework == "tf":
            return tf.cast(
                tf.py_function(self._value, [t], tf.float64),
                tf.float32,
                name="schedule_value")
        return self._value(t)

    def __call__(self, t):
        """
        Simply calls `self.value(t)`.
        """
        return self.value(t)
