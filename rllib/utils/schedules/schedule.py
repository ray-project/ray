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

    def _tf_value_op(self, t):
        """
        Returns the value tf op based on a time step input.

        Args:
            t (tf.Tensor): The time step tf.Tensor.

        Returns:
            tf.Tensor: The calculated value depending on the schedule and `t`.
        """
        # By default (most of the time), tf should work as python code.
        return self._value(t)

    def value(self, t):
        if self.framework == "tf" and not tf.executing_eagerly():
            return self._tf_value_op(t)
        return self._value(t)

    def __call__(self, t):
        """
        Simply calls `self.value(t)`.
        """
        return self.value(t)
