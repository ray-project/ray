from abc import ABCMeta, abstractmethod

from ray.rllib.utils.framework import check_framework


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

    def __init__(self, framework=None):
        # TODO(sven): replace with .tf_value() / torch_value() methods that
        # can be applied late binding, so no need to set framework during
        # construction.
        self.framework = check_framework(framework)

    @abstractmethod
    def value(self, t):
        """
        Returns the value based on a time value.

        Args:
            t (int): The time value (e.g. a time step).
                NOTE: This could be a tf.Tensor.

        Returns:
            any: The calculated value depending on the schedule and `t`.
        """
        raise NotImplementedError

    def __call__(self, t):
        """
        Simply calls `self.value(t)`.
        """
        return self.value(t)
