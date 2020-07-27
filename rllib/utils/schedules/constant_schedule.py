from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules.schedule import Schedule


class ConstantSchedule(Schedule):
    """
    A Schedule where the value remains constant over time.
    """

    def __init__(self, value, framework):
        """
        Args:
            value (float): The constant value to return, independently of time.
        """
        super().__init__(framework=framework)
        self._v = value

    @override(Schedule)
    def _value(self, t):
        return self._v
