from typing import Optional

from ray.rllib.utils.annotations import OldAPIStack, override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.schedules.schedule import Schedule
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()


@OldAPIStack
class ConstantSchedule(Schedule):
    """A Schedule where the value remains constant over time."""

    def __init__(self, value: float, framework: Optional[str] = None):
        """Initializes a ConstantSchedule instance.

        Args:
            value: The constant value to return, independently of time.
            framework: The framework descriptor string, e.g. "tf",
                "torch", or None.
        """
        super().__init__(framework=framework)
        self._v = value

    @override(Schedule)
    def _value(self, t: TensorType) -> TensorType:
        return self._v

    @override(Schedule)
    def _tf_value_op(self, t: TensorType) -> TensorType:
        return tf.constant(self._v)
