import abc
from typing import Tuple

import numpy as np

from ray.rllib.core.models.base import (
    Model,
    ModelConfig,
    _raise_not_decorated_exception,
)
from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    check_output_specs,
    is_input_decorated,
    is_output_decorated,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

_, tf, _ = try_import_tf()


class TfModel(Model, tf.keras.Model, abc.ABC):
    """Base class for RLlib's TensorFlow models.

    This class defines the interface for RLlib's TensorFlow models and checks
    whether inputs and outputs of __call__ are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config: ModelConfig):
        tf.keras.Model.__init__(self)
        Model.__init__(self, config)

        # Raise errors if forward method is not decorated to check specs.
        if not is_input_decorated(self.call):
            _raise_not_decorated_exception(type(self).__name__ + ".call()", "input")
        if not is_output_decorated(self.call):
            _raise_not_decorated_exception(type(self).__name__ + ".call()", "output")

    @check_input_specs("input_specs")
    @check_output_specs("output_specs")
    def call(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        return self._forward(input_dict, **kwargs)

    @override(Model)
    def get_num_parameters(self) -> Tuple[int, int]:
        return (
            sum(int(np.prod(w.shape)) for w in self.trainable_weights),
            sum(int(np.prod(w.shape)) for w in self.non_trainable_weights),
        )

    @override(Model)
    def _set_to_dummy_weights(self, value_sequence=(-0.02, -0.01, 0.01, 0.02)):
        for i, w in enumerate(self.trainable_weights + self.non_trainable_weights):
            fill_val = value_sequence[i % len(value_sequence)]
            w.assign(tf.fill(w.shape, fill_val))
