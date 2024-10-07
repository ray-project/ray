import abc
import logging
from typing import Tuple

import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

logger = logging.getLogger(__name__)
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

    def call(self, input_dict: dict, **kwargs) -> dict:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            dict: The output tensors.
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
