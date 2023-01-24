from typing import List
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.models.specs.checker import (
    input_is_decorated,
    is_output_decorated,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.experimental.base import Model
from ray.rllib.utils.typing import TensorType
from typing import Tuple

_, tf, _ = try_import_tf()


def _call_not_decorated(input_or_output):
    return (
        f"forward not decorated with {input_or_output} specification. Decorate "
        f"with @check_{input_or_output}_specs() to define a specification. See "
        f"BaseModel for examples."
    )


class TFModel(Model):
    """Base class for RLlib models.

    This class is used to define the general interface for RLlib models and checks
    whether inputs and outputs are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config):
        super().__init__(config)
        assert input_is_decorated(self.__call__), _call_not_decorated("input")
        assert is_output_decorated(self.__call__), _call_not_decorated("output")

    def __call__(self, input_dict: TensorDict) -> Tuple[TensorDict, List[TensorType]]:
        """Returns the output of this model for the given input.

        Args:
            input_dict: The input tensors.

        Returns:
            Tuple[TensorDict, List[TensorType]]: The output tensors.
        """
        raise NotImplementedError


class FCNet(tf.Module):
    """A simple fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layers: The sizes of the hidden layers.
        output_dim: The output dimension of the network.
        activation: The activation function to use after each layer.
            Currently "Linear" (no activation) and "ReLU" are supported.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layers: List[int],
        output_dim: int,
        activation: str = "linear",
    ):
        super().__init__()

        assert activation in ("linear", "ReLU", "Tanh"), (
            "Activation function not " "supported"
        )
        assert input_dim is not None, "Input dimension must not be None"
        assert output_dim is not None, "Output dimension must not be None"
        layers = []
        activation = activation.lower()
        # input = tf.keras.layers.Dense(input_dim, activation=activation)
        layers.append(tf.keras.Input(shape=(input_dim,)))
        for i in range(len(hidden_layers)):
            layers.append(
                tf.keras.layers.Dense(hidden_layers[i], activation=activation)
            )
        layers.append(tf.keras.layers.Dense(output_dim))
        self.network = tf.keras.Sequential(layers)

    def __call__(self, inputs):
        return self.network(inputs)
