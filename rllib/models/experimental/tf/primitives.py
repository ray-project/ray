from typing import List
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.models.specs.checker import (
    is_input_decorated,
    is_output_decorated,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.experimental.base import Model
from ray.rllib.utils.typing import TensorType
from ray.rllib.models.utils import get_activation_fn
from typing import Tuple
from ray.rllib.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)

_, tf, _ = try_import_tf()


def _call_not_decorated(input_or_output):
    return (
        f"forward not decorated with {input_or_output} specification. Decorate "
        f"with @check_{input_or_output}_specs() to define a specification. See "
        f"BaseModel for examples."
    )


class TfModel(Model, tf.Module):
    """Base class for RLlib models.

    This class is used to define the general interface for RLlib models and checks
    whether inputs and outputs are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config):
        super().__init__(config)
        # automatically apply spec checking
        if not is_input_decorated(self.__call__):
            self.__call__ = check_input_specs("input_spec", cache=True)(self.__call__)
        if not is_output_decorated(self.__call__):
            self.__call__ = check_output_specs("output_spec", cache=True)(self.__call__)

    @check_input_specs("input_spec", cache=True)
    @check_output_specs("output_spec", cache=True)
    def __call__(self, input_dict: TensorDict) -> Tuple[TensorDict, List[TensorType]]:
        """Returns the output of this model for the given input.

        Args:
            input_dict: The input tensors.

        Returns:
            Tuple[TensorDict, List[TensorType]]: The output tensors.
        """
        raise NotImplementedError


class TfMLP(tf.Module):
    """A multi-layer perceptron.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layer_dims: The sizes of the hidden layers.
        output_dim: The output dimension of the network.
        hidden_layer_activation: The activation function to use after each layer.
            Currently "Linear" (no activation) and "ReLU" are supported.
        output_activation: The activation function to use for the output layer.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layer_dims: List[int],
        output_dim: int,
        hidden_layer_activation: str = "linear",
        output_activation: str = "linear",
    ):
        super().__init__()

        assert hidden_layer_activation in ("linear", "ReLU", "Tanh"), (
            "Activation function not " "supported"
        )
        assert input_dim is not None, "Input dimension must not be None"
        assert output_dim is not None, "Output dimension must not be None"
        layers = []
        hidden_layer_activation = hidden_layer_activation.lower()
        # input = tf.keras.layers.Dense(input_dim, activation=activation)
        layers.append(tf.keras.Input(shape=(input_dim,)))
        for i in range(len(hidden_layer_dims)):
            layers.append(
                tf.keras.layers.Dense(
                    hidden_layer_dims[i], activation=hidden_layer_activation
                )
            )
        if output_activation != "linear":
            output_activation = get_activation_fn(output_activation, framework="torch")
            final_layer = tf.keras.layers.Dense(
                output_dim, activation=output_activation
            )
        else:
            final_layer = tf.keras.layers.Dense(output_dim)

        layers.append(final_layer)
        self.network = tf.keras.Sequential(layers)

    def __call__(self, inputs):
        return self.network(inputs)
