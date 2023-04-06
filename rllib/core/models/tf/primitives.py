import abc
from typing import List

from ray.rllib.core.models.base import Model, ModelConfig
from ray.rllib.core.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)
from ray.rllib.core.models.specs.checker import (
    is_input_decorated,
    is_output_decorated,
)
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict

_, tf, _ = try_import_tf()


def _raise_not_decorated_exception(input_or_output):
    raise ValueError(
        f"`TfModel.__call__()` not decorated with {input_or_output} specification. "
        f"Decorate it with @check_{input_or_output}_specs() to define a specification."
    )


class TfModel(Model, tf.keras.Model, abc.ABC):
    """Base class for RLlib's TensorFlow models.

    This class defines the interface for RLlib's TensorFlow models and checks
    whether inputs and outputs of __call__ are checked with `check_input_specs()` and
    `check_output_specs()` respectively.
    """

    def __init__(self, config: ModelConfig):
        tf.keras.Model.__init__(self)
        Model.__init__(self, config)

        # automatically apply spec checking
        if not is_input_decorated(self.__call__):
            _raise_not_decorated_exception("input")
        if not is_output_decorated(self.__call__):
            _raise_not_decorated_exception("output")

    @check_input_specs("input_specs")
    @check_output_specs("output_specs")
    def __call__(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        return self._forward(input_dict, **kwargs)


class TfMLP(tf.keras.Model):
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

        assert input_dim is not None, "Input dimension must not be None"
        assert output_dim is not None, "Output dimension must not be None"
        layers = []

        # input = tf.keras.layers.Dense(input_dim, activation=activation)
        layers.append(tf.keras.Input(shape=(input_dim,)))
        for i in range(len(hidden_layer_dims)):
            layers.append(
                tf.keras.layers.Dense(
                    hidden_layer_dims[i], activation=hidden_layer_activation
                )
            )
        if output_activation != "linear":
            output_activation = get_activation_fn(output_activation, framework="tf2")
            final_layer = tf.keras.layers.Dense(
                output_dim,
                activation=output_activation,
            )
        else:
            final_layer = tf.keras.layers.Dense(output_dim)

        layers.append(final_layer)
        self.network = tf.keras.Sequential(layers)

    def __call__(self, inputs):
        return self.network(inputs)
