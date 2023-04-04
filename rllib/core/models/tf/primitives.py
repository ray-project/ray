from typing import Callable, List, Optional, Tuple, Union

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

_, tf, _ = try_import_tf()


class TfMLP(tf.keras.Model):
    """A multi-layer perceptron.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layer_dims: The sizes of the hidden layers.
        hidden_layer_activation: The activation function to use after each layer.
            Either a tf.nn.[activation fn] callable or a string that's supported by
            tf.keras.layers.Activation(activation=...), e.g. "relu", "ReLU", "silu",
            or "linear".
        hidden_layer_use_layernorm: Whether to insert a LayerNorm functionality
            in between each hidden layer's output and its activation.
        output_dim: The output dimension of the network. If None, no specific output
            layer will be added.
        output_activation: The activation function to use for the output layer (if any).
            Either a tf.nn.[activation fn] callable or a string that's supported by
            tf.keras.layers.Activation(activation=...), e.g. "relu", "ReLU", "silu",
            or "linear".
        use_bias: Whether to use bias on all dense layers.
    """

    def __init__(
        self,
        *,
        input_dim: int,
        hidden_layer_dims: List[int],
        hidden_layer_activation: Union[str, Callable] = "relu",
        hidden_layer_use_layernorm: bool = False,
        output_dim: Optional[int] = None,
        output_activation: Union[str, Callable] = "linear",
        use_bias: bool = True,
    ):
        """Initialize a TfMLP object."""
        super().__init__()
        assert input_dim > 0

        layers = []
        # Input layer.
        layers.append(tf.keras.Input(shape=(input_dim,)))

        for i in range(len(hidden_layer_dims)):
            # Dense layer with activation (or w/o in case we use LayerNorm, in which
            # case the activation is applied after the layer normalization step).
            layers.append(
                tf.keras.layers.Dense(
                    hidden_layer_dims[i],
                    activation=(
                        hidden_layer_activation
                        if not hidden_layer_use_layernorm
                        else None
                    ),
                    use_bias=use_bias,
                )
            )
            # Add LayerNorm and activation.
            if hidden_layer_use_layernorm:
                layers.append(tf.keras.layers.LayerNormalization(epsilon=1e-5))
                layers.append(tf.keras.layers.Activation(hidden_layer_activation))

        if output_dim is not None:
            output_activation = get_activation_fn(output_activation, framework="tf2")
            layers.append(
                tf.keras.layers.Dense(
                    output_dim,
                    activation=output_activation,
                    use_bias=use_bias,
                )
            )

        self.network = tf.keras.Sequential(layers)

    def call(self, inputs, **kwargs):
        return self.network(inputs)


class TfCNN(tf.keras.Model):
    """A model containing a CNN with N Conv2D layers."""

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]] = None,
        cnn_filter_specifiers: List[List[Union[int, List]]],
        cnn_activation: str = "relu",
        cnn_use_layernorm: bool = False,
        use_bias: bool = True,
    ):
        """Initializes a TfCNN instance.

        Args:
            input_dims: The input dimensions of the network.
            cnn_filter_specifiers: A list of lists, where each element of an inner list
                contains elements of the form
                `[number of filters, [kernel width, kernel height], stride]` to
                specify a convolutional layer stacked in order of the outer list.
            cnn_activation: The activation function to use after each Conv2D layer.
            cnn_use_layernorm: Whether to insert a LayerNorm functionality
                in between each Conv2D layer's output and its activation.
            use_bias: Whether to use bias on all Conv2D layers.
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_activation = get_activation_fn(cnn_activation, framework="tf2")

        layers = []
        # Input layer.
        layers.append(tf.keras.layers.Input(shape=input_dims))

        for num_filters, kernel_size, strides in cnn_filter_specifiers:
            layers.append(
                tf.keras.layers.Conv2D(
                    filters=num_filters,
                    kernel_size=kernel_size,
                    strides=strides,
                    padding="same",
                    use_bias=use_bias,
                    activation=None if cnn_use_layernorm else cnn_activation,
                )
            )
            if cnn_use_layernorm:
                layers.append(tf.keras.layers.LayerNormalization(axis=[-3, -2, -1], epsilon=1e-5))
                layers.append(tf.keras.layers.Activation(cnn_activation))

        # Create the cnn that potentially includes a flattened layer
        self.cnn = tf.keras.Sequential(layers)

        self.expected_input_dtype = tf.float32

    def call(self, inputs, **kwargs):
        return self.cnn(tf.cast(inputs, self.expected_input_dtype))
