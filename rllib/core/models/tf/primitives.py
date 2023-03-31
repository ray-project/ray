from typing import Callable, List, Optional, Tuple, Union

import numpy as np

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

        # input = tf.keras.layers.Dense(input_dim, activation=activation)
        layers.append(tf.keras.Input(shape=(input_dim,)))
        for i in range(len(hidden_layer_dims)):
            # Dense layer with activation (or w/o in case we use LayerNorm, in which
            # case the activation is applied after the layer normalization step).
            layers.append(
                tf.keras.layers.Dense(
                    hidden_layer_dims[i],
                    activation=(
                        hidden_layer_activation if not hidden_layer_use_layernorm
                        else None
                    ),
                    use_bias=use_bias,
                )
            )
            # Add LayerNorm and activation.
            if hidden_layer_use_layernorm:
                layers.append(tf.keras.layers.LayerNormalization())
                layers.append(tf.keras.layers.Activation(hidden_layer_activation))

        if output_dim is not None:
            output_activation = get_activation_fn(output_activation, framework="tf2")
            layers.append(tf.keras.layers.Dense(
                output_dim,
                activation=output_activation,
                use_bias=use_bias,
            ))

        self.network = tf.keras.Sequential(layers)

    def call(self, inputs, **kwargs):
        return self.network(inputs)


class TfCNN(tf.keras.Model):
    """A model containing a CNN with N Conv2D layers."""

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]],
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
            cnn_activation: The activation function to use after each layer (except for
                the output).
            cnn_use_layernorm: Whether to insert a LayerNorm functionality
                in between each Conv2D layer's output and its activation.
            use_bias: Whether to use bias on all Conv2D layers.
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_activation = get_activation_fn(cnn_activation, framework="tf2")

        layers = []

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
                layers.append(tf.keras.layers.LayerNormalization(axis=[-3, -2, -1]))
                layers.append(tf.keras.layers.Activation(cnn_activation))

        # Create the cnn that potentially includes a flattened layer
        self.cnn = tf.keras.Sequential(layers)

        self.expected_input_dtype = tf.float32

    def call(self, inputs, **kwargs):
        return self.cnn(tf.cast(inputs, self.expected_input_dtype))


class TfCNNTranspose(tf.keras.Model):
    """A model containing a CNNTranspose with N Conv2DTranspose layers."""

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]],
        cnn_transpose_filter_specifiers: List[List[Union[int, List]]],
        cnn_transpose_activation: str = "relu",
        cnn_transpose_use_layernorm: bool = False,
        use_bias: bool = True,
    ):
        """Initializes a TfCNNTranspose instance.

        Args:
            input_dims: The input dimensions of the network. This is a 3D tensor.
            cnn_transpose_filter_specifiers: A list of lists, where each element of an
                inner list contains elements of the form
                `[number of filters, [kernel width, kernel height], stride]` to
                specify a convolutional transpose layer stacked in order of the outer
                list.
            cnn_transpose_activation: The activation function to use after each layer
                (except for the last Conv2DTranspose layer, which is always
                non-activated).
            cnn_transpose_use_layernorm: Whether to insert a LayerNorm functionality
                in between each Conv2DTranspose layer's output and its activation.
                The last Conv2DTranspose layer will not be normed, regardless.
            use_bias: Whether to use bias on all Conv2DTranspose layers.
        """
        super().__init__()

        assert len(input_dims) == 3

        cnn_transpose_activation = get_activation_fn(
            cnn_transpose_activation, framework="tf2"
        )

        layers = []

        for i, (num_filters, kernel_size, strides) in enumerate(
            cnn_transpose_filter_specifiers
        ):
            is_final_layer = i == len(cnn_transpose_filter_specifiers) - 1
            layers.append(
                tf.keras.layers.Conv2DTranspose(
                    filters=num_filters,
                    kernel_size=kernel_size,
                    strides=strides,
                    padding="same",
                    # Last layer is never activated (regardless of config).
                    activation=(
                        None if cnn_transpose_use_layernorm or is_final_layer
                        else cnn_transpose_activation
                    ),
                    # Last layer always uses bias (b/c has no LayerNorm, regardless of
                    # config).
                    use_bias=use_bias or is_final_layer,
                )
            )
            if cnn_transpose_use_layernorm and not is_final_layer:
                layers.append(tf.keras.layers.LayerNormalization(axis=[-3, -2, -1]))
                layers.append(tf.keras.layers.Activation(cnn_transpose_activation))

        self.cnn_transpose = tf.keras.Sequential(layers)

        self.expected_input_dtype = tf.float32

    def call(self, inputs, **kwargs):
        return self.cnn_transpose(tf.cast(inputs, self.expected_input_dtype))
