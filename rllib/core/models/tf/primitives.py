from typing import Callable, List, Tuple, Union

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
            in between each hidden layers' outputs and their activations.
        output_dim: The output dimension of the network.
        output_activation: The activation function to use for the output layer.
    """

    def __init__(
        self,
        *,
        input_dim: int,
        hidden_layer_dims: List[int],
        hidden_layer_activation: Union[str, Callable] = "linear",
        hidden_layer_use_layernorm: bool = False,
        output_dim: int,
        output_activation: Union[str, Callable] = "linear",
    ):
        """Initialize a TfMLP object."""
        super().__init__()

        layers = []

        # input = tf.keras.layers.Dense(input_dim, activation=activation)
        layers.append(tf.keras.Input(shape=(input_dim,)))
        for i in range(len(hidden_layer_dims)):
            # Dense layer with activation (or w/o in case we use LayerNorm, in which
            # case the activation is applied after the layer normalization step).
            layers.append(
                tf.keras.layers.Dense(
                    hidden_layer_dims[i], activation=(
                        hidden_layer_activation if not hidden_layer_use_layernorm
                        else None
                    )
                )
            )
            # Add LayerNorm and activation.
            if hidden_layer_use_layernorm:
                layers.append(tf.keras.layers.LayerNorm())
                layers.append(tf.keras.layers.Activation(hidden_layer_activation))

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


class TfCNN(tf.keras.Model):
    """A model containing a CNN and a final linear layer."""

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]] = None,
        cnn_filter_specifiers: List[List[Union[int, List]]],
        cnn_activation: str = "relu",
        cnn_use_layernorm: bool = False,
        output_activation: str = "linear",
    ):
        """Initializes a TorchCNN object.

        Args:
            input_dims: The input dimensions of the network.
            cnn_filter_specifiers: A list of lists, where each element of an inner list
                contains elements of the form
                `[number of filters, [kernel width, kernel height], stride]` to
                specify a convolutional layer stacked in order of the outer list.
            cnn_activation: The activation function to use after each layer (except for
                the output).
            output_activation: The activation function to use for the last filter layer.
        """
        super().__init__()

        assert len(input_dims) == 3

        filter_layer_activation = get_activation_fn(cnn_activation, framework="torch")
        output_activation = get_activation_fn(output_activation, framework="torch")

        layers = []
        # Create some CNN layers
        core_layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for out_depth, kernel, stride in cnn_filter_specifiers:
            # Pad like in tensorflow's SAME mode.
            padding, out_size = same_padding(in_size, kernel, stride)
            # TODO(Artur): Inline SlimConv2d after old models are deprecated.
            core_layers.append(
                SlimConv2d(
                    in_depth,
                    out_depth,
                    kernel,
                    stride,
                    padding,
                    activation_fn=filter_layer_activation,
                )
            )
            in_depth = out_depth
            in_size = out_size

        core_cnn = nn.Sequential(*core_layers)
        layers.append(core_cnn)

        # Append a last convolutional layer of depth-only filters to be flattened.

        # Get info of the last layer of user-specified layers
        [width, height] = in_size
        _, kernel, stride = cnn_filter_specifiers[-1]

        in_size = (
            int(np.ceil((width - kernel[0]) / stride)),
            int(np.ceil((height - kernel[1]) / stride)),
        )
        padding, _ = same_padding(in_size, (1, 1), (1, 1))
        # TODO(Artur): Inline SlimConv2d after old models are deprecated.
        layers.append(
            SlimConv2d(
                in_depth,
                1,
                (1, 1),
                1,
                padding,
                activation_fn=output_activation,
            )
        )
        self.output_width = width
        self.output_height = height

        # Create the cnn that potentially includes a flattened layer
        self.cnn = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, x):
        # Permute b/c data comes in as [B, dim, dim, channels]:
        inputs = x.permute(0, 3, 1, 2)
        return self.cnn(inputs.type(self.expected_input_dtype))
