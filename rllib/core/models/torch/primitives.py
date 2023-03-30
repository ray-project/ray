from typing import Callable, List, Union, Tuple

import numpy as np

from ray.rllib.models.torch.misc import SlimConv2d
from ray.rllib.models.torch.misc import same_padding
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLP(nn.Module):
    """A multi-layer perceptron.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layer_dims: The sizes of the hidden layers.
        hidden_layer_activation: The activation function to use after each layer.
            Currently "linear" (no activation), "relu", "swish", "elu", and "tanh" are
            supported. Also, "silu" is an alias for "swish".
        hidden_layer_use_layernorm: Whether to insert a LayerNorm functionality
            in between each hidden layer's outputs and its activation.
        output_dim: The output dimension of the network.
        output_activation: The activation function to use for the output layer.
            Currently "linear" (no activation), "relu", "swish", "elu", and "tanh" are
            supported. Also, "silu" is an alias for "swish".
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
        """Initialize a TorchMLP object."""
        super().__init__()
        assert input_dim > 0
        assert output_dim > 0
        self.input_dim = input_dim
        self.hidden_layer_dims = hidden_layer_dims
        self.hidden_layer_activation = hidden_layer_activation
        self.hidden_layer_use_layernorm = hidden_layer_use_layernorm
        self.output_dim = output_dim
        self.output_activation = output_activation

        hidden_activation_class = get_activation_fn(
            self.hidden_layer_activation, framework="torch"
        )

        output_activation_class = get_activation_fn(
            self.output_activation, framework="torch"
        )

        layers = []
        dims = [self.input_dim] + self.hidden_layer_dims + [self.output_dim]
        layers.append(nn.Linear(dims[0], dims[1]))
        for i in range(1, len(dims) - 1):
            # Insert a layer normalization in between layer's output and
            # the activation.
            if self.hidden_layer_use_layernorm:
                layers.append(nn.LayerNorm(dims[i]))
            # Add the activation function.
            if hidden_activation_class is not None:
                layers.append(hidden_activation_class())
            # Add the next layer (in the last iteration, this will be the
            # output layer of size `output_dim`).
            layers.append(nn.Linear(dims[i], dims[i + 1]))

        if output_activation_class is not None:
            layers.append(output_activation_class())

        self.mlp = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, x):
        return self.mlp(x.type(self.expected_input_dtype))


class TorchCNN(nn.Module):
    """A model containing a CNN and a final linear layer.

    Attributes:
        input_dims: The input dimensions of the network.
        cnn_filter_specifiers: A list of lists, where each element of an inner
            list contains elements of the form
            `[number of filters, [kernel width, kernel height], stride]` to
            specify a convolutional layer stacked in order of the outer list.
        cnn_activation: The activation function to use after each layer (except for
            the output).
        cnn_use_layernorm: Whether to insert a LayerNorm functionality
            in between each CNN layer's outputs and its activation.
        output_activation: The activation function to use for the last filter layer.

    """

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]],
        cnn_filter_specifiers: List[List[Union[int, List]]],
        cnn_activation: str = "relu",
        cnn_use_layernorm: bool = False,
        output_activation: str = "linear",
        use_bias: bool = True,TODO: use this! and rewrite entire class
    ):
        """Initializes a TorchCNN object."""
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
            #TODO:
            # if cnn_use_layernorm:
            #    core_layers.append(nn.LayerNorm((out_depth)))
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

        # Create the cnn that potentially includes a flattened layer.
        self.cnn = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, x):
        # Permute b/c data comes in as [B, dim, dim, channels]:
        inputs = x.permute(0, 3, 1, 2)
        return self.cnn(inputs.type(self.expected_input_dtype))
