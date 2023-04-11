from typing import List, Optional, Union, Tuple

import numpy as np

from ray.rllib.models.torch.misc import SlimConv2d
from ray.rllib.models.torch.misc import same_padding
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLP(nn.Module):
    """A multi-layer perceptron."""

    def __init__(
        self,
        input_dim: int,
        hidden_layer_dims: List[int],
        output_dim: Optional[int] = None,
        hidden_layer_activation: str = "linear",
        output_activation: str = "linear",
    ):
        """Initialize a TorchMLP object.

        Args:
            input_dim: The input dimension of the network.
            hidden_layer_dims: The sizes of the hidden layers.
            output_dim: The output dimension of the network. If None, the last layer
                would be the last hidden layer.
            hidden_layer_activation: The activation function to use after each layer.
                output_activation: The activation function to use for the output layer.
            output_activation: The activation function to use for the output layer.
        """
        super().__init__()
        assert input_dim > 0
        assert output_dim > 0
        self.input_dim = input_dim
        hidden_layer_dims = hidden_layer_dims

        hidden_activation_class = get_activation_fn(
            hidden_layer_activation, framework="torch"
        )

        output_activation_class = get_activation_fn(
            output_activation, framework="torch"
        )

        layers = []
        dims = (
            [self.input_dim]
            + list(hidden_layer_dims)
            + ([output_dim] if output_dim else [])
        )
        layers.append(nn.Linear(dims[0], dims[1]))
        for i in range(1, len(dims) - 1):
            if hidden_activation_class is not None:
                layers.append(hidden_activation_class())
            layers.append(nn.Linear(dims[i], dims[i + 1]))

        if output_activation_class is not None:
            layers.append(output_activation_class())

        self.output_dim = dims[-1]
        self.mlp = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, x):
        return self.mlp(x.type(self.expected_input_dtype))


class TorchCNN(nn.Module):
    """A model containing a CNN and a final linear layer."""

    def __init__(
        self,
        input_dims: Union[List[int], Tuple[int]] = None,
        filter_specifiers: List[List[Union[int, List]]] = None,
        filter_layer_activation: str = "relu",
        output_activation: str = "linear",
    ):
        """Initialize a TorchCNN object.

        Args:
            input_dims: The input dimensions of the network.
            filter_specifiers: A list of lists, where each element of an inner list
                contains elements of the form
                `[number of filters, [kernel width, kernel height], stride]` to
                specify a convolutional layer stacked in order of the outer list.
            filter_layer_activation: The activation function to use after each layer (
                except for the output).
            output_activation: The activation function to use for the last filter layer.
        """
        super().__init__()

        assert len(input_dims) == 3
        assert filter_specifiers is not None, "Must provide filter specifiers."

        filter_layer_activation = get_activation_fn(
            filter_layer_activation, framework="torch"
        )

        output_activation = get_activation_fn(output_activation, framework="torch")

        layers = []
        # Create some CNN layers
        core_layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for out_depth, kernel, stride in filter_specifiers:
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
        _, kernel, stride = filter_specifiers[-1]

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
