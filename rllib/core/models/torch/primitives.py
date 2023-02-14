from typing import List, Optional, Union

from ray.rllib.models.torch.misc import SlimConv2d
from ray.rllib.models.torch.misc import same_padding
from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

_, nn = try_import_torch()


class TorchMLP(nn.Module):
    """A multi-layer perceptron.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layer_dims: The sizes of the hidden layers.
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
        hidden_layer_activation: The activation function to use after each layer.
        output_activation: The activation function to use for the output layer.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layer_dims: List[int],
        output_dim: Optional[int] = None,
        hidden_layer_activation: str = "linear",
        output_activation: str = "linear",
    ):
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
        dims = [input_dim] + hidden_layer_dims + [output_dim]
        layers.append(nn.Linear(dims[0], dims[1]))
        for i in range(1, len(dims) - 1):
            if hidden_activation_class is not None:
                layers.append(hidden_activation_class())
            layers.append(nn.Linear(dims[i], dims[i + 1]))

        if output_activation_class is not None:
            layers.append(output_activation_class())

        self.output_dim = dims[-1]
        self.mlp = nn.Sequential(*layers)

    def forward(self, x):
        return self.mlp(x)


class TorchCNN(nn.Module):
    """A convolutional neural network.

    Attributes:
        input_dims: The input dimensions `[width, height, depth`] of the network.
        Cannot be None.
        filters_layer_dims: The sizes of the hidden layers.
        filter_layer_activation: The activation function to use after each layer.
        output_activation: The activation function to use for the output layer.
    """

    def __init__(
        self,
        input_dims: List[int],
        filter_specifiers: List[List[Union[int, List]]] = None,
        filter_layer_activation: str = "relu",
        output_activation: str = "relu",
    ):
        super().__init__()
        assert filter_specifiers is not None, "Must provide filter specifiers."

        layers = []

        # Add hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for out_depth, kernel, stride in filter_specifiers[:-1]:
            padding, out_size = same_padding(in_size, kernel, stride)
            # TODO(Artur): Inline SlimConv2d
            layers.append(
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

        # Add final convolutional layer (possibly with a different activation)
        out_depth, kernel, stride = filter_specifiers[-1]
        padding, out_size = same_padding(in_size, kernel, stride)
        # TODO(Artur): Inline SlimConv2d
        layers.append(
            SlimConv2d(
                in_depth,
                out_depth,
                kernel,
                stride,
                padding,
                activation_fn=output_activation,
            )
        )

        # Make some dimensions available to upward abstractions
        # Store [width, height, depth] of the last layer accessible
        self.out_dims = [*out_size, out_depth]
        # Store kernel and stride of the last layer
        self.last_kernel = kernel
        self.last_stride = stride

        # Build the model
        self.cnn = nn.Sequential(*layers)

    def forward(self, x: TensorType) -> TensorType:
        return self.cnn(x)
