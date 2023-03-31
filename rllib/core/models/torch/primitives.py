from typing import Callable, List, Optional, Union, Tuple

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
        output_dim: The output dimension of the network. If None, no specific output
            layer will be added.
        output_activation: The activation function to use for the output layer (if any).
            Currently "linear" (no activation), "relu", "swish", "elu", and "tanh" are
            supported. Also, "silu" is an alias for "swish".
        use_bias: Whether to use bias on all dense layers.
    """

    def __init__(
        self,
        *,
        input_dim: int,
        hidden_layer_dims: List[int],
        hidden_layer_activation: Union[str, Callable] = "linear",
        hidden_layer_use_layernorm: bool = False,
        output_dim: Optional[int] = None,
        output_activation: Union[str, Callable] = "linear",
        use_bias: bool = True,
    ):
        """Initialize a TorchMLP object."""
        super().__init__()
        assert input_dim > 0

        self.input_dim = input_dim

        hidden_activation = get_activation_fn(
            hidden_layer_activation, framework="torch"
        )

        layers = []
        dims = (
            [self.input_dim] + hidden_layer_dims + ([output_dim] if output_dim else [])
        )
        for i in range(0, len(dims) - 1):
            layers.append(nn.Linear(dims[i], dims[i + 1], bias=use_bias))

            # We are still in the hidden layer section: Possibly add layernorm and
            # hidden activation.
            if output_dim is None or i < len(dims) - 2:
                # Insert a layer normalization in between layer's output and
                # the activation.
                if hidden_layer_use_layernorm:
                    layers.append(nn.LayerNorm(dims[i + 1]))
                # Add the activation function.
                if hidden_activation is not None:
                    layers.append(hidden_activation())

        # Add output layer's (if any) activation.
        output_activation = get_activation_fn(
            output_activation, framework="torch"
        )
        if output_dim is not None and output_activation is not None:
            layers.append(output_activation())

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
        use_bias: Whether to use bias on all Conv2D layers.
    """

    def __init__(
        self,
        *,
        input_dims: Union[List[int], Tuple[int]],
        cnn_filter_specifiers: List[List[Union[int, List]]],
        cnn_activation: str = "relu",
        cnn_use_layernorm: bool = False,
        use_bias: bool = True,
    ):
        """Initializes a TorchCNN object."""
        super().__init__()

        assert len(input_dims) == 3

        cnn_activation = get_activation_fn(cnn_activation, framework="torch")

        layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for out_depth, kernel, stride in cnn_filter_specifiers:
            # Pad like in tensorflow's SAME mode.
            padding, out_size = same_padding(in_size, kernel, stride)
            layers.extend([
                nn.ZeroPad2d(padding),
                nn.Conv2d(in_depth, out_depth, kernel, stride, bias=use_bias)
            ])
            # Layernorm.
            if cnn_use_layernorm:
                layers.append(nn.LayerNorm((out_depth, out_size[0], out_size[1])))
            # Activation.
            if cnn_activation is not None:
                layers.append(cnn_activation())

            in_size = out_size
            in_depth = out_depth

        self.output_width, self.output_height = out_size
        self.output_depth = out_depth

        # Create the CNN.
        self.cnn = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, inputs):
        # Permute b/c data comes in as [B, dim, dim, channels]:
        inputs = inputs.permute(0, 3, 1, 2)
        return self.cnn(inputs.type(self.expected_input_dtype))


class TorchCNNTranspose(nn.Module):
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
        """Initializes a TorchCNNTranspose instance.

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
            cnn_transpose_activation, framework="torch"
        )

        layers = []

        # Add user-specified hidden convolutional layers first
        width, height, in_depth = input_dims
        in_size = [width, height]
        for i, (out_depth, kernel, stride) in enumerate(
            cnn_transpose_filter_specifiers
        ):
            is_final_layer = i == len(cnn_transpose_filter_specifiers) - 1
            # Pad like in tensorflow's SAME mode.
            padding, out_size = same_padding(in_size, kernel, stride)
            layers.extend([
                nn.ZeroPad2d(padding),
                nn.ConvTranspose2d(
                    in_depth,
                    out_depth,
                    kernel,
                    stride,
                    # Last layer always uses bias (b/c has no LayerNorm, regardless of
                    # config).
                    bias=use_bias or is_final_layer,
                ),
            ])
            # Layernorm (never for final layer).
            if cnn_transpose_use_layernorm and not is_final_layer:
                layers.append(nn.LayerNorm((out_depth, out_size[0], out_size[1])))
            # Last layer is never activated (regardless of config).
            if cnn_transpose_activation is not None and not is_final_layer:
                layers.append(cnn_transpose_activation())

            in_size = out_size
            in_depth = out_depth

        self.output_width, self.output_height = out_size
        self.output_depth = out_depth

        self.cnn_transpose = nn.Sequential(*layers)

        self.expected_input_dtype = torch.float32

    def forward(self, inputs):
        # Permute b/c data comes in as [B, dim, dim, channels]:
        out = inputs.permute(0, 3, 1, 2)
        out = self.cnn_transpose(out.type(self.expected_input_dtype))
        return out.permute(0, 2, 3, 1)
