from typing import List, Optional

from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import try_import_torch

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
