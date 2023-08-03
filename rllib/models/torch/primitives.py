from typing import List, Optional
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.deprecation import deprecation_warning
from ray.util import log_once

torch, nn = try_import_torch()

# TODO (Kourosh): Find a better hierarchy for the primitives after the POC is done.


class FCNet(nn.Module):
    """A simple fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        output_dim: The output dimension of the network. If None, the output_dim will
            be the number of nodes in the last hidden layer.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layers: List[int],
        output_dim: Optional[int] = None,
        activation: str = "linear",
    ):
        if log_once("fc_net_torch_deprecation"):
            deprecation_warning(
                old="ray.rllib.models.torch.fcnet.FCNet",
            )
        super().__init__()
        self.input_dim = input_dim
        self.hidden_layers = hidden_layers

        activation_class = getattr(nn, activation, lambda: None)()
        self.layers = []
        self.layers.append(nn.Linear(self.input_dim, self.hidden_layers[0]))
        for i in range(len(self.hidden_layers) - 1):
            if activation != "linear":
                self.layers.append(activation_class)
            self.layers.append(
                nn.Linear(self.hidden_layers[i], self.hidden_layers[i + 1])
            )

        if output_dim is not None:
            if activation != "linear":
                self.layers.append(activation_class)
            self.layers.append(nn.Linear(self.hidden_layers[-1], output_dim))

        if output_dim is None:
            self.output_dim = hidden_layers[-1]
        else:
            self.output_dim = output_dim

        self.layers = nn.Sequential(*self.layers)

    def forward(self, x):
        return self.layers(x)
