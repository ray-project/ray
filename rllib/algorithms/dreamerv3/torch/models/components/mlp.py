"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.torch.models.components import (
    dreamerv3_normal_initializer,
)
from ray.rllib.algorithms.dreamerv3.utils import (
    get_dense_hidden_units,
    get_num_dense_layers,
)
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class MLP(nn.Module):
    """An MLP primitive used by several DreamerV3 components and described in [1] Fig 5.

    MLP=multi-layer perceptron.

    See Appendix B in [1] for the MLP sizes depending on the given `model_size`.
    """

    def __init__(
        self,
        *,
        input_size: int,
        model_size: str = "XS",
        num_dense_layers: Optional[int] = None,
        dense_hidden_units: Optional[int] = None,
        output_layer_size=None,
    ):
        """Initializes an MLP instance.

        Args:
            input_size: The input size of the MLP.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
            num_dense_layers: The number of hidden layers in the MLP. If None,
                will use `model_size` and appendix B to figure out this value.
            dense_hidden_units: The number of nodes in each hidden layer. If None,
                will use `model_size` and appendix B to figure out this value.
            output_layer_size: The size of an optional linear (no activation) output
                layer. If None, no output layer will be added on top of the MLP dense
                stack.
        """
        super().__init__()

        self.output_size = None

        num_dense_layers = get_num_dense_layers(model_size, override=num_dense_layers)
        dense_hidden_units = get_dense_hidden_units(
            model_size, override=dense_hidden_units
        )

        layers = []
        for _ in range(num_dense_layers):
            # In this order: layer, normalization, activation.
            linear = nn.Linear(input_size, dense_hidden_units, bias=False)
            # Use same initializers as the Author in their JAX repo.
            dreamerv3_normal_initializer(linear.weight)
            layers.append(linear)
            layers.append(nn.LayerNorm(dense_hidden_units, eps=0.001))
            layers.append(nn.SiLU())
            input_size = dense_hidden_units
            self.output_size = (dense_hidden_units,)

        self.output_layer = None
        if output_layer_size:
            linear = nn.Linear(input_size, output_layer_size, bias=True)
            # Use same initializers as the Author in their JAX repo.
            dreamerv3_normal_initializer(linear.weight)
            nn.init.zeros_(linear.bias)
            layers.append(linear)
            self.output_size = (output_layer_size,)

        self._net = nn.Sequential(*layers)

    def forward(self, input_):
        """Performs a forward pass through this MLP.

        Args:
            input_: The input tensor for the MLP dense stack.
        """
        return self._net(input_)
