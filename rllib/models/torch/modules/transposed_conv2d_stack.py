from typing import Tuple

from ray.rllib.models.torch.modules.reshape import Reshape
from ray.rllib.utils.framework import get_activation_fn, try_import_torch

torch, nn = try_import_torch()
if torch:
    import torch.distributions as td


class TransposedConv2DStack(nn.Module):
    """Transposed Conv2D decoder generating image distribution from a vector.
    """

    def __init__(self,
                 input_size: int,
                 in_channels: int = 32,
                 filters: Tuple[Tuple[int]] = ((32, ), (), (), ()),
                 activation: str = "relu",
                 output_shape: Tuple[int] = (3, 64, 64)):
        """Initializes a TransposedConv2DStack instance.

        Args:
            input_size (int): The size of the 1D input vector, from which to
                generate the image distribution.
            in_channels (int): Number of channels in the first transposed
                Conv2d layer.
            activation (str): Activation function descriptor (str).
            output_shape (Tuple[int]): Shape of the final output image.
        """
        super().__init__()
        self.activation = get_activation_fn(activation)
        self.in_channels = in_channels
        self.output_shape = output_shape

        self.layers = [
            nn.Linear(input_size, 32 * self.in_channels),
            Reshape([-1, 32 * self.in_channels, 1, 1]),
            nn.ConvTranspose2d(32 * self.in_channels, 4 * self.in_channels, 5, stride=2),
            self.activation(),
            nn.ConvTranspose2d(4 * self.in_channels, 2 * self.in_channels, 5, stride=2),
            self.activation(),
            nn.ConvTranspose2d(2 * self.in_channels, self.in_channels, 6, stride=2),
            self.activation(),
            nn.ConvTranspose2d(self.in_channels, self.output_shape[0], 6, stride=2),
        ]
        self.model = nn.Sequential(*self.layers)

    def forward(self, x):
        # x is [batch, hor_length, input_size]
        orig_shape = list(x.size())
        x = self.model(x)

        reshape_size = orig_shape[:-1] + self.output_shape
        mean = x.view(*reshape_size)

        # Equivalent to making a multivariate diag
        return td.Independent(td.Normal(mean, 1), len(self.output_shape))
