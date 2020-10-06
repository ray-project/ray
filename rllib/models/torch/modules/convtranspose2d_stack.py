from typing import Tuple

from ray.rllib.models.torch.modules.reshape import Reshape
from ray.rllib.models.utils import get_initializer
from ray.rllib.utils.framework import get_activation_fn, try_import_torch

torch, nn = try_import_torch()
if torch:
    import torch.distributions as td


class ConvTranspose2DStack(nn.Module):
    """ConvTranspose2D decoder generating an image distribution from a vector.
    """

    def __init__(self,
                 *,
                 input_size: int,
                 filters: Tuple[Tuple[int]] = (
                 (1024, 5, 2), (128, 5, 2), (64, 6, 2), (32, 6, 2)),
                 initializer="default",
                 bias_init=0,
                 activation_fn: str = "relu",
                 output_shape: Tuple[int] = (3, 64, 64)):
        """Initializes a TransposedConv2DStack instance.

        Args:
            input_size (int): The size of the 1D input vector, from which to
                generate the image distribution.
            filters (Tuple[Tuple[int]]): Tuple of filter setups (1 for each
                ConvTranspose2D layer): [in_channels, kernel, stride].
            initializer (Union[str]):
            bias_init (float): The initial bias values to use.
            activation_fn (str): Activation function descriptor (str).
            output_shape (Tuple[int]): Shape of the final output image.
        """
        super().__init__()
        self.activation = get_activation_fn(activation_fn, framework="torch")
        self.output_shape = output_shape
        initializer = get_initializer(initializer, framework="torch")

        in_channels = filters[0][0]
        self.layers = [
            # Map from 1D-input vector to correct initial size for the
            # Conv2DTransposed stack.
            nn.Linear(input_size, in_channels),
            # Reshape into image format (channels first).
            Reshape([-1, in_channels, 1, 1]),
        ]
        for out_channels, kernel, stride in filters:
            conv_transp = nn.ConvTranspose2d(
                in_channels, out_channels, kernel, stride)
            # Apply initializer.
            initializer(conv_transp.weight)
            nn.init.constant_(conv_transp.bias, bias_init)
            self.layers.append(conv_transp)
            # Apply activation function, if any.
            if self.activation is not None:
                self.layers.append(self.activation())

            # num-outputs == num-inputs for next layer.
            in_channels = out_channels

        self._model = nn.Sequential(*self.layers)

    def forward(self, x):
        # x is [batch, hor_length, input_size]
        orig_shape = list(x.size())
        x = self._model(x)

        reshape_size = orig_shape[:-1] + self.output_shape
        mean = x.view(*reshape_size)

        # Equivalent to making a multivariate diag
        return td.Independent(td.Normal(mean, 1), len(self.output_shape))
