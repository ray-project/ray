"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import numpy as np

from ray.rllib.algorithms.dreamerv3.utils import get_cnn_multiplier
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class ConvTransposeAtari(nn.Module):
    """A Conv2DTranspose decoder to generate Atari images from a latent space.

    Wraps an initial single linear layer with a stack of 4 Conv2DTranspose layers (with
    layer normalization) and a diag Gaussian, from which we then sample the final image.
    """

    def __init__(
        self,
        *,
        input_size: int,
        model_size: str = "XS",
        cnn_multiplier: Optional[int] = None,
        gray_scaled: bool,
    ):
        """Initializes a ConvTransposeAtari instance.

        Args:
            input_size: The input size of the ConvTransposeAtari network.
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the `cnn_multiplier`.
            cnn_multiplier: Optional override for the additional factor used to multiply
                the number of filters with each CNN transpose layer. Starting with
                8 * `cnn_multiplier` filters in the first CNN transpose layer, the
                number of filters then decreases via `4*cnn_multiplier`,
                `2*cnn_multiplier`, till `1*cnn_multiplier`.
            gray_scaled: Whether the last Conv2DTranspose layer's output has only 1
                color channel (gray_scaled=True) or 3 RGB channels (gray_scaled=False).
        """
        super().__init__()

        cnn_multiplier = get_cnn_multiplier(model_size, override=cnn_multiplier)

        self.input_dims = (4, 4, 8 * cnn_multiplier)

        self.gray_scaled = gray_scaled

        self.dense_layer = nn.Linear(
            input_size,
            int(np.prod(self.input_dims)),
            bias=True,
        )

        self.conv_transpose_layers = nn.ModuleList([
            nn.ConvTranspose2d(
                4 * cnn_multiplier,
                2 * cnn_multiplier,
                kernel_size=4,
                stride=2,
                padding=1,
                output_padding=0,
                bias=False,
            ),
            nn.ConvTranspose2d(
                2 * cnn_multiplier,
                1 * cnn_multiplier,
                kernel_size=4,
                stride=2,
                padding=1,
                output_padding=0,
                bias=False,
            ),
            nn.ConvTranspose2d(
                1 * cnn_multiplier,
                1 if self.gray_scaled else 3,
                kernel_size=4,
                stride=2,
                padding=1,
                output_padding=0,
                bias=True,
            ),
        ])

        self.layer_normalizations = nn.ModuleList([
            nn.LayerNorm(2 * cnn_multiplier * self.input_dims[0] * self.input_dims[1]),
            nn.LayerNorm(1 * cnn_multiplier * self.input_dims[0] * self.input_dims[1]),
            nn.LayerNorm(1 * self.input_dims[0] * self.input_dims[1]),
        ])

        self.output_conv2d_transpose = nn.ConvTranspose2d(
            1 * self.input_dims[2],
            1 if self.gray_scaled else 3,
            kernel_size=4,
            stride=2,
            padding=1,
            output_padding=0,
            bias=True,
        )

    def forward(self, h, z):
        """Performs a forward pass through the Conv2D transpose decoder.

        Args:
            h: The deterministic hidden state of the sequence model.
            z: The sequence of stochastic discrete representations of the original
                observation input. Note: `z` is not used for the dynamics predictor
                model (which predicts z from h).
        """
        z_shape = z.size()
        z = z.view(z_shape[0], -1)

        input_ = torch.cat([h, z], dim=-1)

        out = self.dense_layer(input_)
        out = out.view(-1, *self.input_dims)

        for conv_transpose_2d, layer_norm in zip(
            self.conv_transpose_layers, self.layer_normalizations
        ):
            out = layer_norm(conv_transpose_2d(out))
            out = nn.functional.silu(out)

        out = self.output_conv2d_transpose(out)
        out += 0.5
        out_shape = out.size()
        loc = out.view(out_shape[0], -1)

        return loc
