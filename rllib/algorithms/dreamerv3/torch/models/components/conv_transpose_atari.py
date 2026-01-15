"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.torch.models.components import (
    dreamerv3_normal_initializer,
)
from ray.rllib.algorithms.dreamerv3.utils import get_cnn_multiplier
from ray.rllib.core.models.configs import CNNTransposeHeadConfig
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
        self.gray_scaled = gray_scaled
        config = CNNTransposeHeadConfig(
            input_dims=[input_size],
            initial_image_dims=(4, 4, 8 * cnn_multiplier),
            initial_dense_weights_initializer=dreamerv3_normal_initializer,
            cnn_transpose_filter_specifiers=[
                [4 * cnn_multiplier, 4, 2],
                [2 * cnn_multiplier, 4, 2],
                [1 * cnn_multiplier, 4, 2],
                [1 if self.gray_scaled else 3, 4, 2],
            ],
            cnn_transpose_use_bias=False,
            cnn_transpose_use_layernorm=True,
            cnn_transpose_activation="silu",
            cnn_transpose_kernel_initializer=dreamerv3_normal_initializer,
        )
        # Make sure the output dims match Atari.
        # assert config.output_dims == (64, 64, 1 if self.gray_scaled else 3)

        self._transpose_2d_head = config.build(framework="torch")

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

        out = self._transpose_2d_head(input_)

        # Interpret output as means of a diag-Gaussian with std=1.0:
        # From [2]:
        # "Distributions: The image predictor outputs the mean of a diagonal Gaussian
        # likelihood with unit variance, ..."

        # Reshape `out` for the diagonal multi-variate Gaussian (each pixel is its own
        # independent (b/c diagonal co-variance matrix) variable).
        loc = torch.reshape(out, (z_shape[0], -1))
        return loc
