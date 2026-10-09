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
from ray.rllib.core.models.base import ENCODER_OUT
from ray.rllib.core.models.configs import CNNEncoderConfig
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class CNNAtari(nn.Module):
    """An image encoder mapping 64x64 RGB images via 4 CNN layers into a 1D space."""

    def __init__(
        self,
        *,
        model_size: str = "XS",
        cnn_multiplier: Optional[int] = None,
        gray_scaled: bool,
    ):
        """Initializes a CNNAtari instance.

        Args:
            model_size: The "Model Size" used according to [1] Appendix B.
                Use None for manually setting the `cnn_multiplier`.
            cnn_multiplier: Optional override for the additional factor used to multiply
                the number of filters with each CNN layer. Starting with
                1 * `cnn_multiplier` filters in the first CNN layer, the number of
                filters then increases via `2*cnn_multiplier`, `4*cnn_multiplier`, till
                `8*cnn_multiplier`.
            gray_scaled: Whether the input is a gray-scaled image (1 color channel) or
                not (3 RGB channels).
        """
        super().__init__()

        cnn_multiplier = get_cnn_multiplier(model_size, override=cnn_multiplier)

        config = CNNEncoderConfig(
            input_dims=[64, 64, 1 if gray_scaled else 3],
            cnn_filter_specifiers=[
                [1 * cnn_multiplier, 4, 2],
                [2 * cnn_multiplier, 4, 2],
                [4 * cnn_multiplier, 4, 2],
                [8 * cnn_multiplier, 4, 2],
            ],
            cnn_use_bias=False,
            cnn_use_layernorm=True,
            cnn_activation="silu",
            cnn_kernel_initializer=dreamerv3_normal_initializer,
            flatten_at_end=True,
        )
        self.cnn_stack = config.build(framework="torch")
        self.output_size = config.output_dims

    def forward(self, inputs):
        """Performs a forward pass through the CNN Atari encoder.

        Args:
            inputs: The image inputs of shape (B, 64, 64, 3).
        """
        return self.cnn_stack({SampleBatch.OBS: inputs})[ENCODER_OUT]
