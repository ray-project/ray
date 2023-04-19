"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import numpy as np
import tensorflow as tf

from utils.model_dimensions import get_cnn_multiplier


class CNNAtari(tf.keras.Model):
    # TODO: Un-hard-code all hyperparameters, such as input dims, activation,
    #  filters, etc..
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        cnn_multiplier: Optional[int] = None,
    ):
        super().__init__()

        cnn_multiplier = get_cnn_multiplier(model_dimension, override=cnn_multiplier)

        # See appendix C in [1]:
        # "We use a similar network architecture but employ layer normalization and
        # SiLU as the activation function. For better framework support, we use
        # same-padded convolutions with stride 2 and kernel size 3 instead of
        # valid-padded convolutions with larger kernels ..."
        # HOWEVER: In the author's DreamerV3 repo, they use kernel size=4.
        self.conv_layers = [
            tf.keras.layers.Conv2D(
                filters=1 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
            tf.keras.layers.Conv2D(
                filters=2 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
            tf.keras.layers.Conv2D(
                filters=4 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
            # .. until output is 4 x 4 x [num_filters].
            tf.keras.layers.Conv2D(
                filters=8 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
        ]
        self.layer_normalizations = []
        for _ in range(len(self.conv_layers)):
            self.layer_normalizations.append(tf.keras.layers.LayerNormalization())
        # -> 4 x 4 x num_filters -> now flatten.
        self.flatten_layer = tf.keras.layers.Flatten(data_format="channels_last")

    def call(self, inputs):
        # [B, h, w] -> grayscale.
        if len(inputs.shape) == 3:
            inputs = tf.expand_dims(inputs, -1)
        out = inputs
        for conv_2d, layer_norm in zip(self.conv_layers, self.layer_normalizations):
            out = tf.nn.silu(layer_norm(inputs=conv_2d(out)))
        assert out.shape[1] == 4 and out.shape[2] == 4
        return self.flatten_layer(out)


if __name__ == "__main__":
    # World Models (and DreamerV2/3) Atari input space: 64 x 64 x 3
    inputs = np.random.random(size=(1, 64, 64, 3))
    model = CNNAtari(model_dimension="XS")
    out = model(inputs)
    print(out.shape)
