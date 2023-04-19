"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
import numpy as np
from typing import Optional

import tensorflow as tf
import tensorflow_probability as tfp

from ray.rllib.algorithms.dreamerv3.utils import get_cnn_multiplier


class ConvTransposeAtari(tf.keras.Model):
    # TODO: Un-hard-code all hyperparameters, such as input dims, activation,
    #  filters, etc..
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        cnn_multiplier: Optional[int] = None,
        gray_scaled: bool = True,
    ):
        super().__init__()

        cnn_multiplier = get_cnn_multiplier(model_dimension, override=cnn_multiplier)

        # The shape going into the first Conv2DTranspose layer.
        self.input_dims = (4, 4, 8 * cnn_multiplier)

        self.gray_scaled = gray_scaled

        # See appendix B in [1]:
        # "The decoder starts with a dense layer, followed by reshaping
        # to 4 × 4 × C and then inverts the encoder architecture. ..."
        self.dense_layer = tf.keras.layers.Dense(
            units=int(np.prod(self.input_dims)),
            activation=None,
            use_bias=True,
        )
        # Inverse conv2d stack. See cnn_atari.py for Conv2D stack.
        self.conv_transpose_layers = [
            tf.keras.layers.Conv2DTranspose(
                filters=4 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
            tf.keras.layers.Conv2DTranspose(
                filters=2 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
            tf.keras.layers.Conv2DTranspose(
                filters=1 * cnn_multiplier,
                kernel_size=4,
                strides=(2, 2),
                padding="same",
                # No bias or activation due to layernorm.
                activation=None,
                use_bias=False,
            ),
        ]
        # Create one LayerNorm layer for each of the Conv2DTranspose.
        self.layer_normalizations = []
        for _ in range(len(self.conv_transpose_layers)):
            self.layer_normalizations.append(tf.keras.layers.LayerNormalization())

        # .. until output is 64 x 64 x 3.

        # Important! No activation or layer norm for last layer as the outputs of
        # this one go directly into the diag-gaussian as parameters.
        self.output_conv2d_transpose = tf.keras.layers.Conv2DTranspose(
            filters=1 if self.gray_scaled else 3,
            kernel_size=4,
            strides=(2, 2),
            padding="same",
            activation=None,
            use_bias=True,  # Last layer does use bias (b/c has no LayerNorm).
        )

    def call(self, h, z):
        """TODO

        Args:
            h: The deterministic hidden state of the sequence model.
            z: The sequence of stochastic discrete representations of the original
                observation input. Note: `z` is not used for the dynamics predictor
                model (which predicts z from h).
        """
        # Flatten last two dims of z.
        assert len(z.shape) == 3
        z_shape = tf.shape(z)
        z = tf.reshape(tf.cast(z, tf.float32), shape=(z_shape[0], -1))
        assert len(z.shape) == 2
        input_ = tf.concat([h, z], axis=-1)

        # Feed through initial dense layer to get the right number of input nodes
        # for the first conv2dtranspose layer.
        out = self.dense_layer(input_)
        # Reshape to image format.
        out = tf.reshape(out, shape=(-1,) + self.input_dims)

        # Pass through stack of Conv2DTransport layers (and layer norms).
        for conv_transpose_2d, layer_norm in zip(self.conv_transpose_layers, self.layer_normalizations):
            out = tf.nn.silu(layer_norm(inputs=conv_transpose_2d(out)))
        # Last output conv2d-transpose layer:
        out = self.output_conv2d_transpose(out)
        out += 0.5  # See Danijar's code
        out_shape = tf.shape(out)

        # Interpret output as means of a diag-Gaussian with std=1.0:
        # From [2]:
        # "Distributions The image predictor outputs the mean of a diagonal Gaussian
        # likelihood with unit variance, ..."
        # Reshape `out` for the diagonal multi-variate Gaussian (each pixel is its own
        # independent (b/c diagonal co-variance matrix) variable).
        loc = tf.reshape(out, shape=(out_shape[0], -1))
        distribution = tfp.distributions.MultivariateNormalDiag(
            loc=loc,
            # Scale == 1.0.
            # [2]: "Distributions The image predictor outputs the mean of a diagonal
            # Gaussian likelihood with **unit variance** ..."
            scale_diag=tf.ones_like(loc),
        )
        pred_obs = distribution.sample()
        #if return_distribution:
        return pred_obs, distribution
        #return pred_obs


if __name__ == "__main__":
    # DreamerV2/3 Atari input space: B x 32 (num_categoricals) x 32 (num_classes)
    inputs = np.random.random(size=(1, 32, 32))
    model = ConvTransposeAtari()
    out = model(inputs)
    print(out.shape)
