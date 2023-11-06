"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.utils import get_cnn_multiplier
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class CNNAtari(tf.keras.Model):
    """An image encoder mapping 64x64 RGB images via 4 CNN layers into a 1D space."""

    def __init__(
        self,
        *,
        model_size: Optional[str] = "XS",
        cnn_multiplier: Optional[int] = None,
    ):
        """Initializes a CNNAtari instance.

        Args:
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the `cnn_multiplier`.
            cnn_multiplier: Optional override for the additional factor used to multiply
                the number of filters with each CNN layer. Starting with
                1 * `cnn_multiplier` filters in the first CNN layer, the number of
                filters then increases via `2*cnn_multiplier`, `4*cnn_multiplier`, till
                `8*cnn_multiplier`.
        """
        super().__init__(name="image_encoder")

        cnn_multiplier = get_cnn_multiplier(model_size, override=cnn_multiplier)

        # See appendix C in [1]:
        # "We use a similar network architecture but employ layer normalization and
        # SiLU as the activation function. For better framework support, we use
        # same-padded convolutions with stride 2 and kernel size 3 instead of
        # valid-padded convolutions with larger kernels ..."
        # HOWEVER: In Danijar's DreamerV3 repo, kernel size=4 is used, so we use it
        # here, too.
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

    @tf.function(
        input_signature=[
            tf.TensorSpec(
                shape=[None, 64, 64, 3],
                dtype=tf.keras.mixed_precision.global_policy().compute_dtype
                or tf.float32,
            )
        ]
    )
    def call(self, inputs):
        """Performs a forward pass through the CNN Atari encoder.

        Args:
            inputs: The image inputs of shape (B, 64, 64, 3).
        """
        # [B, h, w] -> grayscale.
        if len(inputs.shape) == 3:
            inputs = tf.expand_dims(inputs, -1)
        out = inputs
        for conv_2d, layer_norm in zip(self.conv_layers, self.layer_normalizations):
            out = tf.nn.silu(layer_norm(inputs=conv_2d(out)))
        assert out.shape[1] == 4 and out.shape[2] == 4
        return self.flatten_layer(out)
