"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Optional

import tensorflow as tf

from utils.model_dimensions import get_dense_hidden_units, get_num_dense_layers


class MLP(tf.keras.Model):
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        num_dense_layers: Optional[int] = None,
        dense_hidden_units: Optional[int] = None,
        output_layer_size=None,
        trainable: bool = True,
    ):
        """TODO:

        Args:
            output_layer_size: The size of an optional linear (no activation) output
                layer. If None, no output layer will be added on top of the MLP dense
                stack.
        """
        super().__init__()

        num_dense_layers = get_num_dense_layers(
            model_dimension, override=num_dense_layers
        )
        dense_hidden_units = get_dense_hidden_units(
            model_dimension, override=dense_hidden_units
        )

        self.dense_layers = []
        for _ in range(num_dense_layers):
            self.dense_layers.append(
                tf.keras.layers.Dense(
                    dense_hidden_units,
                    trainable=trainable,
                    # Danijar's code uses no biases iff there is LayerNormalization
                    # (which there always is), and performs the activation after(!)
                    # the layer norm, not before.
                    activation=None,#tf.nn.silu,
                    use_bias=False,
                )
            )

        self.layer_normalizations = []
        for _ in range(len(self.dense_layers)):
            self.layer_normalizations.append(
                tf.keras.layers.LayerNormalization(trainable=trainable)
            )

        self.output_layer = None
        if output_layer_size:
            self.output_layer = tf.keras.layers.Dense(
                output_layer_size, activation=None, trainable=trainable
            )

    def call(self, input_):
        """

        Args:
            input_: The input tensor for the MLP dense stack.
        """
        out = input_

        for dense_layer, layer_norm in zip(self.dense_layers, self.layer_normalizations):
            # In this order: layer, normalization, activation.
            out = tf.nn.silu(layer_norm(dense_layer(out)))

        if self.output_layer is not None:
            out = self.output_layer(out)

        return out
