"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from typing import Optional

from ray.rllib.algorithms.dreamerv3.utils import (
    get_dense_hidden_units,
    get_num_dense_layers,
)
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()


class MLP(tf.keras.Model):
    """An MLP primitive used by several DreamerV3 components and described in [1] Fig 5.

    MLP=multi-layer perceptron.

    See Appendix B in [1] for the MLP sizes depending on the given `model_size`.
    """

    def __init__(
        self,
        *,
        model_size: Optional[str] = "XS",
        num_dense_layers: Optional[int] = None,
        dense_hidden_units: Optional[int] = None,
        output_layer_size=None,
        trainable: bool = True,
        name: Optional[str] = None
    ):
        """Initializes an MLP instance.

        Args:
            model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
            num_dense_layers: The number of hidden layers in the MLP. If None,
                will use `model_size` and appendix B to figure out this value.
            dense_hidden_units: The number of nodes in each hidden layer. If None,
                will use `model_size` and appendix B to figure out this value.
            output_layer_size: The size of an optional linear (no activation) output
                layer. If None, no output layer will be added on top of the MLP dense
                stack.
            trainable: Whether the MLP is trainable (updated by an optimizer) or not.
            name: An optional name for the MLP keras model.
        """
        super().__init__(name=name or "mlp")

        num_dense_layers = get_num_dense_layers(model_size, override=num_dense_layers)
        dense_hidden_units = get_dense_hidden_units(
            model_size, override=dense_hidden_units
        )

        self.dense_layers = []
        for _ in range(num_dense_layers):
            self.dense_layers.append(
                tf.keras.layers.Dense(
                    dense_hidden_units,
                    trainable=trainable,
                    # Use no biases, iff there is LayerNormalization
                    # (which there always is), and perform the activation after the
                    # layer normalization.
                    activation=None,
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
        """Performs a forward pass through this MLP.

        Args:
            input_: The input tensor for the MLP dense stack.
        """
        out = input_

        for dense_layer, layer_norm in zip(
            self.dense_layers, self.layer_normalizations
        ):
            # In this order: layer, normalization, activation.
            out = tf.nn.silu(layer_norm(dense_layer(out)))

        if self.output_layer is not None:
            out = self.output_layer(out)

        return out
