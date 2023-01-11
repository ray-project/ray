from typing import List
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

_, tf, _ = try_import_tf()

# TODO (Kourosh): Find a better hierarchy for the primitives after the POC is done.


class FCNet(tf.keras.Model):
    """A simple fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layers: The sizes of the hidden layers.
        output_dim: The output dimension of the network.
        activation: The activation function to use after each layer.
            Currently "Linear" (no activation) and "ReLU" are supported.
    """

    def __init__(
        self,
        input_dim: int,
        hidden_layers: List[int],
        output_dim: int,
        activation: str = "linear",
    ):
        super().__init__()

        if activation == "linear":
            activation_class = None
        elif activation == "ReLU":
            activation_class = tf.keras.layers.ReLU
        else:
            raise ValueError("Activation function not supported")

        layers = []
        layers.append(tf.keras.Input(shape=(input_dim,)))

        for i in range(len(hidden_layers) - 1):
            if activation_class:
                layers.append(activation_class())
            layers.append(tf.keras.layers.Dense(hidden_layers[i]))

        if activation_class:
            layers.append(activation_class())
        layers.append(tf.keras.layers.Dense(output_dim))
        self.network = tf.keras.Sequential(layers)

    @override(tf.keras.Model)
    def call(self, inputs, training=None, mask=None):
        return self.network(inputs)
