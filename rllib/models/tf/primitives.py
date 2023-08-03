from typing import List
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.deprecation import Deprecated

_, tf, _ = try_import_tf()

# TODO (Kourosh): Find a better hierarchy for the primitives after the POC is done.


@Deprecated(error=False)
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

        if activation not in ("linear", "ReLU", "Tanh"):
            raise ValueError("Activation function not supported")
        assert input_dim is not None, "Input dimension must not be None"
        assert output_dim is not None, "Output dimension must not be None"
        layers = []
        activation = activation.lower()
        # input = tf.keras.layers.Dense(input_dim, activation=activation)
        layers.append(tf.keras.Input(shape=(input_dim,)))
        for i in range(len(hidden_layers)):
            layers.append(
                tf.keras.layers.Dense(hidden_layers[i], activation=activation)
            )
        layers.append(tf.keras.layers.Dense(output_dim))
        self.network = tf.keras.Sequential(layers)

    @override(tf.keras.Model)
    def call(self, inputs, training=None, mask=None):
        return self.network(inputs)


@Deprecated(error=False)
class IdentityNetwork(tf.keras.Model):
    """A network that returns the input as the output."""

    @override(tf.keras.Model)
    def call(self, inputs, training=None, mask=None):
        return inputs
