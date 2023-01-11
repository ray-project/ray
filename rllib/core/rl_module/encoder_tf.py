from dataclasses import dataclass, field
from typing import List

from ray.rllib.core.rl_module.encoder import EncoderConfig
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.models.tf.primitives import FCNet

tf1, tf, tfv = try_import_tf()


@dataclass
class FCTfConfig(EncoderConfig):
    """Configuration for a fully connected network.
    input_dim: The input dimension of the network. It cannot be None.
    hidden_layers: The sizes of the hidden layers.
    activation: The activation function to use after each layer (except for the
        output).
    output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    output_dim: int = None
    hidden_layers: List[int] = field(default_factory=lambda: [256, 256])
    activation: str = "ReLU"

    def build(self):
        return FCNet(
            self.input_dim, self.hidden_layers, self.output_dim, self.activation
        )
