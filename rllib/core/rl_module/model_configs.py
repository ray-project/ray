from dataclasses import dataclass, field
from typing import List
import functools

from ray.rllib.models.torch.primitives import Identity
from ray.rllib.models.base_model import ModelConfig, Model


def check_framework(fn):
    @functools.wraps(fn)
    def checked_build(self, framework, **kwargs):
        if framework not in ("torch", "tf", "tf2"):
            raise ValueError(f"Framework {framework} not supported.")
        return fn(self, framework, **kwargs)

    return checked_build


@dataclass
class FCConfig(ModelConfig):
    """Configuration for a fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer (except for the
            output).
        output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    hidden_layers: List[int] = field(default_factory=lambda: [256, 256])
    activation: str = "ReLU"
    output_activation: str = "ReLU"

    @check_framework
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.core.rl_module.torch.fcmodel import FCModel
        else:
            from ray.rllib.core.rl_module.tf.fcmodel import FCModel
        return FCModel(self)


@dataclass
class FCEncoderConfig(FCConfig):
    def build(self, framework: str = "torch"):
        if framework == "torch":
            from ray.rllib.core.rl_module.torch.encoder import FCEncoder
        else:
            from ray.rllib.core.rl_module.tf.encoder import FCEncoder
        return FCEncoder(self)


@dataclass
class LSTMEncoderConfig(ModelConfig):
    input_dim: int = None
    hidden_dim: int = None
    num_layers: int = None
    batch_first: bool = True

    @check_framework
    def build(self, framework: str = "torch"):
        if not framework == "torch":
            raise ValueError("Only torch framework supported.")
        from rllib.core.rl_module.torch.encoder import LSTMEncoder

        return LSTMEncoder(self)


@dataclass
class IdentityConfig(ModelConfig):
    """Configuration for an identity encoder."""

    @check_framework
    def build(self, framework: str = "torch"):
        return Identity(self)
