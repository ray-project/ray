from dataclasses import dataclass, field
from typing import List, Callable
import functools

from ray.rllib.models.experimental.base import ModelConfig, Model
from ray.rllib.models.experimental.encoder import Encoder
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
def _framework_implemented(torch: bool = True, tf2: bool = True):
    """Decorator to check if a model was implemented in a framework.

    Args:
        torch: Whether we can build this model with torch.
        tf2: Whether we can build this model with tf2.

    Returns:
        The decorated function.

    Raises:
        ValueError: If the framework is not available to build.
    """
    accepted = []
    if torch:
        accepted.append("torch")
    if tf2:
        accepted.append("tf")
        accepted.append("tf2")

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def checked_build(self, framework, **kwargs):
            if framework not in accepted:
                raise ValueError(f"Framework {framework} not supported.")
            return fn(self, framework, **kwargs)

        return checked_build

    return decorator


@dataclass
class MLPConfig(ModelConfig):
    """Configuration for a fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layer_dims: The sizes of the hidden layers.
        hidden_layer_activation: The activation function to use after each layer (
        except for the output).
        output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    hidden_layer_dims: List[int] = field(default_factory=lambda: [256, 256])
    hidden_layer_activation: str = "ReLU"
    output_activation: str = "linear"

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.models.experimental.torch.mlp import TorchMLPModel

            return TorchMLPModel(self)
        else:
            from ray.rllib.models.experimental.tf.mlp import TfMLPModel

            return TfMLPModel(self)


@dataclass
class MLPEncoderConfig(MLPConfig):
    @_framework_implemented()
    def build(self, framework: str = "torch") -> Encoder:
        if framework == "torch":
            from ray.rllib.models.experimental.torch.encoder import TorchMLPEncoder

            return TorchMLPEncoder(self)
        else:
            from ray.rllib.models.experimental.tf.encoder import TfMLPEncoder

            return TfMLPEncoder(self)


@dataclass
class LSTMEncoderConfig(ModelConfig):
    input_dim: int = None
    hidden_dim: int = None
    num_layers: int = None
    batch_first: bool = True
    output_activation: str = "linear"

    @_framework_implemented(tf2=False)
    def build(self, framework: str = "torch") -> Encoder:
        if framework == "torch":
            from ray.rllib.models.experimental.torch.encoder import TorchLSTMEncoder

            return TorchLSTMEncoder(self)


@dataclass
class IdentityConfig(ModelConfig):
    """Configuration for an identity encoder."""

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.models.experimental.torch.encoder import TorchIdentityEncoder

            return TorchIdentityEncoder(self)
        else:
            from ray.rllib.models.experimental.tf.encoder import TfIdentityEncoder

            return TfIdentityEncoder(self)
