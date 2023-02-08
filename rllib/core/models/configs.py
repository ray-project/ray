from dataclasses import dataclass, field
from typing import List, Callable
import functools

from ray.rllib.core.models.base import ModelConfig, Model
from ray.rllib.core.models.encoder import Encoder
from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
def _maybe_fit_activation_fn_to_tf(activation_fn: str):
    """Maybe fit the given activation function to reflect tf."""
    if activation_fn == "tanh":
        activation_fn = "Tanh"
    elif activation_fn == "relu":
        activation_fn = "ReLU"
    elif activation_fn == "linear":
        activation_fn = "linear"
    return activation_fn


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
                raise ValueError(
                    f"This config does not support framework "
                    f"{framework}. Only frameworks in {accepted} are "
                    f"supported."
                )
            return fn(self, framework, **kwargs)

        return checked_build

    return decorator


@dataclass
class MLPModelConfig(ModelConfig):
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
    hidden_layer_activation: str = "relu"
    output_activation: str = "linear"

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.core.models.torch.mlp import TorchMLPModel

            return TorchMLPModel(self)
        else:
            from ray.rllib.core.models.tf.mlp import TfMLPModel

            self.output_activation = _maybe_fit_activation_fn_to_tf(
                self.output_activation
            )
            self.hidden_layer_activation = _maybe_fit_activation_fn_to_tf(
                self.hidden_layer_activation
            )
            return TfMLPModel(self)


@dataclass
class MLPEncoderConfig(MLPModelConfig):
    @_framework_implemented()
    def build(self, framework: str = "torch") -> Encoder:
        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import TorchMLPEncoder

            return TorchMLPEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfMLPEncoder

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
            from ray.rllib.core.models.torch.encoder import TorchLSTMEncoder

            return TorchLSTMEncoder(self)


@dataclass
class IdentityConfig(ModelConfig):
    """Configuration for an identity encoder."""

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import TorchIdentityEncoder

            return TorchIdentityEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfIdentityEncoder

            return TfIdentityEncoder(self)


@dataclass
class ActorCriticEncoderConfig(ModelConfig):
    """Configuration for an actor-critic encoder."""

    base_encoder_config: ModelConfig = None
    shared: bool = True

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import (
                TorchActorCriticEncoder,
            )

            return TorchActorCriticEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfActorCriticEncoder

            return TfActorCriticEncoder(self)
