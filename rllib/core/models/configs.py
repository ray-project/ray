from dataclasses import dataclass, field
from typing import List, Callable
import functools

from ray.rllib.core.models.base import ModelConfig, Model
from ray.rllib.core.models.encoder import Encoder
from ray.rllib.utils.annotations import ExperimentalAPI


@ExperimentalAPI
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


@ExperimentalAPI
@dataclass
class MLPModelConfig(ModelConfig):
    """Configuration for a fully connected network.

    See ModelConfig for usage details.

    Atributes:
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

            # Activation functions in TF are lower case
            self.output_activation = self.output_activation.lower()
            self.hidden_layer_activation = self.hidden_layer_activation.lower()

            return TfMLPModel(self)


@ExperimentalAPI
@dataclass
class MLPEncoderConfig(MLPModelConfig):
    """Configuration for an MLP that acts as an encoder.

    See ModelConfig for usage details.
    """

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Encoder:
        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import TorchMLPEncoder

            return TorchMLPEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfMLPEncoder

            # Activation functions in TF are lower case
            self.output_activation = self.output_activation.lower()
            self.hidden_layer_activation = self.hidden_layer_activation.lower()

            return TfMLPEncoder(self)


@ExperimentalAPI
@dataclass
class LSTMEncoderConfig(ModelConfig):
    """Configuration for a LSTM encoder.

    See ModelConfig for usage details.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_dim: The size of the hidden layer.
        num_layers: The number of LSTM layers.
        batch_first: Wether the input is batch first or not.
        output_activation: The activation function to use for the output layer.
    """

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


@ExperimentalAPI
@dataclass
class IdentityConfig(ModelConfig):
    """Configuration for an IdentityEncoder

    This creates a dummy encoder that does not transform the input but can be used as a
    pass-through to heads. See ModelConfig for usage details.
    """

    @_framework_implemented()
    def build(self, framework: str = "torch") -> Model:
        if framework == "torch":
            from ray.rllib.core.models.torch.encoder import TorchIdentityEncoder

            return TorchIdentityEncoder(self)
        else:
            from ray.rllib.core.models.tf.encoder import TfIdentityEncoder

            return TfIdentityEncoder(self)


@ExperimentalAPI
@dataclass
class ActorCriticEncoderConfig(ModelConfig):
    """Configuration for an ActorCriticEncoder.

    The base encoder functions like other encoders in RLlib. It is wrapped by the
    ActorCriticEncoder to provides a shared encoder Model to use in RLModules that
    provides twofold outputs: one for the actor and one for the critic. See
    ModelConfig for usage details.

    Attributes:
        base_encoder_config: The configuration for the base encoder.
        shared: Whether the base encoder is shared between the actor and critic.
    """

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
