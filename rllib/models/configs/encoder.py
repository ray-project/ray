import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple

from ray.rllib.models.specs.specs_dict import ModelSpec
from ray.rllib.models.torch.encoders.vector import TorchVectorEncoder

if TYPE_CHECKING:
    from ray.rllib.models.torch.encoders.vector import Encoder


@dataclass
class EncoderConfig:
    """The base config for encoder models.

    Each config should define a `build` method that builds a model from the config.

    All user-configurable parameters known before runtime
    (e.g. framework, activation, num layers, etc.) should be defined as attributes.

    Parameters unknown before runtime (e.g. the output size of the module providing
    input for this module) should be passed as arguments to `build`. This should be
    as few params as possible.

    `build` should return an instance of the encoder associated with the config.

    Attributes:
        framework_str: The tensor framework to construct a model for.
            This can be 'torch', 'tf2', or 'jax'.
    """

    framework_str: str = "torch"

    @abc.abstractmethod
    def build(self, input_spec: ModelSpec, **kwargs) -> "Encoder":
        """Builds the EncoderConfig into an Encoder instance"""


@dataclass
class VectorEncoderConfig(EncoderConfig):
    """An MLP encoder mappings tensors with shape [..., feature] to [..., output].

    Attributes:
        activation: The type of activation function to use between hidden layers.
            Options are 'relu', 'swish', 'tanh', or 'linear'
        final_activation: The activation function to use after the final linear layer.
            Options are the same as for activation.
        hidden_layer_sizes: A list, where each element represents the number of neurons
            in that layer. For example, [128, 64] would produce a two-layer MLP with
            128 hidden neurons and 64 hidden neurons.
        output_key: Write the output of the encoder to this key in the NestedDict.
    """

    activation: str = "relu"
    final_activation: str = "linear"
    hidden_layer_sizes: Tuple[int, ...] = (128, 128)
    output_key: str = "encoding"

    def build(self, input_spec: ModelSpec) -> TorchVectorEncoder:
        """Build the config into a VectorEncoder model instance.

        Args:
            input_spec: The output spec of the previous module(s) that will feed
                inputs to this encoder.

        Returns:
            A VectorEncoder of the specified framework.
        """
        assert (
            len(self.hidden_layer_sizes) > 1
        ), "Must have at least a single hidden layer"
        for k in input_spec.shallow_keys():
            assert isinstance(
                input_spec[k].shape[-1], int
            ), "Input spec {k} does not define the size of the feature (last) dimension"

        if self.framework_str == "torch":
            return TorchVectorEncoder(input_spec, self)
        else:
            raise NotImplementedError(
                "{self.__class__.__name__} not implemented"
                " for framework {self.framework}"
            )
