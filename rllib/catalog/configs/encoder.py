import abc
from dataclasses import dataclass
from typing import TYPE_CHECKING, List
from rllib.catalog.torch.encoders.vector import TorchVectorEncoder
from rllib.models.specs.specs_dict import ModelSpec

if TYPE_CHECKING:
    from rllib.catalog.torch.encoders.vector import Encoder


@dataclass(frozen=True)
class EncoderConfig:
    """The base config for encoder models. Each config should define a `build` method
    that builds a model from the config.

    All parameters known before runtime (e.g. framework, activation, num layers, etc.)
    should be defined as attributes.

    Parameters unknown before runtime (e.g. the output size of the module providing
    input for this model) should be passed as arguments to `build`. This should be
    as few params as possible.

    `build` should return an instance of the encoder associated with the config.

    Attributes:
        framework: The tensor framework to construct a model for.
            This can be 'torch', 'tf2', or 'jax'.
    """

    framework: str = "torch"

    @abc.abstractmethod
    def build(self, input_spec: ModelSpec, **kwargs) -> "Encoder":
        """Builds the config into a model instance"""


@dataclass(frozen=True)
class VectorEncoderConfig(EncoderConfig):
    """A basic MLP encoder.

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
    hidden_layer_sizes: List[int] = [128, 128]
    output_key: str = "encoding"

    def build(self, input_spec: ModelSpec) -> TorchVectorEncoder:
        assert (
            len(self.hidden_layer_sizes) > 1
        ), "Must have at least a single hidden layer"
        # TODO: This requires modification/implementation in specs_base.py
        # Do spec surgery:
        #   input_spec == {"bork": (a, b, c, d), d=3}
        # should produce:
        #   output_spec == {"encoding": (a, b, c, d), d=self.hidden_layer_sizes[-1]}
        output_spec = ModelSpec.output_from_input(
            input_spec,
            out_key=self.output_key,
            feature_size=self.hidden_layer_sizes[-1],
        )
        for k in input_spec.shallow_keys():
            assert isinstance(
                input_spec[k].shape()[-1], int
            ), "All input specs must define the size of the feature (last) dimension"
        if self.framework == "torch":
            return TorchVectorEncoder(input_spec, output_spec, self)
        else:
            raise NotImplementedError(
                "{self.__class__.__name__} not implemented"
                " for framework {self.framework}"
            )
