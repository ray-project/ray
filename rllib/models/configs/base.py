import abc
from dataclasses import dataclass

from ray.rllib.models.specs.specs_dict import ModelSpec


@dataclass
class ModelConfig:
    """The base config for models.

    Each config should define a `build` method that builds a model from the config.

    All user-configurable parameters known before runtime
    (e.g. framework, activation, num layers, etc.) should be defined as attributes.

    Parameters unknown before runtime (e.g. the output size of the module providing
    input for this module) should be passed as arguments to `build`. This should be
    as few params as possible.

    `build` should return an instance of the model associated with the config.

    Attributes:
        framework_str: The tensor framework to construct a model for.
            This can be 'torch', 'tf2', or 'jax'.
    """

    framework_str: str = "torch"

    @abc.abstractmethod
    def build(self, input_spec: ModelSpec, **kwargs) -> "Encoder":
        """Builds the ModelConfig into an Encoder instance"""
