from dataclasses import dataclass
import abc

from ray.rllib.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.utils.annotations import ExperimentalAPI

ForwardOutputType = TensorDict


@ExperimentalAPI
@dataclass
class ModelConfig(abc.ABC):
    """Configuration for a model.

    Attributes:
        output_dim: The output dimension of the network. If None, the output_dim will
            be the number of nodes in the last hidden layer.
    """

    output_dim: int = None

    @abc.abstractmethod
    def build(self, framework: str = "torch"):
        """Builds the model.

        Args:
            framework: The framework to use for building the model.
        """
        raise NotImplementedError


class Model:
    """Framework-agnostic base class for RLlib models.

    Models are low-level neural network components that offer input- and
    output-specification, a forward method, and a get_initial_state method. They are
    therefore not algorithm-specific. Models are composed in RLModules, where tensors
    are passed through them.
    """

    def __init__(self, config: ModelConfig):
        self.config = config

    def get_initial_state(self):
        """Returns the initial state of the model."""
        return {}

    @property
    @abc.abstractmethod
    def output_spec(self) -> SpecDict:
        """Returns the outputs spec of this model.

        This can include the state specs as well.

        Examples:
            >>> ...
        """
        # If no checking is needed, we can simply return an empty spec.
        return SpecDict()

    @property
    @abc.abstractmethod
    def input_spec(self) -> SpecDict:
        """Returns the input spec of this model.

        This can include the state specs as well.

        Examples:
            >>> ...
        """
        # If no checking is needed, we can simply return an empty spec.
        return SpecDict()

    @check_input_specs("input_spec", filter=True, cache=True)
    @check_output_specs("output_spec", cache=True)
    @abc.abstractmethod
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        """Computes the output of this module for each timestep.

        Outputs and inputs should be subject to spec checking.

        Args:
            inputs: A TensorDict containing model inputs
            kwargs: For forwards compatibility

        Examples:
            # This is abstract, see the torch/tf2/jax implementations
            >>> out = model(TensorDict({"in": np.arange(10)}))
            >>> out # TensorDict(...)
        """
        raise NotImplementedError
