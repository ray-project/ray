from dataclasses import dataclass
import abc

from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.utils.annotations import ExperimentalAPI

ForwardOutputType = TensorDict


@ExperimentalAPI
@dataclass
class ModelConfig(abc.ABC):
    """Base class for model configurations.

    Attributes:
        output_dim: The output dimension of the network.
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
    output-specification, a forward method, and a get_initial_state method. Models
    are composed in RLModules.
    """

    def __init__(self, config: ModelConfig):
        self.config = config

    @abc.abstractmethod
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
