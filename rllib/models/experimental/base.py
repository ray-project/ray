import abc
from dataclasses import dataclass
from typing import List, Union

from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.nested_dict import NestedDict


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


class Model(abc.ABC):
    """Framework-agnostic base class for RLlib models.

    Models are low-level neural network components that offer input- and
    output-specification, a forward method, and a get_initial_state method. Models
    are composed in RLModules.
    """

    def __init__(self, config: ModelConfig):
        self.config = config

    def get_initial_state(self) -> Union[NestedDict, List[TensorType]]:
        """Returns the initial state of the Model.

        It can be left empty if this Model is not stateful.
        """
        return {}

    @abc.abstractmethod
    def _forward(self, input_dict: NestedDict, **kwargs) -> NestedDict:
        """Returns the output of this model for the given input.

        Implement this method to write your own Model in Rllib.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        raise NotImplementedError
