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

    ModelConfigs are framework-agnostic.
    A ModelConfig is usually built by RLModules by querying it form a Catalog object.
    It is therefore a means of configuration for RLModules.
    However, that is not a limitation, and they can be built directly as well.

    Attributes:
        output_dim: The output dimension of the network.

    Usage Example together with Model:

    >>> class MyModel(Model):
    ...     def __init__(self, config):
    ...         super().__init__(config)
    ...         self.my_param = config.my_param * 2
    >>>
    >>> def _forward(self, input_dict):
    ...     return input_dict["obs"] * self.my_param
    >>>
    >>> class MyModelConfig(ModelConfig):
    ...     my_parameter: int = 42
    ...
    ...     @classmethod
    ...     def build(self, framework: str):
    ...         if framework == "bork":
    ...             return MyModel(self)
    """

    output_dim: int = None

    @abc.abstractmethod
    def build(self, framework: str):
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

    Usage Example together with ModelConfig:

    >>> class MyModel(Model):
    ...     def __init__(self, config):
    ...         super().__init__(config)
    ...         self.my_param = config.my_param * 2
    >>>
    >>> def _forward(self, input_dict):
    ...     return input_dict["obs"] * self.my_param
    >>>
    >>>
    >>> class MyModelConfig(ModelConfig):
    ...     my_parameter: int = 42
    ...
    ...     @classmethod
    ...     def build(self, framework: str):
    ...         if framework == "bork":
    ...             return MyModel(self)

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

        This method is called by the forwarding method of the respective framework
        that is itself wrapped by RLLib in order to check model inputs and outputs.

        Args:
            input_dict: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        raise NotImplementedError
