from dataclasses import dataclass
import abc

from ray.rllib.models.specs.checker import (
    check_input_specs,
    check_output_specs,
)
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.utils.typing import TensorType
from ray.rllib.utils.annotations import ExperimentalAPI

ForwardOutputType = TensorDict
STATE_IN: str = "state_in"
STATE_OUT: str = "state_out"


@ExperimentalAPI
@dataclass
class ModelConfig(abc.ABC):
    """Configuration for a model.

    Attributes:
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
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
    """Base class for RLlib models."""

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

        Outputs and inputs should be subjected to spec checking.

        Args:
            inputs: A TensorDict containing model inputs
            kwargs: For forwards compatibility

        Returns:
            outputs: A TensorDict containing model outputs

        Examples:
            # This is abstract, see the torch/tf/jax implementations
            >>> out = model(TensorDict({"in": np.arange(10)}))
            >>> out # TensorDict(...)
        """
        raise NotImplementedError


class Encoder(Model):
    """The base class for all encoders Rllib produces.

    Encoders are used to encode observations into a latent space in RLModules.
    Therefore, their input_spec usually contains the observation space dimensions.
    Their output_spec usually contains the latent space dimensions.
    Encoders can be recurrent, in which case they should also have state_specs.
    """

    def __init__(self, config: dict):
        super().__init__(config)

    def get_initial_state(self) -> TensorType:
        """Returns the initial state of the encoder.

        This is the initial state of the encoder.
        It can be left empty if this encoder is not stateful.

        Examples:
            >>> ...
        """
        return {}

    @check_input_specs("input_spec", cache=True)
    @check_output_specs("output_spec", cache=True)
    @abc.abstractmethod
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        """Computes the output of this module for each timestep.

        Outputs and inputs are subjected to spec checking.

        Args:
            inputs: A TensorDict containing model inputs
            kwargs: For forwards compatibility

        Returns:
            outputs: A TensorDict containing model outputs

        Examples:
            # This is abstract, see the torch/tf/jax implementations
            >>> out = model(TensorDict({"in": np.arange(10)}))
            >>> out # TensorDict(...)
        """
        raise NotImplementedError
