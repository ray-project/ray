import abc

from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.utils.typing import TensorType
from ray.rllib.models.experimental.base import Model, ForwardOutputType

STATE_IN: str = "state_in"
STATE_OUT: str = "state_out"


class Encoder(Model):
    """The framework-agnostic base class for all encoders RLlib produces.

    Encoders are used to encode observations into a latent space in RLModules.
    Therefore, their input_spec contains the observation space dimensions.
    Similarly, their output_spec usually the latent space dimensions.
    Encoders can be recurrent, in which case they should also have state_specs.

    Encoders encode observations into a latent space that serve as input to heads.
    Outputs of encoders are generally of shape (B, latent_dim) or (B, T, latent_dim).
    That is, for time-series data, we encode into the latent space for each time step.
    This should be reflected in the output_spec.
    """

    def get_initial_state(self) -> TensorType:
        """Returns the initial state of the encoder.

        It can be left empty if this encoder is not stateful.

        Examples:
            >>> encoder = Encoder(...)
            >>> state = encoder.get_initial_state()
            >>> out = encoder.forward({"obs": ..., STATE_IN: state})
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
            # This is abstract, see the framework implementations
            >>> out = encoder.forward({"obs": np.arange(10)}))
        """
        raise NotImplementedError
