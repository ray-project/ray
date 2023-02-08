import abc

from ray.rllib.core.models.base import Model, ModelConfig
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.policy.sample_batch import SampleBatch

# Top level keys that unify model i/o.
STATE_IN: str = "state_in"
STATE_OUT: str = "state_out"
ENCODER_OUT: str = "encoder_out"
# For Actor-Critic algorithms, these signify data related to the actor and critic
ACTOR: str = "actor"
CRITIC: str = "critic"


class Encoder(Model, abc.ABC):
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

    def __init__(self, config: ModelConfig):
        super().__init__(config)
        self.input_spec = SpecDict({SampleBatch.OBS: None, STATE_IN: None})
        self.output_spec = [ENCODER_OUT, STATE_OUT]
