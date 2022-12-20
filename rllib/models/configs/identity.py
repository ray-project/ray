from dataclasses import dataclass

from ray.rllib.core.rl_module.encoder import EncoderConfig
from rllib.models.torch.identity import Identity
from ray.rllib.models.specs.specs_dict import ModelSpec


@dataclass
class IdentityConfig(EncoderConfig):
    """Configuration for an identity encoder."""

    def build(self, input_spec: ModelSpec) -> Identity:
        return Identity(input_spec=input_spec, config=self)
