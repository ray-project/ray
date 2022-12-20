from ray.rllib.core.rl_module.encoder import Encoder, EncoderConfig
from ray.rllib.models.specs.specs_dict import ModelSpec


class Identity(Encoder):
    """Implements the identity function."""

    def __init__(self, input_spec: ModelSpec, config: EncoderConfig) -> None:
        self._input_spec = input_spec
        super().__init__(config)

    @property
    def input_spec(self) -> ModelSpec:
        return self._input_spec

    @property
    def output_spec(self) -> ModelSpec:
        return self._input_spec

    def _forward(self, input_dict) -> dict:
        return input_dict
