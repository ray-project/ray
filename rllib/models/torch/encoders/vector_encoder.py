from ray.rllib.models.torch.model import TorchModel
from ray.rllib.models.base_encoder import Encoder
from ray.rllib.models.temp_spec_classes import ModelConfig, TensorDict
from ray.rllib.models.specs.specs_dict import ModelSpecDict
from ray.rllib.models.base_model import ForwardOutputType

from torch import nn

from ray.rllib.utils.annotations import (
    override,
)


class VectorEncoder(TorchModel, Encoder):
    """A basic MLP encoder that maps a batch
    of inputs to a batch of latent features"""

    # TODO: Do we want to use a ModelConfig or create a new
    # EncoderConfig?
    def __init__(self, config: ModelConfig):
        self.mlp = nn.Sequential(
            nn.Linear(
                config.encoder_config.input_size, config.encoder_config.hidden_size
            ),
            nn.ReLU(),
            nn.Linear(
                config.encoder_config.hidden_size, config.encoder_config.hidden_size
            ),
            nn.ReLU(),
            nn.Linear(
                config.encoder_config.hidden_size, config.encoder_config.hidden_size
            ),
        )

    @override(TorchModel)
    @property
    def input_spec(self):
        return ModelSpecDict({"batch, obs"}, hidden=self.config.input_size)

    @override(TorchModel)
    @property
    def output_spec(self):
        # TODO: This is named hidden in the doc, but I think we should probably
        # call it output to be consistent
        return ModelSpecDict({"batch, hidden"}, hidden=self.config.hidden_size)

    @override(TorchModel)
    def _forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        # TODO: Maybe we should use something besides 'obs' so we can chain
        # encoders together. We would need the same key for input and output
        # e.g. input=TensorDict({"features:..."}), output=TensorDict({"features:..."})
        output = self.mlp(inputs["obs"])
        return TensorDict({"encoder_output": output})
