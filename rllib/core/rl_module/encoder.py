import torch
import torch.nn as nn
import tree

from dataclasses import dataclass

from ray.rllib.models.base_model import (
    ModelConfig,
    Model,
    STATE_IN,
    STATE_OUT,
    ForwardOutputType,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.core.rl_module.fc import FC, FCConfig


ENCODER_OUT: str = "encoder_out"


@dataclass
class FCEncoderConfig(FCConfig):
    def build(self):
        return FCEncoder(self)


class FCEncoder(FC):
    @property
    def input_spec(self):
        return SpecDict(
            {SampleBatch.OBS: TorchTensorSpec("b, h", h=self.config.input_dim)}
        )

    @property
    def output_spec(self):
        return SpecDict(
            {ENCODER_OUT: TorchTensorSpec("b, h", h=self.config.output_dim)}
        )

    @check_input_specs("input_spec", filter=True, cache=False)
    @check_output_specs("output_spec", cache=False)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


@dataclass
class LSTMEncoderConfig(ModelConfig):
    input_dim: int = None
    hidden_dim: int = None
    num_layers: int = None
    batch_first: bool = True

    def build(self):
        return LSTMEncoder(self)


class LSTMEncoder(Model, nn.Module):
    def __init__(self, config: LSTMEncoderConfig) -> None:
        nn.Module.__init__(self)
        Model.__init__(self, config)

        self.lstm = nn.LSTM(
            config.input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_first,
        )
        self.linear = nn.Linear(config.hidden_dim, config.output_dim)

    def get_initial_state(self):
        config = self.config
        return {
            "h": torch.zeros(config.num_layers, config.hidden_dim),
            "c": torch.zeros(config.num_layers, config.hidden_dim),
        }

    @property
    def input_spec(self):
        config = self.config
        return SpecDict(
            {
                # bxt is just a name for better readability to indicated padded batch
                self.config.input_key: TorchTensorSpec("bxt, h", h=config.input_dim),
                self.config.state_in_key: {
                    "h": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                },
                SampleBatch.SEQ_LENS: None,
            }
        )

    @property
    def output_spec(self):
        config = self.config
        return SpecDict(
            {
                self.config.output_key: TorchTensorSpec("bxt, h", h=config.output_dim),
                self.config.state_out_key: {
                    "h": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                },
            }
        )

    @check_input_specs("input_spec", filter=True, cache=False)
    @check_output_specs("output_spec", cache=False)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        x = inputs[SampleBatch.OBS]
        states = inputs[STATE_IN]
        # states are batch-first when coming in
        states = tree.map_structure(lambda x: x.transpose(0, 1), states)

        x = add_time_dimension(
            x,
            seq_lens=inputs[SampleBatch.SEQ_LENS],
            framework="torch",
            time_major=not self.config.batch_first,
        )
        states_o = {}
        x, (states_o["h"], states_o["c"]) = self.lstm(x, (states["h"], states["c"]))

        x = self.linear(x)
        x = x.view(-1, x.shape[-1])

        return {
            self.config.output_key: x,
            self.config.state_out_key: tree.map_structure(
                lambda x: x.transpose(0, 1), states_o
            ),
        }


@dataclass
class IdentityConfig(ModelConfig):
    """Configuration for an identity encoder."""

    def build(self):
        return IdentityEncoder(self)


class IdentityEncoder(Model):
    def _forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        pass

    def __init__(self, config: IdentityConfig) -> None:
        super().__init__(config)

    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return inputs
