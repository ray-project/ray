import torch
import torch.nn as nn
import tree

from ray.rllib.models.base_model import (
    Model,
    STATE_IN,
    STATE_OUT,
    ForwardOutputType,
    ModelConfig,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.core.rl_module.tf.fcmodel import FCModel
from ray.rllib.core.rl_module.torch.encoder import ENCODER_OUT


class FCEncoder(FCModel):
    @property
    def input_spec(self):
        return SpecDict(
            {SampleBatch.OBS: TFTensorSpecs("b, h", h=self.config.input_dim)}
        )

    @property
    def output_spec(self):
        return SpecDict({ENCODER_OUT: TFTensorSpecs("b, h", h=self.config.output_dim)})

    @check_input_specs("input_spec", filter=True, cache=False)
    @check_output_specs("output_spec", cache=False)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


class LSTMEncoder(Model, nn.Module):
    def __init__(self, config: ModelConfig) -> None:
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
                SampleBatch.OBS: TFTensorSpecs("bxt, h", h=config.input_dim),
                STATE_IN: {
                    "h": TFTensorSpecs(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TFTensorSpecs(
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
                ENCODER_OUT: TFTensorSpecs("bxt, h", h=config.output_dim),
                STATE_OUT: {
                    "h": TFTensorSpecs(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TFTensorSpecs(
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
            ENCODER_OUT: x,
            STATE_OUT: tree.map_structure(lambda x: x.transpose(0, 1), states_o),
        }


class Identity(Model):
    def _forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        pass

    def __init__(self, config: ModelConfig) -> None:
        super().__init__(config)

    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return inputs
