import torch
import torch.nn as nn
import tree

from ray.rllib.models.experimental.base import (
    ForwardOutputType,
    ModelConfig,
)
from ray.rllib.models.experimental.encoder import (
    Encoder,
    STATE_IN,
    STATE_OUT,
)
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.experimental.torch.primitives import TorchMLP, TorchModel

ENCODER_OUT: str = "encoder_out"


class TorchMLPEncoder(TorchModel, Encoder):
    """A fully connected encoder."""

    def __init__(self, config: ModelConfig) -> None:
        TorchModel.__init__(self, config)
        Encoder.__init__(self, config)

        self.net = TorchMLP(
            input_dim=config.input_dim,
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dim,
            hidden_layer_activation=config.hidden_layer_activation,
        )

    @property
    @override(TorchModel)
    def input_spec(self) -> SpecDict:
        return SpecDict(
            {SampleBatch.OBS: TorchTensorSpec("b, h", h=self.config.input_dim)}
        )

    @property
    @override(TorchModel)
    def output_spec(self) -> SpecDict:
        return SpecDict(
            {ENCODER_OUT: TorchTensorSpec("b, h", h=self.config.output_dim)}
        )

    @check_input_specs("input_spec", cache=False)
    @check_output_specs("output_spec", cache=False)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return {ENCODER_OUT: self.net(inputs[SampleBatch.OBS])}


class TorchLSTMEncoder(TorchModel, Encoder):
    """An encoder that uses an LSTM cell and a linear layer."""

    def __init__(self, config: ModelConfig) -> None:
        TorchModel.__init__(self, config)

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
    @override(TorchModel)
    def input_spec(self) -> SpecDict:
        config = self.config
        return SpecDict(
            {
                # bxt is just a name for better readability to indicated padded batch
                SampleBatch.OBS: TorchTensorSpec("bxt, h", h=config.input_dim),
                STATE_IN: {
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
    @override(TorchModel)
    def output_spec(self) -> SpecDict:
        config = self.config
        return SpecDict(
            {
                ENCODER_OUT: TorchTensorSpec("bxt, h", h=config.output_dim),
                STATE_OUT: {
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
            ENCODER_OUT: x,
            STATE_OUT: tree.map_structure(lambda x: x.transpose(0, 1), states_o),
        }


class TorchIdentityEncoder(TorchModel):
    """An encoder that does nothing but passing on inputs.

    We use this so that we avoid having many if/else statements in the RLModule.
    """

    @property
    def input_spec(self) -> SpecDict:
        return SpecDict(
            # Use the output dim as input dim because identity.
            {SampleBatch.OBS: TorchTensorSpec("b, h", h=self.config.output_dim)}
        )

    @property
    def output_spec(self) -> SpecDict:
        return SpecDict(
            {ENCODER_OUT: TorchTensorSpec("b, h", h=self.config.output_dim)}
        )

    @check_input_specs("input_spec", cache=False)
    @check_output_specs("output_spec", cache=False)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return {ENCODER_OUT: inputs[SampleBatch.OBS]}
