from typing import Union

import tree

from ray.rllib.core.models.base import (
    Encoder,
    ActorCriticEncoder,
    STATE_IN,
    STATE_OUT,
    ENCODER_OUT,
)
from ray.rllib.core.models.base import ModelConfig, Model
from ray.rllib.core.models.tf.primitives import TfMLP
from ray.rllib.core.models.tf.primitives import TfModel
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

torch, nn = try_import_torch()


class TfMLPEncoder(Encoder, TfModel):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        self.net = TfMLP(
            input_dim=config.input_dim,
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dim,
            hidden_layer_activation=config.hidden_layer_activation,
        )

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                SampleBatch.OBS: TFTensorSpecs("b, h", h=self.config.input_dim),
                STATE_IN: None,
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                ENCODER_OUT: TFTensorSpecs("b, h", h=self.config.output_dim),
                STATE_OUT: None,
            }
        )

    @override(Model)
    def _forward(self, inputs: NestedDict) -> NestedDict:
        return NestedDict(
            {
                ENCODER_OUT: self.net(inputs[SampleBatch.OBS]),
                STATE_OUT: inputs[STATE_IN],
            }
        )


class LSTMEncoder(Encoder, TfModel):
    """An encoder that uses an LSTM cell and a linear layer."""

    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)
        Encoder.__init__(self, config)

        # Create the neural networks
        self.lstm = nn.LSTM(
            config.input_dim,
            config.hidden_dim,
            config.num_layers,
            batch_first=config.batch_first,
        )
        self.linear = nn.Linear(config.hidden_dim, config.output_dim)

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                # bxt is just a name for better readability to indicated padded batch
                SampleBatch.OBS: TFTensorSpecs("bxt, h", h=self.config.input_dim),
                STATE_IN: {
                    "h": TFTensorSpecs(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                    "c": TFTensorSpecs(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                },
                SampleBatch.SEQ_LENS: None,
            }
        )

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return SpecDict(
            {
                ENCODER_OUT: TFTensorSpecs("bxt, h", h=self.config.output_dim),
                STATE_OUT: {
                    "h": TFTensorSpecs(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                    "c": TFTensorSpecs(
                        "b, l, h", h=self.config.hidden_dim, l=self.config.num_layers
                    ),
                },
            }
        )

    @override(Model)
    def get_initial_state(self):
        config = self.config
        return {
            "h": torch.zeros(config.num_layers, config.hidden_dim),
            "c": torch.zeros(config.num_layers, config.hidden_dim),
        }

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        x = inputs[SampleBatch.OBS]
        states = inputs[STATE_IN]
        # states are batch-first when coming in
        states = tree.map_structure(lambda x: x.transpose(0, 1), states)

        x = add_time_dimension(
            x,
            seq_lens=inputs[SampleBatch.SEQ_LENS],
            framework="tf",
            time_major=not self.config.batch_first,
        )
        states_o = {}
        x, (states_o["h"], states_o["c"]) = self.lstm(x, (states["h"], states["c"]))

        x = self.linear(x)
        x = x.view(-1, x.shape[-1])

        return NestedDict(
            {
                ENCODER_OUT: x,
                STATE_OUT: tree.map_structure(lambda x: x.transpose(0, 1), states_o),
            }
        )


class TfActorCriticEncoder(TfModel, ActorCriticEncoder):
    """An encoder that can hold two encoders."""

    framework = "tf"

    def __init__(self, config: ModelConfig) -> None:
        ActorCriticEncoder.__init__(self, config)
        TfModel.__init__(self, config)
