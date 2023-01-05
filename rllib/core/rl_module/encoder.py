import torch
import torch.nn as nn
import tree
from typing import List

from dataclasses import dataclass, field

from ray.rllib.models.base_model import Model
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.primitives import FCNet

# TODO (Kourosh): Find a better / more straight fwd approach for sub-components

ENCODER_OUT = "encoder_out"
STATE_IN = "state_in_0"
STATE_OUT = "state_out_0"


@dataclass
class EncoderConfig:
    """Configuration for an encoder network.

    Attributes:
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
    """

    output_dim: int = None
    input_key: str = None
    output_key: str = None


@dataclass
class IdentityConfig(EncoderConfig):
    """Configuration for an identity encoder."""

    def build(self):
        return IdentityEncoder(self)


@dataclass
class FCConfig(EncoderConfig):
    """Configuration for a fully connected network.
    input_dim: The input dimension of the network. It cannot be None.
    hidden_layers: The sizes of the hidden layers.
    activation: The activation function to use after each layer (except for the
        output).
    output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    hidden_layers: List[int] = field(default_factory=lambda: [256, 256])
    activation: str = "ReLU"

    def build(self):
        return FullyConnectedEncoder(self)


@dataclass
class LSTMConfig(EncoderConfig):
    input_dim: int = None
    hidden_dim: int = None
    num_layers: int = None
    batch_first: bool = True
    state_in_key: str = None
    state_out_key: str = None

    def build(self):
        return LSTMEncoder(self)


class Encoder(Model, nn.Module):
    def __init__(self, config: EncoderConfig) -> None:
        nn.Module.__init__(self)
        Model.__init__(self)
        self.config = config

    def get_initial_state(self):
        return []

    @property
    def input_spec(self):
        return SpecDict(
            {self.config.input_key: TorchTensorSpec("b, h", h=self.config.input_dim)}
        )

    @property
    def output_spec(self):
        return SpecDict(
            {self.config.output_key: TorchTensorSpec("b, h", h=self.config.output_dim)}
        )

    @check_input_specs("input_spec")
    @check_output_specs("output_spec")
    def forward(self, input_dict):
        return self._forward(input_dict)

    def _forward(self, input_dict):
        raise NotImplementedError


class FullyConnectedEncoder(Encoder):
    def __init__(self, config: FCConfig) -> None:
        super().__init__(config)

        self.net = FCNet(
            input_dim=config.input_dim,
            hidden_layers=config.hidden_layers,
            output_dim=config.output_dim,
            activation=config.activation,
        )

    def _forward(self, input_dict):
        return {self.config.output_key: self.net(input_dict[self.config.input_key])}


class LSTMEncoder(Encoder):
    def __init__(self, config: LSTMConfig) -> None:
        super().__init__(config)

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

    def _forward(self, input_dict: SampleBatch):
        x = input_dict[SampleBatch.OBS]
        states = input_dict[STATE_IN]
        # states are batch-first when coming in
        states = tree.map_structure(lambda x: x.transpose(0, 1), states)

        x = add_time_dimension(
            x,
            seq_lens=input_dict[SampleBatch.SEQ_LENS],
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


class IdentityEncoder(Encoder):
    def __init__(self, config: EncoderConfig) -> None:
        super().__init__(config)

    @property
    def input_spec(self):
        return SpecDict()

    @property
    def output_spec(self):
        return SpecDict()

    def _forward(self, input_dict):
        return input_dict
