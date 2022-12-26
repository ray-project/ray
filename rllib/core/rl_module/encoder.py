import torch
import torch.nn as nn
import tree
from typing import List

from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from dataclasses import dataclass, field

from ray.rllib.utils.annotations import override
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.primitives import FCNet
from ray.rllib.models.base_model import Model, BaseModelIOKeys


@dataclass
class EncoderConfig:
    """Configuration for an encoder network.

    Attributes:
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
    """

    output_dim: int = None


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
        return SpecDict()

    @property
    def output_spec(self):
        return SpecDict()


class FullyConnectedEncoder(Encoder):
    def __init__(self, config: FCConfig) -> None:
        super().__init__(config)

        self.net = FCNet(
            input_dim=config.input_dim,
            hidden_layers=config.hidden_layers,
            output_dim=config.output_dim,
            activation=config.activation,
        )

    @property
    @override(Model)
    def input_spec(self):
        return SpecDict(
            {
                self.io[BaseModelIOKeys.IN]: TorchTensorSpec(
                    "b, h", h=self.config.input_dim
                )
            }
        )

    @property
    @override(Model)
    def output_spec(self):
        return SpecDict(
            {
                self.io[BaseModelIOKeys.OUT]: TorchTensorSpec(
                    "b, h", h=self.config.output_dim
                )
            }
        )

    @check_input_specs("input_spec")
    @check_output_specs("output_spec")
    def _forward(self, input_dict, **kwargs):
        inputs = input_dict[self.io[BaseModelIOKeys.IN]]
        return {self.io[BaseModelIOKeys.OUT]: self.net(inputs)}


class RecurrentEncoder(Encoder):
    def __init__(self, config: EncoderConfig):
        super().__init__(config=config)


class LSTMEncoder(RecurrentEncoder):
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
    @override(Model)
    def input_spec(self):
        config = self.config
        return SpecDict(
            {
                # bxt is just a name for better readability to indicated padded batch
                self.io[BaseModelIOKeys.IN]: TorchTensorSpec(
                    "bxt, h", h=config.input_dim
                ),
                self.io[BaseModelIOKeys.STATE_IN]: {
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
    @override(Model)
    def output_spec(self):
        config = self.config
        return SpecDict(
            {
                self.io[BaseModelIOKeys.OUT]: TorchTensorSpec(
                    "bxt, h", h=config.output_dim
                ),
                self.io[BaseModelIOKeys.STATE_OUT]: {
                    "h": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                },
            }
        )

    @check_input_specs("input_spec")
    @check_output_specs("output_spec")
    def _forward(self, input_dict: SampleBatch, **kwargs):
        x = input_dict[self.io[BaseModelIOKeys.IN]]
        states = input_dict[self.io[BaseModelIOKeys.STATE_IN]]
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
            self.io[BaseModelIOKeys.OUT]: x,
            self.io[BaseModelIOKeys.STATE_OUT]: tree.map_structure(
                lambda x: x.transpose(0, 1), states_o
            ),
        }


class IdentityEncoder(Encoder):
    def __init__(self, config: EncoderConfig) -> None:
        super().__init__(config=config)

    @check_input_specs("input_spec")
    @check_output_specs("output_spec")
    def _forward(self, input_dict, **kwargs):
        return {self.io[BaseModelIOKeys.OUT]: input_dict[self.io[BaseModelIOKeys.IN]]}
