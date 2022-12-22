import torch
import torch.nn as nn
import tree
from typing import List

from dataclasses import dataclass, field

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.models.specs.specs_dict import SpecDict
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.torch.primitives import FCNet

# TODO (Kourosh): Find a better / more straight fwd approach for sub-components


@dataclass
class EncoderConfig:
    """Configuration for an encoder network.

    Attributes:
        output_dim: The output dimension of the network. if None, the last layer would
            be the last hidden layer.
    """

    output_dim: int = None


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


class Encoder(nn.Module):
    def __init__(self, config: EncoderConfig) -> None:
        super().__init__()
        self.config = config
        self._input_spec = self.input_spec()
        self._output_spec = self.output_spec()

    def get_inital_state(self):
        raise []

    def input_spec(self):
        return SpecDict()

    def output_spec(self):
        return SpecDict()

    @check_input_specs("_input_spec")
    @check_output_specs("_output_spec")
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

    def input_spec(self):
        return SpecDict(
            {SampleBatch.OBS: TorchTensorSpec("b, h", h=self.config.input_dim)}
        )

    def output_spec(self):
        return SpecDict(
            {"embedding": TorchTensorSpec("b, h", h=self.config.output_dim)}
        )

    def _forward(self, input_dict):
        return {"embedding": self.net(input_dict[SampleBatch.OBS])}


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

    def get_inital_state(self):
        config = self.config
        return {
            "h": torch.zeros(config.num_layers, config.hidden_dim),
            "c": torch.zeros(config.num_layers, config.hidden_dim),
        }

    def input_spec(self):
        config = self.config
        return SpecDict(
            {
                # bxt is just a name for better readability to indicated padded batch
                SampleBatch.OBS: TorchTensorSpec("bxt, h", h=config.input_dim),
                "state_in": {
                    "h": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                    "c": TorchTensorSpec(
                        "b, l, h", h=config.hidden_dim, l=config.num_layers
                    ),
                },
            }
        )

    def output_spec(self):
        config = self.config
        return SpecDict(
            {
                "embedding": TorchTensorSpec("bxt, h", h=config.output_dim),
                "state_out": {
                    "h": TorchTensorSpec("b, h", h=config.hidden_dim),
                    "c": TorchTensorSpec("b, h", h=config.hidden_dim),
                },
            }
        )

    def forward(self, input_dict: SampleBatch):
        x = input_dict[SampleBatch.OBS]
        states = input_dict["state_in"]
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
            "embedding": x,
            "state_out": tree.map_structure(lambda x: x.transpose(0, 1), states_o),
        }


class IdentityEncoder(Encoder):
    def __init__(self, config: EncoderConfig) -> None:
        super().__init__(config)

    def input_spec(self):
        return SpecDict()

    def output_spec(self):
        return SpecDict()

    def _forward(self, input_dict):
        return input_dict
