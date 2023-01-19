from dataclasses import dataclass
from dataclasses import field
from typing import List

import torch.nn as nn

from ray.rllib.models.base_model import Model, ModelConfig, ForwardOutputType
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.torch.primitives import FCNet


@dataclass
class FCConfig(ModelConfig):
    """Configuration for a fully connected network.

    Attributes:
        input_dim: The input dimension of the network. It cannot be None.
        hidden_layers: The sizes of the hidden layers.
        activation: The activation function to use after each layer (except for the
            output).
        output_activation: The activation function to use for the output layer.
    """

    input_dim: int = None
    hidden_layers: List[int] = field(default_factory=lambda: [256, 256])
    activation: str = "ReLU"
    output_activation: str = "ReLU"

    def build(self) -> Model:
        return FC(self)


class FC(Model, nn.Module):
    def __init__(self, config: FCConfig) -> None:
        nn.Module.__init__(self)
        Model.__init__(self, config)

        self.net = FCNet(
            input_dim=config.input_dim,
            hidden_layers=config.hidden_layers,
            output_dim=config.output_dim,
            activation=config.activation,
        )

    @property
    def input_spec(self):
        return TorchTensorSpec("b, h", h=self.config.input_dim)

    @property
    def output_spec(self):
        return TorchTensorSpec("b, h", h=self.config.output_dim)

    @check_input_specs("input_spec", filter=True, cache=False)
    @check_output_specs("output_spec", cache=False)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return self.net(inputs)
