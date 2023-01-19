import torch.nn as nn

from ray.rllib.models.base_model import Model, ForwardOutputType
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.torch.primitives import FCNet
from rllib.models.base_model import ModelConfig


class FCModel(Model, nn.Module):
    def __init__(self, config: ModelConfig) -> None:
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
