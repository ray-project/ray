import torch.nn as nn

from ray.rllib.models.experimental.base import ForwardOutputType, Model, ModelConfig
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.experimental.torch.primitives import FCNet
from ray.rllib.models.experimental.torch.primitives import TorchModel
from ray.rllib.utils.annotations import override


class FCModel(TorchModel, nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        nn.Module.__init__(self)
        TorchModel.__init__(self, config)

        self.net = FCNet(
            input_dim=config.input_dim,
            hidden_layers=config.hidden_layers,
            output_dim=config.output_dim,
            activation=config.activation,
        )

    @property
    @override(Model)
    def input_spec(self):
        return TorchTensorSpec("b, h", h=self.config.input_dim)

    @property
    @override(Model)
    def output_spec(self):
        return TorchTensorSpec("b, h", h=self.config.output_dim)

    @check_input_specs("input_spec", filter=True, cache=False)
    @check_output_specs("output_spec", cache=False)
    @override(TorchModel)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return self.net(inputs)
