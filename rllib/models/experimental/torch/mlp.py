import torch.nn as nn

from ray.rllib.models.experimental.base import ForwardOutputType, Model, ModelConfig
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.models.temp_spec_classes import TensorDict
from ray.rllib.models.experimental.torch.primitives import TorchMLP
from ray.rllib.models.experimental.torch.primitives import TorchModel
from ray.rllib.utils.annotations import override


class TorchMLPModel(TorchModel, nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        nn.Module.__init__(self)
        TorchModel.__init__(self, config)

        self.net = TorchMLP(
            input_dim=config.input_dim,
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dim,
            hidden_layer_activation=config.hidden_layer_activation,
            output_activation=config.output_activation,
        )

    @property
    @override(Model)
    def input_spec(self) -> TorchTensorSpec:
        return TorchTensorSpec("b, h", h=self.config.input_dim)

    @property
    @override(Model)
    def output_spec(self) -> TorchTensorSpec:
        return TorchTensorSpec("b, h", h=self.config.output_dim)

    @check_input_specs("input_spec", cache=False)
    @check_output_specs("output_spec", cache=False)
    @override(TorchModel)
    def forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        return self.net(inputs)
