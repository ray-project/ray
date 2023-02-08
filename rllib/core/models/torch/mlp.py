from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.base import Model
from ray.rllib.core.models.torch.primitives import TorchMLP, TorchModel
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.nested_dict import NestedDict

_, nn = try_import_torch()


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

        self.input_spec = TorchTensorSpec("b, h", h=self.config.input_dim)
        self.output_spec = TorchTensorSpec("b, h", h=self.config.output_dim)

    @override(Model)
    def _forward(self, inputs: NestedDict, **kwargs) -> NestedDict:
        return self.net(inputs)
