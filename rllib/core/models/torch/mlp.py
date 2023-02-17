from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchMLP
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLPHead(TorchModel, nn.Module):
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

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, h", h=self.config.input_dim)

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, h", h=self.config.output_dim)

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        return self.net(inputs)
