from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchMLP
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLPHead(TorchModel, nn.Module):
    def __init__(self, config: ModelConfig) -> None:
        nn.Module.__init__(self)
        TorchModel.__init__(self, config)

        self.net = TorchMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dims[0],
            hidden_layer_activation=config.hidden_layer_activation,
            output_activation=config.output_activation,
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        return self.net(inputs)


class TorchFreeLogStdMLPHead(TorchModel, nn.Module):
    """An MLPHead that implements floating log stds for Gaussian distributions."""

    def __init__(self, config: ModelConfig) -> None:
        mlp_head_config = config.mlp_head_config

        nn.Module.__init__(self)
        TorchModel.__init__(self, mlp_head_config)

        assert (
            mlp_head_config.output_dims[0] % 2 == 0
        ), "output_dims must be even for free std!"
        self._half_output_dim = mlp_head_config.output_dims[0] // 2

        self.net = TorchMLP(
            input_dim=mlp_head_config.input_dims[0],
            hidden_layer_dims=mlp_head_config.hidden_layer_dims,
            output_dim=self._half_output_dim,
            hidden_layer_activation=mlp_head_config.hidden_layer_activation,
            output_activation=mlp_head_config.output_activation,
        )

        self.log_std = torch.nn.Parameter(
            torch.as_tensor([0.0] * self._half_output_dim)
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)

        return torch.cat(
            [mean, self.log_std.unsqueeze(0).repeat([len(mean), 1])], axis=1
        )
