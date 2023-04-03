from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import FreeLogStdMLPHeadConfig, MLPHeadConfig
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_torch import TorchTensorSpec
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchMLP
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLPHead(TorchModel, nn.Module):
    def __init__(self, config: MLPHeadConfig) -> None:
        nn.Module.__init__(self)
        TorchModel.__init__(self, config)

        self.net = TorchMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            output_dim=config.output_dims[0],
            output_activation=config.output_activation,
            use_bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, d", d=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, d", d=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        return self.net(inputs)


class TorchFreeLogStdMLPHead(TorchModel, nn.Module):
    """An MLPHead that implements floating log stds for Gaussian distributions."""

    def __init__(self, config: FreeLogStdMLPHeadConfig) -> None:
        nn.Module.__init__(self)
        TorchModel.__init__(self, config)

        assert (
            config.output_dims[0] % 2 == 0
        ), "output_dims must be even for free std!"
        self._half_output_dim = config.output_dims[0] // 2

        self.net = TorchMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            output_dim=self._half_output_dim,
            output_activation=config.output_activation,
            use_bias=config.use_bias,
        )

        self.log_std = torch.nn.Parameter(
            torch.as_tensor([0.0] * self._half_output_dim)
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, d", d=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TorchTensorSpec("b, d", d=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)

        return torch.cat(
            [mean, self.log_std.unsqueeze(0).repeat([len(mean), 1])], axis=1
        )
