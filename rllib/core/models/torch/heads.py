from typing import Optional

import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    CNNTransposeHeadConfig,
    FreeLogStdMLPHeadConfig,
    MLPHeadConfig,
)
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchCNNTranspose, TorchMLP
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TorchMLPHead(TorchModel):
    def __init__(self, config: MLPHeadConfig) -> None:
        super().__init__(config)

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
    def get_input_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.input_dims[0], framework="torch")

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.output_dims[0], framework="torch")

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        return self.net(inputs)


class TorchFreeLogStdMLPHead(TorchModel):
    """An MLPHead that implements floating log stds for Gaussian distributions."""

    def __init__(self, config: FreeLogStdMLPHeadConfig) -> None:
        super().__init__(config)

        assert config.output_dims[0] % 2 == 0, "output_dims must be even for free std!"
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
    def get_input_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.input_dims[0], framework="torch")

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.output_dims[0], framework="torch")

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)

        return torch.cat(
            [mean, self.log_std.unsqueeze(0).repeat([len(mean), 1])], axis=1
        )


class TorchCNNTransposeHead(TorchModel):
    def __init__(self, config: CNNTransposeHeadConfig) -> None:
        super().__init__(config)

        # Initial, inactivated Dense layer (always w/ bias).
        # This layer is responsible for getting the incoming tensor into a proper
        # initial image shape (w x h x filters) for the suceeding Conv2DTranspose stack.
        self.initial_dense = nn.Linear(
            in_features=config.input_dims[0],
            out_features=int(np.prod(config.initial_image_dims)),
            bias=True,
        )

        # The main CNNTranspose stack.
        self.cnn_transpose_net = TorchCNNTranspose(
            input_dims=config.initial_image_dims,
            cnn_transpose_filter_specifiers=config.cnn_transpose_filter_specifiers,
            cnn_transpose_activation=config.cnn_transpose_activation,
            cnn_transpose_use_layernorm=config.cnn_transpose_use_layernorm,
            use_bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.input_dims[0], framework="torch")

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return TensorSpec(
            "b, w, h, c",
            w=self.config.output_dims[0],
            h=self.config.output_dims[1],
            c=self.config.output_dims[2],
            framework="torch",
        )

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        out = self.initial_dense(inputs)
        # Reshape to initial 3D (image-like) format to enter CNN transpose stack.
        out = out.reshape((-1,) + tuple(self.config.initial_image_dims))
        out = self.cnn_transpose_net(out)
        # Add 0.5 to center (always non-activated, non-normalized) outputs more
        # around 0.0.
        return out + 0.5
