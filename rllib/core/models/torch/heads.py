import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    CNNTransposeHeadConfig,
    FreeLogStdMLPHeadConfig,
    MLPHeadConfig,
)
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.models.torch.primitives import TorchCNNTranspose, TorchMLP
from ray.rllib.models.utils import get_initializer_fn
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
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=config.output_layer_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
        )
        # If log standard deviations should be clipped. This should be only true for
        # policy heads. Value heads should never be clipped.
        self.clip_log_std = config.clip_log_std
        # The clipping parameter for the log standard deviation.
        self.log_std_clip_param = torch.Tensor([config.log_std_clip_param])
        # Register a buffer to handle device mapping.
        self.register_buffer("log_std_clip_param_const", self.log_std_clip_param)

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Only clip the log standard deviations, if the user wants to clip. This
        # avoids also clipping value heads.
        if self.clip_log_std:
            # Forward pass.
            means, log_stds = torch.chunk(self.net(inputs), chunks=2, dim=-1)
            # Clip the log standard deviations.
            log_stds = torch.clamp(
                log_stds, -self.log_std_clip_param_const, self.log_std_clip_param_const
            )
            return torch.cat((means, log_stds), dim=-1)
        # Otherwise just return the logits.
        else:
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
            hidden_layer_use_bias=config.hidden_layer_use_bias,
            hidden_layer_weights_initializer=config.hidden_layer_weights_initializer,
            hidden_layer_weights_initializer_config=(
                config.hidden_layer_weights_initializer_config
            ),
            hidden_layer_bias_initializer=config.hidden_layer_bias_initializer,
            hidden_layer_bias_initializer_config=(
                config.hidden_layer_bias_initializer_config
            ),
            output_dim=self._half_output_dim,
            output_activation=config.output_layer_activation,
            output_use_bias=config.output_layer_use_bias,
            output_weights_initializer=config.output_layer_weights_initializer,
            output_weights_initializer_config=(
                config.output_layer_weights_initializer_config
            ),
            output_bias_initializer=config.output_layer_bias_initializer,
            output_bias_initializer_config=config.output_layer_bias_initializer_config,
        )

        self.log_std = torch.nn.Parameter(
            torch.as_tensor([0.0] * self._half_output_dim)
        )
        # If log standard deviations should be clipped. This should be only true for
        # policy heads. Value heads should never be clipped.
        self.clip_log_std = config.clip_log_std
        # The clipping parameter for the log standard deviation.
        self.log_std_clip_param = torch.Tensor(
            [config.log_std_clip_param], device=self.log_std.device
        )
        # Register a buffer to handle device mapping.
        self.register_buffer("log_std_clip_param_const", self.log_std_clip_param)

    @override(Model)
    def _forward(self, inputs: torch.Tensor, **kwargs) -> torch.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)

        # If log standard deviation should be clipped.
        if self.clip_log_std:
            # Clip the log standard deviation to avoid running into too small
            # deviations that factually collapses the policy.
            log_std = torch.clamp(
                self.log_std,
                -self.log_std_clip_param_const,
                self.log_std_clip_param_const,
            )
        else:
            log_std = self.log_std

        return torch.cat([mean, log_std.unsqueeze(0).repeat([len(mean), 1])], axis=1)


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

        # Initial Dense layer initializers.
        initial_dense_weights_initializer = get_initializer_fn(
            config.initial_dense_weights_initializer, framework="torch"
        )
        initial_dense_bias_initializer = get_initializer_fn(
            config.initial_dense_bias_initializer, framework="torch"
        )

        # Initialize dense layer weights, if necessary.
        if initial_dense_weights_initializer:
            initial_dense_weights_initializer(
                self.initial_dense.weight,
                **config.initial_dense_weights_initializer_config or {},
            )
        # Initialized dense layer bais, if necessary.
        if initial_dense_bias_initializer:
            initial_dense_bias_initializer(
                self.initial_dense.bias,
                **config.initial_dense_bias_initializer_config or {},
            )

        # The main CNNTranspose stack.
        self.cnn_transpose_net = TorchCNNTranspose(
            input_dims=config.initial_image_dims,
            cnn_transpose_filter_specifiers=config.cnn_transpose_filter_specifiers,
            cnn_transpose_activation=config.cnn_transpose_activation,
            cnn_transpose_use_layernorm=config.cnn_transpose_use_layernorm,
            cnn_transpose_use_bias=config.cnn_transpose_use_bias,
            cnn_transpose_kernel_initializer=config.cnn_transpose_kernel_initializer,
            cnn_transpose_kernel_initializer_config=(
                config.cnn_transpose_kernel_initializer_config
            ),
            cnn_transpose_bias_initializer=config.cnn_transpose_bias_initializer,
            cnn_transpose_bias_initializer_config=(
                config.cnn_transpose_bias_initializer_config
            ),
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
