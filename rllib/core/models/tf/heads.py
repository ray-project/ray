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
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.tf.primitives import TfCNNTranspose, TfMLP
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf1, tf, tfv = try_import_tf()


class TfMLPHead(TfModel):
    def __init__(self, config: MLPHeadConfig) -> None:
        TfModel.__init__(self, config)

        self.net = TfMLP(
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
        return TensorSpec("b, d", d=self.config.input_dims[0], framework="tf2")

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.output_dims[0], framework="tf2")

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        return self.net(inputs)


class TfFreeLogStdMLPHead(TfModel):
    """An MLPHead that implements floating log stds for Gaussian distributions."""

    def __init__(self, config: FreeLogStdMLPHeadConfig) -> None:
        TfModel.__init__(self, config)

        assert config.output_dims[0] % 2 == 0, "output_dims must be even for free std!"
        self._half_output_dim = config.output_dims[0] // 2

        self.net = TfMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            hidden_layer_activation=config.hidden_layer_activation,
            hidden_layer_use_layernorm=config.hidden_layer_use_layernorm,
            output_dim=self._half_output_dim,
            output_activation=config.output_activation,
            use_bias=config.use_bias,
        )

        self.log_std = tf.Variable(
            tf.zeros(self._half_output_dim),
            name="log_std",
            dtype=tf.float32,
            trainable=True,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.input_dims[0], framework="tf2")

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.output_dims[0], framework="tf2")

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)
        log_std_out = tf.tile(tf.expand_dims(self.log_std, 0), [tf.shape(inputs)[0], 1])
        logits_out = tf.concat([mean, log_std_out], axis=1)
        return logits_out


class TfCNNTransposeHead(TfModel):
    def __init__(self, config: CNNTransposeHeadConfig) -> None:
        super().__init__(config)

        # Initial, inactivated Dense layer (always w/ bias).
        # This layer is responsible for getting the incoming tensor into a proper
        # initial image shape (w x h x filters) for the suceeding Conv2DTranspose stack.
        self.initial_dense = tf.keras.layers.Dense(
            units=int(np.prod(config.initial_image_dims)),
            activation=None,
            use_bias=True,
        )

        # The main CNNTranspose stack.
        self.cnn_transpose_net = TfCNNTranspose(
            input_dims=config.initial_image_dims,
            cnn_transpose_filter_specifiers=config.cnn_transpose_filter_specifiers,
            cnn_transpose_activation=config.cnn_transpose_activation,
            cnn_transpose_use_layernorm=config.cnn_transpose_use_layernorm,
            use_bias=config.use_bias,
        )

    @override(Model)
    def get_input_specs(self) -> Optional[Spec]:
        return TensorSpec("b, d", d=self.config.input_dims[0], framework="tf2")

    @override(Model)
    def get_output_specs(self) -> Optional[Spec]:
        return TensorSpec(
            "b, w, h, c",
            w=self.config.output_dims[0],
            h=self.config.output_dims[1],
            c=self.config.output_dims[2],
            framework="tf2",
        )

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Push through initial dense layer to get dimensions of first "image".
        out = self.initial_dense(inputs)
        # Reshape to initial 3D (image-like) format to enter CNN transpose stack.
        out = tf.reshape(
            out,
            shape=(-1,) + tuple(self.config.initial_image_dims),
        )
        # Push through CNN transpose stack.
        out = self.cnn_transpose_net(out)
        # Add 0.5 to center the (always non-activated, non-normalized) outputs more
        # around 0.0.
        return out + 0.5
