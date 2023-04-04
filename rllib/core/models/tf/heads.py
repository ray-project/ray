from typing import Union

import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    CNNTransposeHeadConfig,
    FreeLogStdMLPHeadConfig,
    MLPHeadConfig,
)
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.tf.primitives import TfMLP, TfCNNTranspose
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.models.specs.specs_tf import TfTensorSpec
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
    def get_input_specs(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        return self.net(inputs)


class TfFreeLogStdMLPHead(TfModel):
    """An MLPHead that implements floating log stds for Gaussian distributions."""

    def __init__(self, config: FreeLogStdMLPHeadConfig) -> None:
        mlp_head_config = config.mlp_head_config

        TfModel.__init__(self, mlp_head_config)

        assert (
            mlp_head_config.output_dims[0] % 2 == 0
        ), "output_dims must be even for free std!"
        self._half_output_dim = mlp_head_config.output_dims[0] // 2

        self.net = TfMLP(
            input_dim=mlp_head_config.input_dims[0],
            hidden_layer_dims=mlp_head_config.hidden_layer_dims,
            hidden_layer_activation=mlp_head_config.hidden_layer_activation,
            hidden_layer_use_layernorm=mlp_head_config.hidden_layer_use_layernorm,
            output_dim=self._half_output_dim,
            output_activation=mlp_head_config.output_activation,
            use_bias=mlp_head_config.use_bias,
        )

        self.log_std = tf.Variable(
            tf.zeros(self._half_output_dim),
            name="log_std",
            dtype=tf.float32,
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)

        def tiled_log_std(x):
            return tf.tile(tf.expand_dims(self.log_std, 0), [tf.shape(x)[0], 1])

        log_std_out = tf.keras.layers.Lambda(tiled_log_std)(inputs)
        logits_out = tf.keras.layers.Concatenate(axis=1)([mean, log_std_out])
        return logits_out


class TfCNNTransposeHead(TfModel):
    def __init__(self, config: CNNTransposeHeadConfig) -> None:
        TfModel.__init__(self, config)

        # Initial, inactivated Dense layer (always w/ bias).
        self.initial_dense = tf.keras.layers.Dense(
            units=int(np.prod(config.initial_dense_layer_output_dims)),
            activation=None,
            use_bias=True,
        )

        # The main CNN Transpose stack.
        self.cnn_transpose_net = TfCNNTranspose(
            input_dims=config.initial_dense_layer_output_dims,
            cnn_transpose_filter_specifiers=config.cnn_transpose_filter_specifiers,
            cnn_transpose_activation=config.cnn_transpose_activation,
            cnn_transpose_use_layernorm=config.cnn_transpose_use_layernorm,
            use_bias=config.use_bias,
        )
        
    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.input_dims[0])

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return TfTensorSpec(
            "b, w, h, c",
            w=self.config.output_dims[0],
            h=self.config.output_dims[1],
            c=self.config.output_dims[2],
        )

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        out = self.initial_dense(inputs)
        # Reshape to initial 3D (image-like) format to enter CNN transpose stack.
        out = tf.reshape(out, shape=(-1,) + tuple(self.config.initial_dense_layer_output_dims))
        out = self.cnn_transpose_net(out)
        # Add 0.5 to center (always non-activated, non-normalized) outputs more
        # around 0.0. 
        return out + 0.5
