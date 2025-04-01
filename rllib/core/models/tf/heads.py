import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import (
    CNNTransposeHeadConfig,
    FreeLogStdMLPHeadConfig,
    MLPHeadConfig,
)
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.tf.primitives import TfCNNTranspose, TfMLP
from ray.rllib.models.utils import get_initializer_fn
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
        self.log_std_clip_param = tf.constant([config.log_std_clip_param])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Only clip the log standard deviations, if the user wants to clip. This
        # avoids also clipping value heads.
        if self.clip_log_std:
            # Forward pass.
            means, log_stds = tf.split(self.net(inputs), num_or_size_splits=2, axis=-1)
            # Clip the log standard deviations.
            log_stds = tf.clip_by_value(
                log_stds, -self.log_std_clip_param, self.log_std_clip_param
            )
            return tf.concat([means, log_stds], axis=-1)
        # Otherwise just return the logits.
        else:
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

        self.log_std = tf.Variable(
            tf.zeros(self._half_output_dim),
            name="log_std",
            dtype=tf.float32,
            trainable=True,
        )
        # If log standard deviations should be clipped. This should be only true for
        # policy heads. Value heads should never be clipped.
        self.clip_log_std = config.clip_log_std
        # The clipping parameter for the log standard deviation.
        self.log_std_clip_param = tf.constant([config.log_std_clip_param])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)
        # If log standard deviation should be clipped.
        if self.clip_log_std:
            # Clip log standard deviations to stabilize training. Note, the
            # default clip value is `inf`, i.e. no clipping.
            log_std = tf.clip_by_value(
                self.log_std, -self.log_std_clip_param, self.log_std_clip_param
            )
        else:
            log_std = self.log_std
        log_std_out = tf.tile(tf.expand_dims(log_std, 0), [tf.shape(inputs)[0], 1])
        logits_out = tf.concat([mean, log_std_out], axis=1)
        return logits_out


class TfCNNTransposeHead(TfModel):
    def __init__(self, config: CNNTransposeHeadConfig) -> None:
        super().__init__(config)

        # Initial, inactivated Dense layer (always w/ bias). Use the
        # hidden layer initializer for this layer.
        initial_dense_weights_initializer = get_initializer_fn(
            config.initial_dense_weights_initializer, framework="tf2"
        )
        initial_dense_bias_initializer = get_initializer_fn(
            config.initial_dense_bias_initializer, framework="tf2"
        )

        # This layer is responsible for getting the incoming tensor into a proper
        # initial image shape (w x h x filters) for the suceeding Conv2DTranspose stack.
        self.initial_dense = tf.keras.layers.Dense(
            units=int(np.prod(config.initial_image_dims)),
            activation=None,
            kernel_initializer=(
                initial_dense_weights_initializer(
                    **config.initial_dense_weights_initializer_config
                )
                if config.initial_dense_weights_initializer_config
                else initial_dense_weights_initializer
            ),
            use_bias=True,
            bias_initializer=(
                initial_dense_bias_initializer(
                    **config.initial_dense_bias_initializer_config
                )
                if config.initial_dense_bias_initializer_config
                else initial_dense_bias_initializer
            ),
        )

        # The main CNNTranspose stack.
        self.cnn_transpose_net = TfCNNTranspose(
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
