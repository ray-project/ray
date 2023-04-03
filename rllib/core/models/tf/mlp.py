from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import FreeLogStdMLPHeadConfig, MLPHeadConfig
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_tf import TfTensorSpec
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.tf.primitives import TfMLP
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
        TfModel.__init__(self, config)

        assert (
            config.output_dims[0] % 2 == 0
        ), "output_dims must be even for free std!"
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
    def get_input_specs(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TfTensorSpec("b, d", d=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)
        log_std_out = tf.tile(tf.expand_dims(self.log_std, 0), [tf.shape(inputs)[0], 1])
        logits_out = tf.concat([mean, log_std_out], axis=1)
        return logits_out
