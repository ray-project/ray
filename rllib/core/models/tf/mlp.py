from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.tf.primitives import TfMLP, TfModel
from ray.rllib.core.models.specs.specs_base import Spec
from ray.rllib.core.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override

tf1, tf, tfv = try_import_tf()


class TfMLPHead(TfModel):
    def __init__(self, config: ModelConfig) -> None:
        TfModel.__init__(self, config)

        self.net = TfMLP(
            input_dim=config.input_dims[0],
            hidden_layer_dims=config.hidden_layer_dims,
            output_dim=config.output_dims[0],
            hidden_layer_activation=config.hidden_layer_activation,
            output_activation=config.output_activation,
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        return self.net(inputs)


class TfFreeLogStdMLPHead(TfModel):
    """An MLPHead that implements floating log stds for Gaussian distributions."""

    def __init__(self, config: ModelConfig) -> None:
        mlp_head_config = config.mlp_head_config

        TfModel.__init__(self, mlp_head_config)

        assert (
            mlp_head_config.output_dims[0] % 2 == 0
        ), "output_dims must be even for free std!"
        self._half_output_dim = mlp_head_config.output_dims[0] // 2

        self.net = TfMLP(
            input_dim=mlp_head_config.input_dims[0],
            hidden_layer_dims=mlp_head_config.hidden_layer_dims,
            output_dim=self._half_output_dim,
            hidden_layer_activation=mlp_head_config.hidden_layer_activation,
            output_activation=mlp_head_config.output_activation,
        )

        self.log_std = tf.Variable(
            tf.zeros(self._half_output_dim),
            name="log_std",
            dtype=tf.float32,
        )

    @override(Model)
    def get_input_specs(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_specs(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Compute the mean first, then append the log_std.
        mean = self.net(inputs)

        def tiled_log_std(x):
            return tf.tile(tf.expand_dims(self.log_std, 0), [tf.shape(x)[0], 1])

        log_std_out = tf.keras.layers.Lambda(tiled_log_std)(inputs)
        logits_out = tf.keras.layers.Concatenate(axis=1)([mean, log_std_out])
        return logits_out
