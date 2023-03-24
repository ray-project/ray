from typing import Union

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.base import ModelConfig
from ray.rllib.core.models.tf.primitives import TfMLP, TfModel
from ray.rllib.models.specs.specs_base import Spec
from ray.rllib.models.specs.specs_tf import TFTensorSpecs
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict

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
    def get_input_spec(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        return self.net(inputs)


class FreeStdMLPHead(TfMLPHead):
    """An MLPHead that encapsulates a floating std for Gaussian distributions."""

    def __init__(self, config: ModelConfig) -> None:
        TfMLPHead.__init__(self, config.mlp_head_config)

        # Add a trainable variable for the std.
        self.log_std = tf.Variable(
            tf.zeros(config.output_dims[0] / 2),
            name="log_std",
            dtype=tf.float32,
        )

    @override(Model)
    def get_input_spec(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.input_dims[0])

    @override(Model)
    def get_output_spec(self) -> Union[Spec, None]:
        return TFTensorSpecs("b, h", h=self.config.output_dims[0])

    @override(Model)
    def _forward(self, inputs: tf.Tensor, **kwargs) -> tf.Tensor:
        # Compute the mean and std.
        mean = self.net(inputs)

        def tiled_log_std(x):
            return tf.tile(tf.expand_dims(self.log_std_var, 0), [tf.shape(x)[0], 1])

        log_std_out = tf.keras.layers.Lambda(tiled_log_std)(inputs)
        logits_out = tf.keras.layers.Concatenate(axis=1)([mean, log_std_out])
        return logits_out
