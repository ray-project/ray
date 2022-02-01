import random

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.tf.fcnet import FullyConnectedNetwork
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf

tf1, tf, tfv = try_import_tf()


class EagerModel(TFModelV2):
    """Example of using embedded eager execution in a custom model.

    This shows how to use tf.py_function() to execute a snippet of TF code
    in eager mode. Here the `self.forward_eager` method just prints out
    the intermediate tensor for debug purposes, but you can in general
    perform any TF eager operation in tf.py_function().
    """

    def __init__(
        self, observation_space, action_space, num_outputs, model_config, name
    ):
        super().__init__(
            observation_space, action_space, num_outputs, model_config, name
        )

        inputs = tf.keras.layers.Input(shape=observation_space.shape)
        self.fcnet = FullyConnectedNetwork(
            obs_space=self.obs_space,
            action_space=self.action_space,
            num_outputs=self.num_outputs,
            model_config=self.model_config,
            name="fc1",
        )
        out, value_out = self.fcnet.base_model(inputs)

        def lambda_(x):
            eager_out = tf.py_function(self.forward_eager, [x], tf.float32)
            with tf1.control_dependencies([eager_out]):
                eager_out.set_shape(x.shape)
                return eager_out

        out = tf.keras.layers.Lambda(lambda_)(out)
        self.base_model = tf.keras.models.Model(inputs, [out, value_out])

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        out, self._value_out = self.base_model(input_dict["obs"], state, seq_lens)
        return out, []

    @override(ModelV2)
    def value_function(self):
        return tf.reshape(self._value_out, [-1])

    def forward_eager(self, feature_layer):
        assert tf.executing_eagerly()
        if random.random() > 0.99:
            print(
                "Eagerly printing the feature layer mean value",
                tf.reduce_mean(feature_layer),
            )
        return feature_layer
