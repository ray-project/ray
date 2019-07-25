from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.models.misc import normc_initializer, get_activation_fn
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


class FullyConnectedNetwork(TFModelV2):
    """Generic fully connected network implemented in ModelV2 API.

    TODO(ekl): should make this the default fcnet in the future."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(FullyConnectedNetwork, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        if not model_config["vf_share_layers"]:
            raise NotImplementedError("vf_share_layers=False")

        hiddens = model_config.get("fcnet_hiddens")
        activation = get_activation_fn(model_config.get("fcnet_activation"))
        no_final_linear = model_config.get("no_final_linear")

        inputs = tf.keras.layers.Input(
            shape=obs_space.shape, name="observations")
        last_layer = inputs
        i = 1

        if no_final_linear:
            for size in hiddens[:-1]:
                last_layer = tf.keras.layers.Dense(
                    size,
                    name="fc_{}".format(i),
                    activation=activation,
                    kernel_initializer=normc_initializer(1.0))(last_layer)
                i += 1
            layer_out = tf.keras.layers.Dense(
                size,
                name="fc_out",
                activation=activation,
                kernel_initializer=normc_initializer(1.0))(last_layer)
        else:
            for size in hiddens:
                last_layer = tf.keras.layers.Dense(
                    size,
                    name="fc_{}".format(i),
                    activation=activation,
                    kernel_initializer=normc_initializer(1.0))(last_layer)
                i += 1
            layer_out = tf.keras.layers.Dense(
                num_outputs,
                name="fc_out",
                activation=None,
                kernel_initializer=normc_initializer(0.01))(last_layer)
        value_out = tf.keras.layers.Dense(
            1,
            name="value_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(last_layer)
        self.base_model = tf.keras.Model(inputs, [layer_out, value_out])
        self.register_variables(self.base_model.variables)

    def forward(self, input_dict, state, seq_lens):
        model_out, self._value_out = self.base_model(input_dict["obs"])
        return model_out, state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])
