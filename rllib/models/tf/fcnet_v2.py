import numpy as np

from ray.rllib.models.tf.misc import normc_initializer
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.framework import get_activation_fn, try_import_tf

tf = try_import_tf()


class FullyConnectedNetwork(TFModelV2):
    """Generic fully connected network implemented in ModelV2 API."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super(FullyConnectedNetwork, self).__init__(
            obs_space, action_space, num_outputs, model_config, name)

        activation = get_activation_fn(model_config.get("fcnet_activation"))
        hiddens = model_config.get("fcnet_hiddens")
        no_final_linear = model_config.get("no_final_linear")
        vf_share_layers = model_config.get("vf_share_layers")

        # we are using obs_flat, so take the flattened shape as input
        inputs = tf.keras.layers.Input(
            shape=(np.product(obs_space.shape), ), name="observations")
        last_layer = layer_out = inputs
        i = 1

        # Create layers 0 to second-last.
        for size in hiddens[:-1]:
            last_layer = tf.keras.layers.Dense(
                size,
                name="fc_{}".format(i),
                activation=activation,
                kernel_initializer=normc_initializer(1.0))(last_layer)
            i += 1

        # The last layer is adjusted to be of size num_outputs, but it's a
        # layer with activation.
        if no_final_linear and self.num_outputs:
            layer_out = tf.keras.layers.Dense(
                self.num_outputs,
                name="fc_out",
                activation=activation,
                kernel_initializer=normc_initializer(1.0))(last_layer)
        # Finish the layers with the provided sizes (`hiddens`), plus -
        # iff num_outputs > 0 - a last linear layer of size num_outputs.
        else:
            if len(hiddens) > 0:
                last_layer = tf.keras.layers.Dense(
                    hiddens[-1],
                    name="fc_{}".format(i),
                    activation=activation,
                    kernel_initializer=normc_initializer(1.0))(last_layer)
            if self.num_outputs:
                layer_out = tf.keras.layers.Dense(
                    self.num_outputs,
                    name="fc_out",
                    activation=None,
                    kernel_initializer=normc_initializer(0.01))(last_layer)
            # Adjust self.num_outputs to be the number of nodes in the last
            # layer.
            else:
                self.num_outputs = (
                    [np.product(obs_space.shape)] + hiddens[-1:-1])[-1]

        if not vf_share_layers:
            # build a parallel set of hidden layers for the value net
            last_layer = inputs
            i = 1
            for size in hiddens:
                last_layer = tf.keras.layers.Dense(
                    size,
                    name="fc_value_{}".format(i),
                    activation=activation,
                    kernel_initializer=normc_initializer(1.0))(last_layer)
                i += 1

        value_out = tf.keras.layers.Dense(
            1,
            name="value_out",
            activation=None,
            kernel_initializer=normc_initializer(0.01))(last_layer)

        self.base_model = tf.keras.Model(inputs, [layer_out, value_out])
        self.register_variables(self.base_model.variables)

    def forward(self, input_dict, state, seq_lens):
        model_out, self._value_out = self.base_model(input_dict["obs_flat"])
        return model_out, state

    def value_function(self):
        return tf.reshape(self._value_out, [-1])
