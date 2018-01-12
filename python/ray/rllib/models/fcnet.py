from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer

USER_DATA_CONFIGS = [
    "fcnet_tag",  # Optional tag for fcnets to allow for more than one
    "shared_model", # Optional tag testing if a multiagent shared model is being created
]



class FullyConnectedNetwork(Model):
    """Generic fully connected network."""

    def _init(self, inputs, num_outputs, options):
        hiddens = options.get("fcnet_hiddens", [256, 256])
        if isinstance(hiddens[0], list):
            hiddens = hiddens[0]
        fcnet_activation = options.get("fcnet_activation", "tanh")
        if fcnet_activation == "tanh":
            activation = tf.nn.tanh
        elif fcnet_activation == "relu":
            activation = tf.nn.relu
        print("Constructing fcnet {} {}".format(hiddens, activation))

        user_data = options.get("user_data", {})
        for k in user_data.keys():
            if k not in USER_DATA_CONFIGS:
                raise Exception(
                    "Unknown config key `{}`, all keys: {}".format(k,
                                                            USER_DATA_CONFIGS))
        fcnet_tag = user_data.get("fcnet_tag", None)
        shared_model = user_data.get("shared_model", False)
        # If we're going to use a shared multiagent model, reuose the variables
        if shared_model:
            reuse = tf.AUTO_REUSE
        else:
            reuse = False
        singular = fcnet_tag is None
        with tf.name_scope("fc_net"):
            i = 1
            last_layer = inputs
            for size in hiddens:
                label = "fc{}".format(i) if singular else "fc{}_{}".format(
                    fcnet_tag, i)
                last_layer = slim.fully_connected(
                    last_layer, size,
                    weights_initializer=normc_initializer(1.0),
                    activation_fn=activation,
                    scope=label, reuse=reuse)
                i += 1
            label = "fc_out" if singular else "fc_out_{}".format(fcnet_tag, i)
            output = slim.fully_connected(
                last_layer, num_outputs,
                weights_initializer=normc_initializer(0.01),
                activation_fn=None, scope=label, reuse=reuse)
            return output, last_layer