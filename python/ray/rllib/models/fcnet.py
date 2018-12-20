from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer, get_activation_fn


class FullyConnectedNetwork(Model):
    """Generic fully connected network."""

    def _build_layers(self, inputs, num_outputs, options):
        """Process the flattened inputs.

        Note that dict inputs will be flattened into a vector. To define a
        model that processes the components separately, use _build_layers_v2().
        """
        print("IN THE HOOD")
        print(options)
        hiddens = options.get("fcnet_hiddens")
        activation = get_activation_fn(options.get("fcnet_activation"))
        print(hiddens)
        with tf.name_scope("fc_net"):
            i = 1
            last_layer = inputs
            for size in hiddens:
                print(last_layer)
                label = "fc{}".format(i)
                last_layer = slim.fully_connected(
                    last_layer,
                    size,
                    weights_initializer=normc_initializer(1.0),
                    activation_fn=activation,
                    scope=label)
                i += 1
            label = "fc_out"
            print(last_layer)
            output = slim.fully_connected(
                last_layer,
                num_outputs,
                weights_initializer=normc_initializer(0.01),
                activation_fn=None,
                scope=label)
            print(output)
            return output, last_layer
