from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

import numpy as np

from ray.rllib.models.model import Model


def normc_initializer(std=1.0):
    def _initializer(shape, dtype=None, partition_info=None):
        out = np.random.randn(*shape).astype(np.float32)
        out *= std / np.sqrt(np.square(out).sum(axis=0, keepdims=True))
        return tf.constant(out)
    return _initializer


class FullyConnectedNetwork(Model):
    """Generic fully connected network.

       Options to construct the network are passed to the _init function.
       If options["free_logstd"] is True, the last half of the
       output layer will be free variables that are not dependent on
       inputs. This is often used if the output of the network is used
       to parametrize a probability distribution. In this case, the
       first half of the parameters can be interpreted as a location
       parameter (like a mean) and the second half can be interpreted as
       a scale parameter (like a standard deviation).
    """

    def _init(self, inputs, num_outputs, options):
        hiddens = options.get("fcnet_hiddens", [256, 256])
        activation = options.get("fcnet_activation", tf.nn.tanh)
        print("Constructing fcnet {} {}".format(hiddens, activation))

        if options.get("free_logstd", False):
            num_outputs = num_outputs // 2

        with tf.name_scope("fc_net"):
            i = 1
            last_layer = inputs
            for size in hiddens:
                last_layer = slim.fully_connected(
                    last_layer, size,
                    weights_initializer=normc_initializer(1.0),
                    activation_fn=activation,
                    scope="fc{}".format(i))
                i += 1
            output = slim.fully_connected(
                last_layer, num_outputs,
                weights_initializer=normc_initializer(0.01),
                activation_fn=None, scope="fc_out")
            if options.get("free_logstd", False):
                logstd = tf.get_variable(name="logstd", shape=[num_outputs],
                                         initializer=tf.zeros_initializer)
                output = tf.concat([output, 0.0 * output + logstd], 1)
            return output, last_layer
