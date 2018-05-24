from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer


class LinearNetwork(Model):
    """Generic fully connected network."""

    def _init(self, inputs, num_outputs, options):
        with tf.name_scope("linear"):
            label = "linear_out"
            output = slim.fully_connected(
                inputs, num_outputs,
                weights_initializer=normc_initializer(0.01),
                activation_fn=None, scope=label)
            return output, inputs
