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
    """Generic fully connected network."""

    def _init(self, inputs, num_outputs):
        with tf.name_scope("fc_net"):
            fc1 = slim.fully_connected(
                inputs, 128, weights_initializer=normc_initializer(1.0),
                scope="fc1")
            fc2 = slim.fully_connected(
                fc1, 128, weights_initializer=normc_initializer(1.0),
                scope="fc2")
            fc3 = slim.fully_connected(
                fc2, 128, weights_initializer=normc_initializer(1.0),
                scope="fc3")
            fc4 = slim.fully_connected(
                fc3, num_outputs, weights_initializer=normc_initializer(0.01),
                activation_fn=None, scope="fc4")
            return fc4, fc3
