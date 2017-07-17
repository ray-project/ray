from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model


class VisionNetwork(Model):
    """Generic vision network."""

    def _init(self, inputs, num_outputs):
        with tf.name_scope("vision_net"):
            conv1 = slim.conv2d(inputs, 16, [8, 8], 4, scope="conv1")
            conv2 = slim.conv2d(conv1, 32, [4, 4], 2, scope="conv2")
            fc1 = slim.conv2d(
                conv2, 512, [10, 10], padding="VALID", scope="fc1")
            fc2 = slim.conv2d(fc1, num_outputs, [1, 1], activation_fn=None,
                              normalizer_fn=None, scope="fc2")
            return tf.squeeze(fc2, [1, 2])
