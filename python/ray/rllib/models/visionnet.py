from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

from ray.rllib.models.model import Model
from ray.rllib.models.misc import get_activation_fn


class VisionNetwork(Model):
    """Generic vision network."""

    def _build_layers(self, inputs, num_outputs, options):
        filters = options.get("conv_filters", [
            [16, [8, 8], 4],
            [32, [4, 4], 2],
            [512, [10, 10], 1],
        ])

        activation = get_activation_fn(options.get("conv_activation", "relu"))

        with tf.name_scope("vision_net"):
            for i, (out_size, kernel, stride) in enumerate(filters[:-1], 1):
                inputs = slim.conv2d(
                    inputs, out_size, kernel, stride,
                    activation_fn=activation, scope="conv{}".format(i))
            out_size, kernel, stride = filters[-1]
            fc1 = slim.conv2d(
                inputs, out_size, kernel, stride,
                activation_fn=activation, padding="VALID", scope="fc1")
            fc2 = slim.conv2d(fc1, num_outputs, [1, 1], activation_fn=None,
                              normalizer_fn=None, scope="fc2")
            return tf.squeeze(fc2, [1, 2]), tf.squeeze(fc1, [1, 2])
