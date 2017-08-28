from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer, conv2d, linear


class ConvolutionalNetwork(Model):
    """Generic convolutional network."""
    # TODO(rliaw): converge on one generic ConvNet model
    def _init(self, inputs, num_outputs, options):
        x = inputs
        with tf.name_scope("convnet"):
            for i in range(4):
                x = tf.nn.elu(conv2d(x, 32, "l{}".format(i+1), [3, 3], [2, 2]))
            r, c = x.shape[1].value, x.shape[2].value
            x = tf.reshape(x, [-1, r*c*32])
            fc1 = linear(x, 256, "fc1")
            fc2 = linear(x, num_outputs, "fc2", normc_initializer(0.01))
            return fc2, fc1
