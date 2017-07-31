from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf
import tensorflow.contrib.slim as slim

import numpy as np 

from ray.rllib.models.model import Model
from ray.rllib.models.misc import normc_initializer


def conv2d(x, num_filters, name, filter_size=(3, 3), stride=(1, 1), pad="SAME",
           dtype=tf.float32, collections=None):
    with tf.variable_scope(name):
        stride_shape = [1, stride[0], stride[1], 1]
        filter_shape = [filter_size[0], filter_size[1], int(x.get_shape()[3]),
                        num_filters]

        # There are "num input feature maps * filter height * filter width"
        # inputs to each hidden unit.
        fan_in = np.prod(filter_shape[:3])
        # Each unit in the lower layer receives a gradient from: "num output
        # feature maps * filter height * filter width" / pooling size.
        fan_out = np.prod(filter_shape[:2]) * num_filters
        # Initialize weights with random weights.
        w_bound = np.sqrt(6 / (fan_in + fan_out))

        w = tf.get_variable("W", filter_shape, dtype,
                            tf.random_uniform_initializer(-w_bound, w_bound),
                            collections=collections)
        b = tf.get_variable("b", [1, 1, 1, num_filters],
                            initializer=tf.constant_initializer(0.0),
                            collections=collections)
        return tf.nn.conv2d(x, w, stride_shape, pad) + b


def linear(x, size, name, initializer=None, bias_init=0):
    w = tf.get_variable(name + "/w", [x.get_shape()[1], size],
                        initializer=initializer)
    b = tf.get_variable(name + "/b", [size],
                        initializer=tf.constant_initializer(bias_init))
    return tf.matmul(x, w) + b


class A3CConvolutionalNetwork(Model):
    """Generic convolutional network."""

    def _init(self, inputs, num_outputs, options):
        x = inputs
        with tf.name_scope("a3c_convnet"):
            for i in range(4):
                x = tf.nn.elu(conv2d(x, 32, "l{}".format(i + 1), [3, 3], [2, 2]))
            r, c = x.shape[1].value, x.shape[2].value
            x = tf.reshape(x, [-1, r*c*32])
            fc1 = linear(x, 256, "fc1")
            fc2 = linear(x, num_outputs, "fc2", normc_initializer(0.01))
            return fc2, fc1
