import numpy as np
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


def normc_initializer(std=1.0):
    def _initializer(shape, dtype=None, partition_info=None):
        out = np.random.randn(*shape).astype(np.float32)
        out *= std / np.sqrt(np.square(out).sum(axis=0, keepdims=True))
        return tf.constant(out)

    return _initializer


def conv2d(x,
           num_filters,
           name,
           filter_size=(3, 3),
           stride=(1, 1),
           pad="SAME",
           dtype=None,
           collections=None):
    if dtype is None:
        dtype = tf.float32

    with tf.variable_scope(name):
        stride_shape = [1, stride[0], stride[1], 1]
        filter_shape = [
            filter_size[0], filter_size[1],
            int(x.get_shape()[3]), num_filters
        ]

        # There are "num input feature maps * filter height * filter width"
        # inputs to each hidden unit.
        fan_in = np.prod(filter_shape[:3])
        # Each unit in the lower layer receives a gradient from: "num output
        # feature maps * filter height * filter width" / pooling size.
        fan_out = np.prod(filter_shape[:2]) * num_filters
        # Initialize weights with random weights.
        w_bound = np.sqrt(6 / (fan_in + fan_out))

        w = tf.get_variable(
            "W",
            filter_shape,
            dtype,
            tf.random_uniform_initializer(-w_bound, w_bound),
            collections=collections)
        b = tf.get_variable(
            "b", [1, 1, 1, num_filters],
            initializer=tf.constant_initializer(0.0),
            collections=collections)
        return tf.nn.conv2d(x, w, stride_shape, pad) + b


def linear(x, size, name, initializer=None, bias_init=0):
    w = tf.get_variable(
        name + "/w", [x.get_shape()[1], size], initializer=initializer)
    b = tf.get_variable(
        name + "/b", [size], initializer=tf.constant_initializer(bias_init))
    return tf.matmul(x, w) + b


def flatten(x):
    return tf.reshape(x, [-1, np.prod(x.get_shape().as_list()[1:])])
