import numpy as np
from typing import Tuple, Any, Optional

from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()


def normc_initializer(std: float = 1.0) -> Any:
    def _initializer(shape, dtype=None, partition_info=None):
        out = np.random.randn(*shape).astype(np.float32)
        out *= std / np.sqrt(np.square(out).sum(axis=0, keepdims=True))
        return tf.constant(out)

    return _initializer


def conv2d(
    x: TensorType,
    num_filters: int,
    name: str,
    filter_size: Tuple[int, int] = (3, 3),
    stride: Tuple[int, int] = (1, 1),
    pad: str = "SAME",
    dtype: Optional[Any] = None,
    collections: Optional[Any] = None,
) -> TensorType:
    if dtype is None:
        dtype = tf.float32

    with tf1.variable_scope(name):
        stride_shape = [1, stride[0], stride[1], 1]
        filter_shape = [
            filter_size[0],
            filter_size[1],
            int(x.get_shape()[3]),
            num_filters,
        ]

        # There are "num input feature maps * filter height * filter width"
        # inputs to each hidden unit.
        fan_in = np.prod(filter_shape[:3])
        # Each unit in the lower layer receives a gradient from: "num output
        # feature maps * filter height * filter width" / pooling size.
        fan_out = np.prod(filter_shape[:2]) * num_filters
        # Initialize weights with random weights.
        w_bound = np.sqrt(6 / (fan_in + fan_out))

        w = tf1.get_variable(
            "W",
            filter_shape,
            dtype,
            tf1.random_uniform_initializer(-w_bound, w_bound),
            collections=collections,
        )
        b = tf1.get_variable(
            "b",
            [1, 1, 1, num_filters],
            initializer=tf1.constant_initializer(0.0),
            collections=collections,
        )
        return tf1.nn.conv2d(x, w, stride_shape, pad) + b


def linear(
    x: TensorType,
    size: int,
    name: str,
    initializer: Optional[Any] = None,
    bias_init: float = 0.0,
) -> TensorType:
    w = tf1.get_variable(name + "/w", [x.get_shape()[1], size], initializer=initializer)
    b = tf1.get_variable(
        name + "/b", [size], initializer=tf1.constant_initializer(bias_init)
    )
    return tf.matmul(x, w) + b


def flatten(x: TensorType) -> TensorType:
    return tf.reshape(x, [-1, np.prod(x.get_shape().as_list()[1:])])
