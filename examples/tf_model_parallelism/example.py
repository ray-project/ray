import numpy as np
import tensorflow as tf

import ray
from ray.experimental.tf_utils import tf_differentiable

tf.enable_eager_execution()

ray.init(num_cpus=2)


@ray.remote
class Actor(object):
    def __init__(self):
        tf.enable_eager_execution()

    @tf_differentiable
    def sum(self, *xs):
        value = 0
        for x in xs:
            value += x
        return value

    @tf_differentiable
    def square(self, x):
        return x**2


a = Actor.remote()

x = tf.Variable(np.arange(4, dtype=np.float32))
with tf.GradientTape() as t:
    w = x**2
    y1_id = a.sum.remote(x, w, 1)
    y2_id = a.square.remote(x)
    y3_id = a.square.remote(w)
    y4_id = a.square.remote(w)
    y5_id = a.square.remote(w)
    y6_id = a.square.remote(w)
    y7_id = a.square.remote(y1_id)
    y8_id = a.sum.remote(w, y2_id, y3_id, y4_id)
    y5, y6, y7 = ray.get([y5_id, y6_id, y7_id])
    y8 = ray.get(y8_id)
    loss = tf.reduce_sum(y5 + y6 + y7 + y8 + w + x)

t.gradient(loss, [x, w])

# Compare with non-Ray.


def sum_(*xs):
    value = 0
    for x in xs:
        value += x
    return value


def square(x):
    return x**2


x = tf.Variable(np.arange(4, dtype=np.float32))
with tf.GradientTape() as t:
    w = x**2
    y1 = sum_(x, w, 1)
    y2 = square(x)
    y3 = square(w)
    y4 = square(w)
    y5 = square(w)
    y6 = square(w)
    y7 = square(y1)
    y8 = sum_(w, y2, y3, y4)
    loss = tf.reduce_sum(y5 + y6 + y7 + y8 + w + x)

t.gradient(loss, [x, w])
