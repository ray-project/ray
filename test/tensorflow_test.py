from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import tensorflow as tf
import ray

class TensorFlowTest(unittest.TestCase):

  def testTensorFlowVariables(self):
    ray.init(start_ray_local=True, num_workers=2)

    x_data = tf.placeholder(tf.float32, shape=[100])
    y_data = tf.placeholder(tf.float32, shape=[100])

    w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
    b = tf.Variable(tf.zeros([1]))
    y = w * x_data + b

    loss = tf.reduce_mean(tf.square(y - y_data))

    init = tf.global_variables_initializer()
    sess = tf.Session()
    sess.run(init)

    variables = ray.experimental.TensorFlowVariables(loss, sess)
    weights = variables.get_weights()

    for (name, val) in weights.items():
      weights[name] += 1.0

    variables.set_weights(weights)
    self.assertEqual(weights, variables.get_weights())

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
