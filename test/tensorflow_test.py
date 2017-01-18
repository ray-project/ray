from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import uuid
import tensorflow as tf
import ray
from numpy.testing import assert_almost_equal

def net_vars_initializer():
  # Random prefix so variable names do not clash if we use nets with
  # the same name.
  prefix = str(uuid.uuid1().hex)
  # Use the tensorflow variable_scope to prefix all of the variables
  with tf.variable_scope(prefix):
    # Define the inputs.
    x_data = tf.placeholder(tf.float32, shape=[100])
    y_data = tf.placeholder(tf.float32, shape=[100])
    # Define the weights and computation.
    w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
    b = tf.Variable(tf.zeros([1]))
    y = w * x_data + b
    # Define the loss.
    loss = tf.reduce_mean(tf.square(y - y_data))
    # Define the weight initializer and session.
    init = tf.global_variables_initializer()
    sess = tf.Session()
    # Additional code for setting and getting the weights.
    variables = ray.experimental.TensorFlowVariables(loss, sess, prefix=True)
    # Initialize the session.
    sess.run(init)
  # Return all of the data needed to use the network.
  return variables

def net_vars_reinitializer(net_vars):
  return net_vars

class TensorFlowTest(unittest.TestCase):

  def testTensorFlowVariables(self):
    ray.init(num_workers=2)

    x_data = tf.placeholder(tf.float32, shape=[100])
    y_data = tf.placeholder(tf.float32, shape=[100])

    w = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
    b = tf.Variable(tf.zeros([1]))
    y = w * x_data + b
    loss = tf.reduce_mean(tf.square(y - y_data))

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    variables = ray.experimental.TensorFlowVariables(loss, sess)
    weights = variables.get_weights()

    for (name, val) in weights.items():
      weights[name] += 1.0

    variables.set_weights(weights)
    self.assertEqual(weights, variables.get_weights())

    w2 = tf.Variable(tf.random_uniform([1], -1.0, 1.0), name="w")
    b2 = tf.Variable(tf.zeros([1]), name="b")
    y2 = w2 * x_data + b2
    loss2 = tf.reduce_mean(tf.square(y2 - y_data))

    sess.run(tf.global_variables_initializer())

    variables2 = ray.experimental.TensorFlowVariables(loss2, sess)
    weights2 = variables2.get_weights()

    for (name, val) in weights2.items():
      weights2[name] += 2.0

    variables2.set_weights(weights2)
    self.assertEqual(weights2, variables2.get_weights())

    flat_weights = variables2.get_flat() + 2.0
    variables2.set_flat(flat_weights)
    assert_almost_equal(flat_weights, variables2.get_flat())

    variables3 = ray.experimental.TensorFlowVariables(loss2)
    self.assertEqual(variables3.sess, None)
    sess = tf.Session()
    variables3.set_session(sess)
    self.assertEqual(variables3.sess, sess)

    ray.worker.cleanup()

  def testTensorFlowVariableCollision(self):
    ray.init(num_workers=2)

    ray.env.net_vars1 = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)
    ray.env.net_vars2 = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)

    ray.env.net_vars1.set_weights(ray.env.net_vars2.get_weights())

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
