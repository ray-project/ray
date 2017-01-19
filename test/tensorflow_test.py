from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import uuid
import tensorflow as tf
import ray
from numpy.testing import assert_almost_equal

def make_linear_network(w_name=None, b_name=None):
  # Define the inputs.
  x_data = tf.placeholder(tf.float32, shape=[100])
  y_data = tf.placeholder(tf.float32, shape=[100])
  # Define the weights and computation.
  w = tf.Variable(tf.random_uniform([1], -1.0, 1.0), name=w_name)
  b = tf.Variable(tf.zeros([1]), name=b_name)
  y = w * x_data + b
  # Return the loss and weight initializer.
  return tf.reduce_mean(tf.square(y - y_data)), tf.global_variables_initializer()

def net_vars_initializer():
  # Random prefix so variable names do not clash if we use nets with
  # the same name.
  prefix = str(uuid.uuid1().hex)
  # Use the tensorflow variable_scope to prefix all of the variables
  with tf.variable_scope(prefix):
    # Create the network.
    loss, init = make_linear_network()
    sess = tf.Session()
    # Additional code for setting and getting the weights.
    variables = ray.experimental.TensorFlowVariables(loss, sess, prefix=True)
  # Return all of the data needed to use the network.
  return variables, init, sess

def net_vars_reinitializer(net_vars):
  return net_vars

class TensorFlowTest(unittest.TestCase):

  def testTensorFlowVariables(self):
    ray.init(num_workers=2)

    sess = tf.Session()
    loss, init = make_linear_network()
    sess.run(init)

    variables = ray.experimental.TensorFlowVariables(loss, sess)
    weights = variables.get_weights()

    for (name, val) in weights.items():
      weights[name] += 1.0

    variables.set_weights(weights)
    self.assertEqual(weights, variables.get_weights())

    loss2, init2 = make_linear_network("w", "b")
    sess.run(init2)

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

  # Test that the variable names for the two different nets are not
  # modified by TensorFlow to be unique (i.e. they should already
  # be unique because of the variable prefix).
  def testVariableNameCollision(self):
    ray.init(num_workers=2)

    ray.env.net1 = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)
    ray.env.net2 = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)
    
    net_vars1, init1, sess1 = ray.env.net1
    net_vars2, init2, sess2 = ray.env.net2

    # Initialize the networks
    sess1.run(init1)
    sess2.run(init2)

    # This is checking that the variable names of the two nets are the same,
    # i.e. that the names in the weight dictionaries are the same
    ray.env.net1[0].set_weights(ray.env.net2[0].get_weights())

    ray.worker.cleanup()

  # Test that different networks on the same worker are independent and
  # we can get/set their weights without any interaction.
  def testNetworksIndependent(self):
    # Note we use only one worker to ensure that all of the remote functions run on the same worker.
    ray.init(num_workers=1)

    ray.env.net1 = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)
    ray.env.net2 = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)
    
    net_vars1, init1, sess1 = ray.env.net1
    net_vars2, init2, sess2 = ray.env.net2

    # Initialize the networks
    sess1.run(init1)
    sess2.run(init2)

    @ray.remote
    def get_vars1():
      return ray.env.net1[0].get_weights()

    @ray.remote
    def get_vars2():
      return ray.env.net2[0].get_weights()

    @ray.remote
    def set_vars1(weights):
      ray.env.net1[0].set_weights(weights)

    @ray.remote
    def set_vars2(weights):
      ray.env.net2[0].set_weights(weights)

    # Get the weights.
    weights1 = net_vars1.get_weights()
    weights2 = net_vars2.get_weights()
    self.assertNotEqual(weights1, weights2)

    # Swap the weights.
    set_vars2.remote(weights1)
    set_vars1.remote(weights2)

    # Get the new weights.
    new_weights1 = ray.get(get_vars1.remote())
    new_weights2 = ray.get(get_vars2.remote())
    self.assertNotEqual(new_weights1, new_weights2)

    # Check that the weights were swapped.
    self.assertEqual(weights1, new_weights2)
    self.assertEqual(weights2, new_weights1)

    ray.worker.cleanup()

  def testNetworkDriverWorkerIndependent(self):
    ray.init(num_workers=1)

    # Create a network on the driver locally.
    sess1 = tf.Session()
    loss1, init1 = make_linear_network()
    net_vars1 = ray.experimental.TensorFlowVariables(loss1, sess1)
    sess1.run(init1)

    # Create a network on the driver via an environment variable.
    ray.env.net = ray.EnvironmentVariable(net_vars_initializer, net_vars_reinitializer)

    net_vars2, init2, sess2 = ray.env.net
    sess2.run(init2)

    # Get the weights.
    weights1 = net_vars1.get_weights()
    weights2 = net_vars2.get_weights()
    self.assertNotEqual(weights1, weights2)

    # Swap the weights.
    net_vars1.set_weights(weights2)
    net_vars2.set_weights(weights1)

    # Get the new weights.
    new_weights1 = net_vars1.get_weights()
    new_weights2 = net_vars2.get_weights()
    self.assertNotEqual(new_weights1, new_weights2)

    # Check that the weights were swapped.
    self.assertEqual(weights1, new_weights2)
    self.assertEqual(weights2, new_weights1)

    ray.worker.cleanup()

if __name__ == "__main__":
  unittest.main(verbosity=2)
