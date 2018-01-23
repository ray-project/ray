from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from numpy.testing import assert_almost_equal
import tensorflow as tf
import unittest

import ray


def make_linear_network(w_name=None, b_name=None):
    # Define the inputs.
    x_data = tf.placeholder(tf.float32, shape=[100])
    y_data = tf.placeholder(tf.float32, shape=[100])
    # Define the weights and computation.
    w = tf.Variable(tf.random_uniform([1], -1.0, 1.0), name=w_name)
    b = tf.Variable(tf.zeros([1]), name=b_name)
    y = w * x_data + b
    # Return the loss and weight initializer.
    return (tf.reduce_mean(tf.square(y - y_data)),
            tf.global_variables_initializer(), x_data, y_data)


class NetActor(object):

    def __init__(self):
        # Uses a separate graph for each network.
        with tf.Graph().as_default():
            # Create the network.
            loss, init, _, _ = make_linear_network()
            sess = tf.Session()
            # Additional code for setting and getting the weights.
            variables = ray.experimental.TensorFlowVariables(loss, sess)
        # Return all of the data needed to use the network.
        self.values = [variables, init, sess]
        sess.run(init)

    def set_and_get_weights(self, weights):
        self.values[0].set_weights(weights)
        return self.values[0].get_weights()

    def get_weights(self):
        return self.values[0].get_weights()


class TrainActor(object):

    def __init__(self):
        # Almost the same as above, but now returns the placeholders and
        # gradient.
        with tf.Graph().as_default():
            loss, init, x_data, y_data = make_linear_network()
            sess = tf.Session()
            variables = ray.experimental.TensorFlowVariables(loss, sess)
            optimizer = tf.train.GradientDescentOptimizer(0.9)
            grads = optimizer.compute_gradients(loss)
            train = optimizer.apply_gradients(grads)
        self.values = [loss, variables, init, sess, grads, train,
                       [x_data, y_data]]
        sess.run(init)

    def training_step(self, weights):
        _, variables, _, sess, grads, _, placeholders = self.values
        variables.set_weights(weights)
        return sess.run([grad[0] for grad in grads],
                        feed_dict=dict(zip(placeholders,
                                           [[1] * 100, [2] * 100])))

    def get_weights(self):
        return self.values[1].get_weights()


class TensorFlowTest(unittest.TestCase):
    def tearDown(self):
        ray.worker.cleanup()

    def testTensorFlowVariables(self):
        ray.init(num_workers=2)

        sess = tf.Session()
        loss, init, _, _ = make_linear_network()
        sess.run(init)

        variables = ray.experimental.TensorFlowVariables(loss, sess)
        weights = variables.get_weights()

        for (name, val) in weights.items():
            weights[name] += 1.0

        variables.set_weights(weights)
        self.assertEqual(weights, variables.get_weights())

        loss2, init2, _, _ = make_linear_network("w", "b")
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

    # Test that the variable names for the two different nets are not
    # modified by TensorFlow to be unique (i.e. they should already
    # be unique because of the variable prefix).
    def testVariableNameCollision(self):
        ray.init(num_workers=2)

        net1 = NetActor()
        net2 = NetActor()

        # This is checking that the variable names of the two nets are the
        # same, i.e. that the names in the weight dictionaries are the same
        net1.values[0].set_weights(net2.values[0].get_weights())

    # Test that different networks on the same worker are independent and
    # we can get/set their weights without any interaction.
    def testNetworksIndependent(self):
        # Note we use only one worker to ensure that all of the remote
        # functions run on the same worker.
        ray.init(num_workers=1)
        net1 = NetActor()
        net2 = NetActor()

        # Make sure the two networks have different weights. TODO(rkn): Note
        # that equality comparisons of numpy arrays normally does not work.
        # This only works because at the moment they have size 1.
        weights1 = net1.get_weights()
        weights2 = net2.get_weights()
        self.assertNotEqual(weights1, weights2)

        # Set the weights and get the weights, and make sure they are
        # unchanged.
        new_weights1 = net1.set_and_get_weights(weights1)
        new_weights2 = net2.set_and_get_weights(weights2)
        self.assertEqual(weights1, new_weights1)
        self.assertEqual(weights2, new_weights2)

        # Swap the weights.
        new_weights1 = net2.set_and_get_weights(weights1)
        new_weights2 = net1.set_and_get_weights(weights2)
        self.assertEqual(weights1, new_weights1)
        self.assertEqual(weights2, new_weights2)

    # This test creates an additional network on the driver so that the
    # tensorflow variables on the driver and the worker differ.
    def testNetworkDriverWorkerIndependent(self):
        ray.init(num_workers=1)

        # Create a network on the driver locally.
        sess1 = tf.Session()
        loss1, init1, _, _ = make_linear_network()
        ray.experimental.TensorFlowVariables(loss1, sess1)
        sess1.run(init1)

        net2 = ray.remote(NetActor).remote()
        weights2 = ray.get(net2.get_weights.remote())

        new_weights2 = ray.get(net2.set_and_get_weights.remote(
            net2.get_weights.remote()))
        self.assertEqual(weights2, new_weights2)

    def testVariablesControlDependencies(self):
        ray.init(num_workers=1)

        # Creates a network and appends a momentum optimizer.
        sess = tf.Session()
        loss, init, _, _ = make_linear_network()
        minimizer = tf.train.MomentumOptimizer(0.9, 0.9).minimize(loss)
        net_vars = ray.experimental.TensorFlowVariables(minimizer, sess)
        sess.run(init)

        # Tests if all variables are properly retrieved, 2 variables and 2
        # momentum variables.
        self.assertEqual(len(net_vars.variables.items()), 4)

    def testRemoteTrainingStep(self):
        ray.init(num_workers=1)

        net = ray.remote(TrainActor).remote()
        ray.get(net.training_step.remote(net.get_weights.remote()))

    def testRemoteTrainingLoss(self):
        ray.init(num_workers=2)

        net = ray.remote(TrainActor).remote()
        (loss, variables, _, sess, grads,
         train, placeholders) = TrainActor().values

        before_acc = sess.run(loss,
                              feed_dict=dict(zip(placeholders,
                                                 [[2] * 100, [4] * 100])))

        for _ in range(3):
            gradients_list = ray.get(
                [net.training_step.remote(variables.get_weights())
                 for _ in range(2)])
            mean_grads = [sum([gradients[i] for gradients in gradients_list]) /
                          len(gradients_list) for i
                          in range(len(gradients_list[0]))]
            feed_dict = {grad[0]: mean_grad for (grad, mean_grad)
                         in zip(grads, mean_grads)}
            sess.run(train, feed_dict=feed_dict)
        after_acc = sess.run(loss, feed_dict=dict(zip(placeholders,
                                                      [[2] * 100, [4] * 100])))
        self.assertTrue(before_acc < after_acc)


if __name__ == "__main__":
    unittest.main(verbosity=2)
