from numpy.testing import assert_almost_equal

import ray
import ray.experimental.tf_utils
from ray.rllib.utils.framework import try_import_tf

tf, _, _ = try_import_tf()


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


class LossActor:
    def __init__(self, use_loss=True):
        # Uses a separate graph for each network.
        with tf.Graph().as_default():
            # Create the network.
            var = [tf.Variable(1)]
            loss, init, _, _ = make_linear_network()
            sess = tf.Session()
            # Additional code for setting and getting the weights.
            weights = ray.experimental.tf_utils.TensorFlowVariables(
                loss if use_loss else None, sess, input_variables=var)
        # Return all of the data needed to use the network.
        self.values = [weights, init, sess]
        sess.run(init)

    def set_and_get_weights(self, weights):
        self.values[0].set_weights(weights)
        return self.values[0].get_weights()

    def get_weights(self):
        return self.values[0].get_weights()


class NetActor:
    def __init__(self):
        # Uses a separate graph for each network.
        with tf.Graph().as_default():
            # Create the network.
            loss, init, _, _ = make_linear_network()
            sess = tf.Session()
            # Additional code for setting and getting the weights.
            variables = ray.experimental.tf_utils.TensorFlowVariables(
                loss, sess)
        # Return all of the data needed to use the network.
        self.values = [variables, init, sess]
        sess.run(init)

    def set_and_get_weights(self, weights):
        self.values[0].set_weights(weights)
        return self.values[0].get_weights()

    def get_weights(self):
        return self.values[0].get_weights()


class TrainActor:
    def __init__(self):
        # Almost the same as above, but now returns the placeholders and
        # gradient.
        with tf.Graph().as_default():
            loss, init, x_data, y_data = make_linear_network()
            sess = tf.Session()
            variables = ray.experimental.tf_utils.TensorFlowVariables(
                loss, sess)
            optimizer = tf.train.GradientDescentOptimizer(0.9)
            grads = optimizer.compute_gradients(loss)
            train = optimizer.apply_gradients(grads)
        self.values = [
            loss, variables, init, sess, grads, train, [x_data, y_data]
        ]
        sess.run(init)

    def training_step(self, weights):
        _, variables, _, sess, grads, _, placeholders = self.values
        variables.set_weights(weights)
        return sess.run(
            [grad[0] for grad in grads],
            feed_dict=dict(zip(placeholders, [[1] * 100, [2] * 100])))

    def get_weights(self):
        return self.values[1].get_weights()


def test_tensorflow_variables(ray_start_2_cpus):
    sess = tf.Session()
    loss, init, _, _ = make_linear_network()
    sess.run(init)

    variables = ray.experimental.tf_utils.TensorFlowVariables(loss, sess)
    weights = variables.get_weights()

    for (name, val) in weights.items():
        weights[name] += 1.0

    variables.set_weights(weights)
    assert weights == variables.get_weights()

    loss2, init2, _, _ = make_linear_network("w", "b")
    sess.run(init2)

    variables2 = ray.experimental.tf_utils.TensorFlowVariables(loss2, sess)
    weights2 = variables2.get_weights()

    for (name, val) in weights2.items():
        weights2[name] += 2.0

    variables2.set_weights(weights2)
    assert weights2 == variables2.get_weights()
    flat_weights = variables2.get_flat() + 2.0
    variables2.set_flat(flat_weights)
    assert_almost_equal(flat_weights, variables2.get_flat())

    sess = tf.Session()
    variables3 = ray.experimental.tf_utils.TensorFlowVariables(
        [loss2], sess=sess)
    assert variables3.sess == sess


# Test that the variable names for the two different nets are not
# modified by TensorFlow to be unique (i.e., they should already
# be unique because of the variable prefix).
def test_variable_name_collision(ray_start_2_cpus):
    net1 = NetActor()
    net2 = NetActor()

    # This is checking that the variable names of the two nets are the
    # same, i.e., that the names in the weight dictionaries are the same.
    net1.values[0].set_weights(net2.values[0].get_weights())


# Test that TensorFlowVariables can take in addition variables through
# input_variables arg and with no loss.
def test_additional_variables_no_loss(ray_start_2_cpus):
    net = LossActor(use_loss=False)
    assert len(net.values[0].variables.items()) == 1
    assert len(net.values[0].placeholders.items()) == 1

    net.values[0].set_weights(net.values[0].get_weights())


# Test that TensorFlowVariables can take in addition variables through
# input_variables arg and with a loss.
def test_additional_variables_with_loss(ray_start_2_cpus):
    net = LossActor()
    assert len(net.values[0].variables.items()) == 3
    assert len(net.values[0].placeholders.items()) == 3

    net.values[0].set_weights(net.values[0].get_weights())


# Test that different networks on the same worker are independent and
# we can get/set their weights without any interaction.
def test_networks_independent(ray_start_2_cpus):
    # Note we use only one worker to ensure that all of the remote
    # functions run on the same worker.
    net1 = NetActor()
    net2 = NetActor()

    # Make sure the two networks have different weights. TODO(rkn): Note
    # that equality comparisons of numpy arrays normally does not work.
    # This only works because at the moment they have size 1.
    weights1 = net1.get_weights()
    weights2 = net2.get_weights()
    assert weights1 != weights2

    # Set the weights and get the weights, and make sure they are
    # unchanged.
    new_weights1 = net1.set_and_get_weights(weights1)
    new_weights2 = net2.set_and_get_weights(weights2)
    assert weights1 == new_weights1
    assert weights2 == new_weights2

    # Swap the weights.
    new_weights1 = net2.set_and_get_weights(weights1)
    new_weights2 = net1.set_and_get_weights(weights2)
    assert weights1 == new_weights1
    assert weights2 == new_weights2


# This test creates an additional network on the driver so that the
# tensorflow variables on the driver and the worker differ.
def test_network_driver_worker_independent(ray_start_2_cpus):
    # Create a network on the driver locally.
    sess1 = tf.Session()
    loss1, init1, _, _ = make_linear_network()
    ray.experimental.tf_utils.TensorFlowVariables(loss1, sess1)
    sess1.run(init1)

    net2 = ray.remote(NetActor).remote()
    weights2 = ray.get(net2.get_weights.remote())

    new_weights2 = ray.get(
        net2.set_and_get_weights.remote(net2.get_weights.remote()))
    assert weights2 == new_weights2


def test_variables_control_dependencies(ray_start_2_cpus):
    # Creates a network and appends a momentum optimizer.
    sess = tf.Session()
    loss, init, _, _ = make_linear_network()
    minimizer = tf.train.MomentumOptimizer(0.9, 0.9).minimize(loss)
    net_vars = ray.experimental.tf_utils.TensorFlowVariables(minimizer, sess)
    sess.run(init)

    # Tests if all variables are properly retrieved, 2 variables and 2
    # momentum variables.
    assert len(net_vars.variables.items()) == 4


def test_remote_training_step(ray_start_2_cpus):
    net = ray.remote(TrainActor).remote()
    ray.get(net.training_step.remote(net.get_weights.remote()))


def test_remote_training_loss(ray_start_2_cpus):
    net = ray.remote(TrainActor).remote()
    net_values = TrainActor().values
    loss, variables, _, sess, grads, train, placeholders = net_values

    before_acc = sess.run(
        loss, feed_dict=dict(zip(placeholders, [[2] * 100, [4] * 100])))

    for _ in range(3):
        gradients_list = ray.get([
            net.training_step.remote(variables.get_weights()) for _ in range(2)
        ])
        mean_grads = [
            sum(gradients[i]
                for gradients in gradients_list) / len(gradients_list)
            for i in range(len(gradients_list[0]))
        ]
        feed_dict = {
            grad[0]: mean_grad
            for (grad, mean_grad) in zip(grads, mean_grads)
        }
        sess.run(train, feed_dict=feed_dict)
    after_acc = sess.run(
        loss, feed_dict=dict(zip(placeholders, [[2] * 100, [4] * 100])))
    assert before_acc < after_acc


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
