import os

import numpy as np
import scipy.optimize
import tensorflow as tf
from tensorflow.examples.tutorials.mnist import input_data

import ray
import ray.experimental.tf_utils


class LinearModel(object):
    """Simple class for a one layer neural network.

    Note that this code does not initialize the network weights. Instead
    weights are set via self.variables.set_weights.

    Example:
        net = LinearModel([10, 10])
        weights = [np.random.normal(size=[10, 10]),
                   np.random.normal(size=[10])]
        variable_names = [v.name for v in net.variables]
        net.variables.set_weights(dict(zip(variable_names, weights)))

    Attributes:
        x (tf.placeholder): Input vector.
        w (tf.Variable): Weight matrix.
        b (tf.Variable): Bias vector.
        y_ (tf.placeholder): Input result vector.
        cross_entropy (tf.Operation): Final layer of network.
        cross_entropy_grads (tf.Operation): Gradient computation.
        sess (tf.Session): Session used for training.
        variables: Extracted variables and methods to
            manipulate them.
    """

    def __init__(self, shape):
        """Creates a LinearModel object."""
        x = tf.placeholder(tf.float32, [None, shape[0]])
        w = tf.Variable(tf.zeros(shape))
        b = tf.Variable(tf.zeros(shape[1]))
        self.x = x
        self.w = w
        self.b = b
        y = tf.nn.softmax(tf.matmul(x, w) + b)
        y_ = tf.placeholder(tf.float32, [None, shape[1]])
        self.y_ = y_
        cross_entropy = tf.reduce_mean(
            -tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1])
        )
        self.cross_entropy = cross_entropy
        self.cross_entropy_grads = tf.gradients(cross_entropy, [w, b])
        self.sess = tf.Session()
        # In order to get and set the weights, we pass in the loss function to
        # Ray's TensorFlowVariables to automatically create methods to modify
        # the weights.
        self.variables = ray.experimental.tf_utils.TensorFlowVariables(
            cross_entropy, self.sess
        )

    def loss(self, xs, ys):
        """Computes the loss of the network."""
        return float(
            self.sess.run(self.cross_entropy, feed_dict={self.x: xs, self.y_: ys})
        )

    def grad(self, xs, ys):
        """Computes the gradients of the network."""
        return self.sess.run(
            self.cross_entropy_grads, feed_dict={self.x: xs, self.y_: ys}
        )


@ray.remote
class NetActor(object):
    def __init__(self, xs, ys):
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
        with tf.device("/cpu:0"):
            self.net = LinearModel([784, 10])
            self.xs = xs
            self.ys = ys

    # Compute the loss on a batch of data.
    def loss(self, theta):
        net = self.net
        net.variables.set_flat(theta)
        return net.loss(self.xs, self.ys)

    # Compute the gradient of the loss on a batch of data.
    def grad(self, theta):
        net = self.net
        net.variables.set_flat(theta)
        gradients = net.grad(self.xs, self.ys)
        return np.concatenate([g.flatten() for g in gradients])

    def get_flat_size(self):
        return self.net.variables.get_flat_size()


# Compute the loss on the entire dataset.
def full_loss(theta):
    theta_id = ray.put(theta)
    loss_ids = [actor.loss.remote(theta_id) for actor in actors]
    return sum(ray.get(loss_ids))


# Compute the gradient of the loss on the entire dataset.
def full_grad(theta):
    theta_id = ray.put(theta)
    grad_ids = [actor.grad.remote(theta_id) for actor in actors]
    # The float64 conversion is necessary for use with fmin_l_bfgs_b.
    return sum(ray.get(grad_ids)).astype("float64")


if __name__ == "__main__":
    ray.init()

    # From the perspective of scipy.optimize.fmin_l_bfgs_b, full_loss is simply
    # a function which takes some parameters theta, and computes a loss.
    # Similarly, full_grad is a function which takes some parameters theta, and
    # computes the gradient of the loss. Internally, these functions use Ray to
    # distribute the computation of the loss and the gradient over the data
    # that is represented by the remote object refs x_batches and y_batches and
    # which is potentially distributed over a cluster. However, these details
    # are hidden from scipy.optimize.fmin_l_bfgs_b, which simply uses it to run
    # the L-BFGS algorithm.

    # Load the mnist data and turn the data into remote objects.
    print("Downloading the MNIST dataset. This may take a minute.")
    mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
    num_batches = 10
    batch_size = mnist.train.num_examples // num_batches
    batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]
    print("Putting MNIST in the object store.")
    actors = [NetActor.remote(xs, ys) for (xs, ys) in batches]
    # Initialize the weights for the network to the vector of all zeros.
    dim = ray.get(actors[0].get_flat_size.remote())
    theta_init = 1e-2 * np.random.normal(size=dim)

    # Use L-BFGS to minimize the loss function.
    print("Running L-BFGS.")
    result = scipy.optimize.fmin_l_bfgs_b(
        full_loss, theta_init, maxiter=10, fprime=full_grad, disp=True
    )
