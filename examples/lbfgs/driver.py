from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray

import numpy as np
import scipy.optimize
import tensorflow as tf

from tensorflow.examples.tutorials.mnist import input_data

class LinearModel(object):
  """Simple class for a one layer neural network.

  Note that this code does not initialize the network weights. Instead weights
  are set via self.variables.set_weights.

  Example:
    net = LinearModel([10,10])
    weights = [np.random.normal(size=[10,10]), np.random.normal(size=[10])]
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
    variables (TensorFlowVariables): Extracted variables and methods to
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
    cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))
    self.cross_entropy = cross_entropy
    self.cross_entropy_grads = tf.gradients(cross_entropy, [w, b])
    self.sess = tf.Session()
    # In order to get and set the weights, we pass in the loss function to Ray's
    # TensorFlowVariables to automatically create methods to modify the weights.
    self.variables = ray.experimental.TensorFlowVariables(cross_entropy, self.sess)

  def loss(self, xs, ys):
    """Computes the loss of the network."""
    return float(self.sess.run(self.cross_entropy, feed_dict={self.x: xs, self.y_: ys}))

  def grad(self, xs, ys):
    """Computes the gradients of the network."""
    return self.sess.run(self.cross_entropy_grads, feed_dict={self.x: xs, self.y_: ys})

def net_initialization():
  with tf.Graph().as_default():
    return LinearModel([784,10])

# By default, when an environment variable is used by a remote function, the
# initialization code will be rerun at the end of the remote task to ensure
# that the state of the variable is not changed by the remote task. However,
# the initialization code may be expensive. This case is one example, because
# a TensorFlow network is constructed. In this case, we pass in a special
# reinitialization function which gets run instead of the original
# initialization code. As users, if we pass in custom reinitialization code,
# we must ensure that no state is leaked between tasks.
def net_reinitialization(net):
  return net

# Register the network with Ray and create an environment variable for it.
ray.env.net = ray.EnvironmentVariable(net_initialization, net_reinitialization)

# Compute the loss on a batch of data.
@ray.remote
def loss(theta, xs, ys):
  net = ray.env.net
  net.variables.set_flat(theta)
  return net.loss(xs,ys)

# Compute the gradient of the loss on a batch of data.
@ray.remote
def grad(theta, xs, ys):
  net = ray.env.net
  net.variables.set_flat(theta)
  gradients = net.grad(xs, ys)
  return np.concatenate([g.flatten() for g in gradients])

# Compute the loss on the entire dataset.
def full_loss(theta):
  theta_id = ray.put(theta)
  loss_ids = [loss.remote(theta_id, xs_id, ys_id) for (xs_id, ys_id) in batch_ids]
  return sum(ray.get(loss_ids))

# Compute the gradient of the loss on the entire dataset.
def full_grad(theta):
  theta_id = ray.put(theta)
  grad_ids = [grad.remote(theta_id, xs_id, ys_id) for (xs_id, ys_id) in batch_ids]
  return sum(ray.get(grad_ids)).astype("float64") # This conversion is necessary for use with fmin_l_bfgs_b.

if __name__ == "__main__":
  ray.init(num_workers=10)

  # From the perspective of scipy.optimize.fmin_l_bfgs_b, full_loss is simply a
  # function which takes some parameters theta, and computes a loss. Similarly,
  # full_grad is a function which takes some parameters theta, and computes the
  # gradient of the loss. Internally, these functions use Ray to distribute the
  # computation of the loss and the gradient over the data that is represented
  # by the remote object IDs x_batches and y_batches and which is potentially
  # distributed over a cluster. However, these details are hidden from
  # scipy.optimize.fmin_l_bfgs_b, which simply uses it to run the L-BFGS
  # algorithm.

  # Load the mnist data and turn the data into remote objects.
  print("Downloading the MNIST dataset. This may take a minute.")
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  batch_size = 100
  num_batches = mnist.train.num_examples // batch_size
  batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]
  print("Putting MNIST in the object store.")
  batch_ids = [(ray.put(xs), ray.put(ys)) for (xs, ys) in batches]

  # Initialize the weights for the network to the vector of all zeros.
  dim = ray.env.net.variables.get_flat_size()
  theta_init = 1e-2 * np.random.normal(size=dim)

  # Use L-BFGS to minimize the loss function.
  print("Running L-BFGS.")
  result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, maxiter=10, fprime=full_grad, disp=True)
