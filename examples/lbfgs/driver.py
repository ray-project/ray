import ray
import argparse

import numpy as np
import scipy.optimize
import tensorflow as tf

from tensorflow.examples.tutorials.mnist import input_data

parser = argparse.ArgumentParser(description="Run the L-BFGS example.")
parser.add_argument("--node-ip-address", default=None, type=str, help="The IP address of this node.")
parser.add_argument("--scheduler-address", default=None, type=str, help="The address of the scheduler.")

if __name__ == "__main__":
  args = parser.parse_args()

  # If node_ip_address and scheduler_address are provided, then this command
  # will connect the driver to the existing scheduler. If not, it will start
  # a local scheduler and connect to it.
  ray.init(start_ray_local=(args.node_ip_address is None),
           node_ip_address=args.node_ip_address,
           scheduler_address=args.scheduler_address,
           num_workers=(10 if args.node_ip_address is None else None))

  # Define the dimensions of the data and of the model.
  image_dimension = 784
  label_dimension = 10
  w_shape = [image_dimension, label_dimension]
  w_size = np.prod(w_shape)
  b_shape = [label_dimension]
  b_size = np.prod(b_shape)
  dim = w_size + b_size

  # Define a function for initializing the network. Note that this code does not
  # call initialize the network weights. If it did, the weights would be
  # randomly initialized on each worker and would differ from worker to worker.
  # We pass the weights into the remote functions loss and grad so that the
  # weights are the same on each worker.
  def net_initialization():
    x = tf.placeholder(tf.float32, [None, image_dimension])
    w = tf.Variable(tf.zeros(w_shape))
    b = tf.Variable(tf.zeros(b_shape))
    y = tf.nn.softmax(tf.matmul(x, w) + b)
    y_ = tf.placeholder(tf.float32, [None, label_dimension])
    cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))
    cross_entropy_grads = tf.gradients(cross_entropy, [w, b])

    w_new = tf.placeholder(tf.float32, w_shape)
    b_new = tf.placeholder(tf.float32, b_shape)
    update_w = w.assign(w_new)
    update_b = b.assign(b_new)

    sess = tf.Session()

    return sess, update_w, update_b, cross_entropy, cross_entropy_grads, x, y_, w_new, b_new

  # By default, when a reusable variable is used by a remote function, the
  # initialization code will be rerun at the end of the remote task to ensure
  # that the state of the variable is not changed by the remote task. However,
  # the initialization code may be expensive. This case is one example, because
  # a TensorFlow network is constructed. In this case, we pass in a special
  # reinitialization function which gets run instead of the original
  # initialization code. As users, if we pass in custom reinitialization code,
  # we must ensure that no state is leaked between tasks.
  def net_reinitialization(net_vars):
    return net_vars

  # Create a reusable variable for the network.
  ray.reusables.net_vars = ray.Reusable(net_initialization, net_reinitialization)

  # Load the weights into the network.
  def load_weights(theta):
    sess, update_w, update_b, _, _, _, _, w_new, b_new = ray.reusables.net_vars
    sess.run([update_w, update_b], feed_dict={w_new: theta[:w_size].reshape(w_shape), b_new: theta[w_size:]})

  # Compute the loss on a batch of data.
  @ray.remote
  def loss(theta, xs, ys):
    sess, _, _, cross_entropy, _, x, y_, _, _ = ray.reusables.net_vars
    load_weights(theta)
    return float(sess.run(cross_entropy, feed_dict={x: xs, y_: ys}))

  # Compute the gradient of the loss on a batch of data.
  @ray.remote
  def grad(theta, xs, ys):
    sess, _, _, _, cross_entropy_grads, x, y_, _, _ = ray.reusables.net_vars
    load_weights(theta)
    gradients = sess.run(cross_entropy_grads, feed_dict={x: xs, y_: ys})
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
  print "Downloading the MNIST dataset. This may take a minute."
  mnist = input_data.read_data_sets("MNIST_data", one_hot=True)
  batch_size = 100
  num_batches = mnist.train.num_examples / batch_size
  batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]
  batch_ids = [(ray.put(xs), ray.put(ys)) for (xs, ys) in batches]

  # Initialize the weights for the network to the vector of all zeros.
  theta_init = 1e-2 * np.random.normal(size=dim)
  # Use L-BFGS to minimize the loss function.
  result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, maxiter=10, fprime=full_grad, disp=True)
