import numpy as np
import scipy.optimize
import os
import ray

import functions

from tensorflow.examples.tutorials.mnist import input_data

if __name__ == "__main__":
  worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")
  ray.services.start_ray_local(num_workers=16, worker_path=worker_path)

  print "Downloading and loading MNIST data..."
  mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)

  batch_size = 100
  num_batches = mnist.train.num_examples / batch_size
  batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]

  batch_refs = [(ray.put(xs), ray.put(ys)) for (xs, ys) in batches]

  # From the perspective of scipy.optimize.fmin_l_bfgs_b, full_loss is simply a
  # function which takes some parameters theta, and computes a loss. Similarly,
  # full_grad is a function which takes some parameters theta, and computes the
  # gradient of the loss. Internally, these functions use Ray to distribute the
  # computation of the loss and the gradient over the data that is represented
  # by the remote object references is x_batches and y_batches and which is
  # potentially distributed over a cluster. However, these details are hidden
  # from scipy.optimize.fmin_l_bfgs_b, which simply uses it to run the L-BFGS
  # algorithm.
  def full_loss(theta):
    theta_ref = ray.put(theta)
    loss_refs = [functions.loss(theta_ref, xs_ref, ys_ref) for (xs_ref, ys_ref) in batch_refs]
    return sum([ray.get(loss_ref) for loss_ref in loss_refs])

  def full_grad(theta):
    theta_ref = ray.put(theta)
    grad_refs = [functions.grad(theta_ref, xs_ref, ys_ref) for (xs_ref, ys_ref) in batch_refs]
    return sum([ray.get(grad_ref) for grad_ref in grad_refs]).astype("float64") # This conversion is necessary for use with fmin_l_bfgs_b.

  theta_init = np.zeros(functions.dim)
  result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, maxiter=10, fprime=full_grad, disp=True)
