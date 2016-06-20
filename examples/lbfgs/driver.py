import numpy as np
import scipy.optimize
import os
import time
import ray
import ray.services as services
import ray.worker as worker

import ray.arrays.remote as ra
import ray.arrays.distributed as da

import functions

from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)

batch_size = 100
num_batches = mnist.train.num_examples / batch_size
batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]

if __name__ == "__main__":
  worker_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "worker.py")
  services.start_singlenode_cluster(return_drivers=False, num_workers_per_objstore=16, worker_path=worker_path)

  x_batches = [ray.push(batches[i][0]) for i in range(num_batches)]
  y_batches = [ray.push(batches[i][1]) for i in range(num_batches)]

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
    theta_ref = ray.push(theta)
    val_ref = ra.sum_list(*[functions.loss(theta_ref, x_batches[i], y_batches[i]) for i in range(num_batches)])
    return ray.pull(val_ref)

  def full_grad(theta):
    theta_ref = ray.push(theta)
    grad_ref = ra.sum_list(*[functions.grad(theta_ref, x_batches[i], y_batches[i]) for i in range(num_batches)])
    return ray.pull(grad_ref).astype("float64") # This conversion is necessary for use with fmin_l_bfgs_b.

  theta_init = np.zeros(functions.dim)

  start_time = time.time()
  result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, maxiter=10, fprime=full_grad, disp=True)
  end_time = time.time()
  print "Elapsed time = {}".format(end_time - start_time)

  services.cleanup()
