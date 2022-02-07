Batch L-BFGS
============

This document provides a walkthrough of the L-BFGS example. To run the
application, first install these dependencies.

.. code-block:: bash

  pip install tensorflow
  pip install scipy

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/source/ray-core/_examples/lbfgs

Then you can run the example as follows.

.. code-block:: bash

  python ray/doc/source/ray-core/_examples/lbfgs/driver.py


Optimization is at the heart of many machine learning algorithms. Much of
machine learning involves specifying a loss function and finding the parameters
that minimize the loss. If we can compute the gradient of the loss function,
then we can apply a variety of gradient-based optimization algorithms. L-BFGS is
one such algorithm. It is a quasi-Newton method that uses gradient information
to approximate the inverse Hessian of the loss function in a computationally
efficient manner.

The serial version
------------------

First we load the data in batches. Here, each element in ``batches`` is a tuple
whose first component is a batch of ``100`` images and whose second component is a
batch of the ``100`` corresponding labels. For simplicity, we use TensorFlow's
built in methods for loading the data.

.. code-block:: python

  from tensorflow.examples.tutorials.mnist import input_data
  mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)
  batch_size = 100
  num_batches = mnist.train.num_examples // batch_size
  batches = [mnist.train.next_batch(batch_size) for _ in range(num_batches)]

Now, suppose we have defined a function which takes a set of model parameters
``theta`` and a batch of data (both images and labels) and computes the loss for
that choice of model parameters on that batch of data. Similarly, suppose we've
also defined a function that takes the same arguments and computes the gradient
of the loss for that choice of model parameters.

.. code-block:: python

  def loss(theta, xs, ys):
      # compute the loss on a batch of data
      return loss

  def grad(theta, xs, ys):
      # compute the gradient on a batch of data
      return grad

  def full_loss(theta):
      # compute the loss on the full data set
      return sum([loss(theta, xs, ys) for (xs, ys) in batches])

  def full_grad(theta):
      # compute the gradient on the full data set
      return sum([grad(theta, xs, ys) for (xs, ys) in batches])

Since we are working with a small dataset, we don't actually need to separate
these methods into the part that operates on a batch and the part that operates
on the full dataset, but doing so will make the distributed version clearer.

Now, if we wish to optimize the loss function using L-BFGS, we simply plug these
functions, along with an initial choice of model parameters, into
``scipy.optimize.fmin_l_bfgs_b``.

.. code-block:: python

  theta_init = 1e-2 * np.random.normal(size=dim)
  result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, fprime=full_grad)

The distributed version
-----------------------

In this example, the computation of the gradient itself can be done in parallel
on a number of workers or machines.

First, let's turn the data into a collection of remote objects.

.. code-block:: python

  batch_ids = [(ray.put(xs), ray.put(ys)) for (xs, ys) in batches]

We can load the data on the driver and distribute it this way because MNIST
easily fits on a single machine. However, for larger data sets, we will need to
use remote functions to distribute the loading of the data.

Now, lets turn ``loss`` and ``grad`` into methods of an actor that will contain our network.

.. code-block:: python

  class Network(object):
      def __init__():
          # Initialize network.

      def loss(theta, xs, ys):
          # compute the loss
          return loss

      def grad(theta, xs, ys):
          # compute the gradient
          return grad

Now, it is easy to speed up the computation of the full loss and the full
gradient.

.. code-block:: python

  def full_loss(theta):
      theta_id = ray.put(theta)
      loss_ids = [actor.loss(theta_id) for actor in actors]
      return sum(ray.get(loss_ids))

  def full_grad(theta):
      theta_id = ray.put(theta)
      grad_ids = [actor.grad(theta_id) for actor in actors]
      return sum(ray.get(grad_ids)).astype("float64") # This conversion is necessary for use with fmin_l_bfgs_b.

Note that we turn ``theta`` into a remote object with the line ``theta_id =
ray.put(theta)`` before passing it into the remote functions. If we had written

.. code-block:: python

  [actor.loss(theta_id) for actor in actors]

instead of

.. code-block:: python

  theta_id = ray.put(theta)
  [actor.loss(theta_id) for actor in actors]

then each task that got sent to the scheduler (one for every element of
``batch_ids``) would have had a copy of ``theta`` serialized inside of it. Since
``theta`` here consists of the parameters of a potentially large model, this is
inefficient. *Large objects should be passed by object ref to remote functions
and not by value*.

We use remote actors and remote objects internally in the implementation of
``full_loss`` and ``full_grad``, but the user-facing behavior of these methods is
identical to the behavior in the serial version.

We can now optimize the objective with the same function call as before.

.. code-block:: python

  theta_init = 1e-2 * np.random.normal(size=dim)
  result = scipy.optimize.fmin_l_bfgs_b(full_loss, theta_init, fprime=full_grad)
