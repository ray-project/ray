Parameter Server
================

This document walks through how to implement simple synchronous and asynchronous
parameter servers using actors.

To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/doc/examples/parameter_server

Parameter Server
----------------

The parameter server is implemented as an actor, which exposes the
methods ``apply_gradients`` and ``get_weights``. A constant linear scaling
rule is applied by scaling the learning rate by the number of workers.

.. code-block:: python

  @ray.remote
  class ParameterServer(object):
      def __init__(self, learning_rate):
          self.net = model.SimpleCNN(learning_rate=learning_rate)

      def apply_gradients(self, *gradients):
          self.net.apply_gradients(np.mean(gradients, axis=0))
          return self.net.variables.get_flat()

      def get_weights(self):
          return self.net.variables.get_flat()


Workers are actors which expose the method ``compute_gradients``.

.. code-block:: python

  @ray.remote
  class Worker(object):
      def __init__(self, worker_index, batch_size=50):
          self.worker_index = worker_index
          self.batch_size = batch_size
          self.mnist = input_data.read_data_sets("MNIST_data", one_hot=True,
                                                 seed=worker_index)
          self.net = model.SimpleCNN()

      def compute_gradients(self, weights):
          self.net.variables.set_flat(weights)
          xs, ys = self.mnist.train.next_batch(self.batch_size)
          return self.net.compute_gradients(xs, ys)

Training alternates between computing the gradients given the current weights
from the parameter server and updating the parameter server's weights with the
resulting gradients.

.. code-block:: python

  while True:
      gradients = [worker.compute_gradients.remote(current_weights)
                   for worker in workers]
      current_weights = ps.apply_gradients.remote(*gradients)

Both of these examples implement the parameter server using a single actor,
however they can be easily extended to **split the parameters across multiple
actors**.
