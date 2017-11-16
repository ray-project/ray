Parameter Server
================

This document walks through how to implement simple synchronous and asynchronous
parameter servers using actors. To run the application, first install some
dependencies.

.. code-block:: bash

  pip install tensorflow

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/parameter_server

The examples can be run as follows.

.. code-block:: bash

  # Run the asynchronous parameter server.
  python ray/examples/parameter_server/async_parameter_server.py --num-workers=4

  # Run the synchronous parameter server.
  python ray/examples/parameter_server/sync_parameter_server.py --num-workers=4

Note that this examples uses distributed actor handles, which are still
considered experimental.

Asynchronous Parameter Server
-----------------------------

The asynchronous parameter server itself is implemented as an actor, which
exposes the methods ``push`` and ``pull``.

.. code-block:: python

  @ray.remote
  class ParameterServer(object):
      def __init__(self, keys, values):
          values = [value.copy() for value in values]
          self.weights = dict(zip(keys, values))

      def push(self, keys, values):
          for key, value in zip(keys, values):
              self.weights[key] += value

      def pull(self, keys):
          return [self.weights[key] for key in keys]

We then define a worker task, which take a parameter server as an argument and
submits tasks to it. The structure of the code looks as follows.

.. code-block:: python

  @ray.remote
  def worker_task(ps):
      while True:
          # Get the latest weights from the parameter server.
          weights = ray.get(ps.pull.remote(keys))

          # Compute an update.
          ...

          # Push the update to the parameter server.
          ps.push.remote(keys, update)

Then we can create a parameter server and initiate training as follows.

.. code-block:: python

  ps = ParameterServer.remote(keys, initial_values)
  worker_tasks = [worker_task.remote(ps) for _ in range(4)]

Synchronous Parameter Server
----------------------------

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
however they can be easily extended to **shard the parameters across multiple
actors**.
