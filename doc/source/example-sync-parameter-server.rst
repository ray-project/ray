Parameter Server
================

This document walks through how to implement a simple synchronous parameter server example
using actors. To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/sync_parameter_server

The example can be run as follows.

.. code-block:: bash

  python ray/examples/parameter_server/sync_parameter_server.py --num-workers=4


The parameter server is implemented as an actor, which exposes the
methods ``apply_gradients`` and ``get_weights``. A constant linear scaling
rule is applied by scaling the learning rate by the number of workers.

.. code-block:: python

    @ray.remote
    class ParameterServer(object):

        def __init__(self, num_workers):
            self.num_workers = num_workers
            self.net = model.SimpleCNN(learning_rate=1e-4 * num_workers)

        def apply_gradients(self, *gradients):
            self.net.apply_gradients(np.sum(np.array(gradients), axis=0)/self.num_workers)
            return self.net.variables.get_flat()

        def get_weights(self):
            return self.net.variables.get_flat()


Workers are actors which expose the method ``compute_gradients``.
Each worker obtains the same mini-batch per iteration,
and computes the gradient for the per-worker batch corresponding to ``worker_index``.

.. code-block:: python

    @ray.remote
    class Worker(object):

        def __init__(self, worker_index, num_workers, batch_size=50, seed=1337):
            self.worker_index = worker_index
            self.num_workers = num_workers
            self.batch_size = batch_size
            self.mnist = input_data.read_data_sets("MNIST_data", one_hot=True, seed=seed)
            self.net = model.SimpleCNN()

        def compute_gradients(self, weights):
            self.net.variables.set_flat(weights)
            s = self.worker_index * self.batch_size
            e = s + self.batch_size
            xs, ys = self.mnist.train.next_batch(self.num_workers * self.batch_size)
            return self.net.compute_gradients(xs[s:e], ys[s:e])


Training is carried out by first computing the gradients against the
current weights computed by the parameter server, then applying the set of weights
to the .

.. code-block:: python

    gradients = [worker.compute_gradients.remote(current_weights) for worker in workers]
    current_weights = ps.apply_gradients.remote(*gradients)
