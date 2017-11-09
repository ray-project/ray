Parameter Server
================

This document walks through how to implement a simple parameter server example
using actors. To run the application, first install some dependencies.

.. code-block:: bash

  pip install tensorflow

You can view the `code for this example`_.

.. _`code for this example`: https://github.com/ray-project/ray/tree/master/examples/parameter_server

The example can be run as follows.

.. code-block:: bash

  python ray/examples/parameter_server/parameter_server.py --num-workers=4

Note that this examples uses distributed actor handles, which are still
considered experimental.

The parameter server itself is implemented as an actor, which exposes the
methods ``push`` and ``pull``.

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
