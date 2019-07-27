How-to: Using Actors
====================

An actor is essentially a stateful worker (or a service). When a new actor is instantiated, a new worker is created, and methods of the actor are scheduled on that specific worker and
can access and mutate the state of that worker.

Creating an actor
-----------------

You can convert a standard python class into an Actor class as follows:

.. code-block:: python

  @ray.remote
  class Counter(object):
      def __init__(self):
          self.value = 0

      def increment(self):
          self.value += 1
          return self.value

Note that the above is equivalent to the following:

.. code-block:: python

  class Counter(object):
      def __init__(self):
          self.value = 0

      def increment(self):
          self.value += 1
          return self.value

  RemoteCounter = ray.remote(Counter)

When an actor is instantiated, the following events happen.

1. A node in the cluster is chosen and a worker process is created on that node
   for the purpose of running methods called on the actor.
2. A ``Counter`` object is created on that worker and the ``Counter``
   constructor is run.

Any method of the actor can return multiple object IDs with the ``ray.method`` decorator:

.. code-block:: python

    @ray.remote
    class Foo(object):

        @ray.method(num_return_vals=2)
        def bar(self):
            return 1, 2

    f = Foo.remote()

    obj_id1, obj_id2 = f.bar.remote()
    assert ray.get(obj_id1) == 1
    assert ray.get(obj_id2) == 2

Resources with Actors
---------------------

You can specify that an actor requires CPUs or GPUs in the decorator. While Ray has built-in support for CPUs and GPUs, Ray can also handle custom resources.

When using GPUs, Ray will automatically set the environment variable ``CUDA_VISIBLE_DEVICES`` for the actor after instantiated. The actor will have access to a list of the IDs of the GPUs
that it is allowed to use via ``ray.get_gpu_ids()``. This is a list of integers,
like ``[]``, or ``[1]``, or ``[2, 5, 6]``.

.. code-block:: python

  @ray.remote(num_cpus=2, num_gpus=1)
  class GPUActor(object):
      pass

Note that this is equivalent to the following:

.. code-block:: python

  class GPUActor(object):
      pass

  GPUActor = ray.remote(num_cpus=2, num_gpus=1)(GPUActor)

When an ``GPUActor`` instance is created, it will be placed on a node that has at least 1 GPU, and the GPU will be reserved for the actor for the duration of the actor's lifetime (even if the actor is not executing tasks). The GPU resources will be released when the actor terminates.

If you want to use custom resources, make sure your cluster is configured to have these resources (see `configuration instructions <configure.html#cluster-resources>`__):

.. code-block:: python

  @ray.remote(resources={'Resource2': 1})
  class GPUActor(object):
      pass


Terminating Actors
------------------

For any actor, you can call ``__ray_terminate__.remote()`` to terminate the actor.
This will kill the actor process and release resources associated/assigned to the actor:

.. code-block:: python

    @ray.remote
    class Foo(object):
        pass

    f = Foo.remote()
    f.__ray_terminate__.remote()

This is important since actors are not garbage collected.


Passing Around Actor Handles (Experimental)
-------------------------------------------

Actor handles can be passed into other tasks. To see an example of this, take a
look at the `asynchronous parameter server example`_. To illustrate this with
a simple example, consider a simple actor definition. This functionality is
currently **experimental** and subject to the limitations described below.

.. code-block:: python

  @ray.remote
  class Counter(object):
      def __init__(self):
          self.counter = 0

      def inc(self):
          self.counter += 1

      def get_counter(self):
          return self.counter

We can define remote functions (or actor methods) that use actor handles.

.. code-block:: python

  @ray.remote
  def f(counter):
      while True:
          counter.inc.remote()

If we instantiate an actor, we can pass the handle around to various tasks.

.. code-block:: python

  counter = Counter.remote()

  # Start some tasks that use the actor.
  [f.remote(counter) for _ in range(4)]

  # Print the counter value.
  for _ in range(10):
      print(ray.get(counter.get_counter.remote()))

.. _`asynchronous parameter server example`: http://ray.readthedocs.io/en/latest/example-parameter-server.html
