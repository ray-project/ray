How-to: Using Actors
====================

An actor is essentially a stateful worker (or a service). When a new actor is
instantiated, a new worker is created, and methods of the actor are scheduled on
that specific worker and can access and mutate the state of that worker.

Creating an actor
-----------------

You can convert a standard Python class into a Ray actor class as follows:

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

  Counter = ray.remote(Counter)

When the above actor is instantiated, the following events happen.

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

When an ``GPUActor`` instance is created, it will be placed on a node that has
at least 1 GPU, and the GPU will be reserved for the actor for the duration of
the actor's lifetime (even if the actor is not executing tasks). The GPU
resources will be released when the actor terminates.

If you want to use custom resources, make sure your cluster is configured to
have these resources (see `configuration instructions
<configure.html#cluster-resources>`__):

.. important::

  * If you specify resource requirements in an actor class's remote decorator,
    then the actor will acquire those resources for its entire lifetime (if you
    do not specify CPU resources, the default is 1), even if it is not executing
    any methods. The actor will not acquire any additional resources when
    executing methods.
  * If you do not specify any resource requirements in the actor class's remote
    decorator, then by default, the actor will not acquire any resources for its
    lifetime, but every time it executes a method, it will need to acquire 1 CPU
    resource.

If you need to instantiate many copies of the same actor with varying resource
requirements, you can do so as follows.

.. code-block:: python

  a1 = Counter._remote(num_cpus=1, resources={"Custom1": 1})
  a2 = Counter._remote(num_cpus=2, resources={"Custom2": 1})
  a3 = Counter._remote(num_cpus=3, resources={"Custom3": 1})

Note that to create these actors successfully, Ray will need to be started with
sufficient CPU resources and the relevant custom resources.

.. code-block:: python

  @ray.remote(resources={'Resource2': 1})
  class GPUActor(object):
      pass


Terminating Actors
------------------

Actor processes will be terminated automatically when the initial actor handle
goes out of scope in Python. If we create an actor with ``actor_handle =
Counter.remote()``, then when ``actor_handle`` goes out of scope and is
destructed, the actor process will be terminated. Note that this only applies to
the original actor handle created for the actor and not to subsequent actor
handles created by passing the actor handle to other tasks.

If necessary, you can manually terminate an actor by calling
``ray.actor.exit_actor()`` from within one of the actor methods. This will kill
the actor process and release resources associated/assigned to the actor. This
approach should generally not be necessary as actors are automatically garbage
collected.

Passing Around Actor Handles
----------------------------

Actor handles can be passed into other tasks. To see an example of this, take a
look at the `asynchronous parameter server example`_. To illustrate this with a
simple example, consider a simple actor definition.

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

  import time

  @ray.remote
  def f(counter):
      for _ in range(1000):
          time.sleep(0.1)
          counter.inc.remote()

If we instantiate an actor, we can pass the handle around to various tasks.

.. code-block:: python

  counter = Counter.remote()

  # Start some tasks that use the actor.
  [f.remote(counter) for _ in range(3)]

  # Print the counter value.
  for _ in range(10):
      time.sleep(1)
      print(ray.get(counter.get_counter.remote()))

.. _`asynchronous parameter server example`: http://ray.readthedocs.io/en/latest/example-parameter-server.html
