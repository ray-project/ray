The Ray API
===========

Starting Ray
------------

There are two main ways in which Ray can be used. First, you can start all of
the relevant Ray processes and shut them all down within the scope of a single
script. Second, you can connect to and use an existing Ray cluster.

Starting and stopping a cluster within a script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One use case is to start all of the relevant Ray processes when you call
``ray.init`` and shut them down when the script exits. These processes include
local and global schedulers, an object store and an object manager, a redis
server, and more.

**Note:** this approach is limited to a single machine.

This can be done as follows.

.. code-block:: python

  ray.init()

If there are GPUs available on the machine, you should specify this with the
``num_gpus`` argument. Similarly, you can also specify the number of CPUs with
``num_cpus``.

.. code-block:: python

  ray.init(num_cpus=20, num_gpus=2)

By default, Ray will use ``psutil.cpu_count()`` to determine the number of CPUs.
Ray will also attempt to automatically determine the number of GPUs.

Instead of thinking about the number of "worker" processes on each node, we
prefer to think in terms of the quantities of CPU and GPU resources on each
node and to provide the illusion of an infinite pool of workers. Tasks will be
assigned to workers based on the availability of resources so as to avoid
contention and not based on the number of available worker processes.

Connecting to an existing cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once a Ray cluster has been started, the only thing you need in order to connect
to it is the address of the Redis server in the cluster. In this case, your
script will not start up or shut down any processes. The cluster and all of its
processes may be shared between multiple scripts and multiple users. To do this,
you simply need to know the address of the cluster's Redis server. This can be
done with a command like the following.

.. code-block:: python

  ray.init(redis_address="12.345.67.89:6379")

In this case, you cannot specify ``num_cpus`` or ``num_gpus`` in ``ray.init``
because that information is passed into the cluster when the cluster is started,
not when your script is started.

View the instructions for how to `start a Ray cluster`_ on multiple nodes.

.. _`start a Ray cluster`: http://ray.readthedocs.io/en/latest/using-ray-on-a-cluster.html

.. autofunction:: ray.init

Defining remote functions
-------------------------

Remote functions are used to create tasks. To define a remote function, the
``@ray.remote`` decorator is placed over the function definition.

The function can then be invoked with ``f.remote``. Invoking the function
creates a **task** which will be scheduled on and executed by some worker
process in the Ray cluster. The call will return an **object ID** (essentially a
future) representing the eventual return value of the task. Anyone with the
object ID can retrieve its value, regardless of where the task was executed (see
`Getting values from object IDs`_).

When a task executes, its outputs will be serialized into a string of bytes and
stored in the object store.

Note that arguments to remote functions can be values or object IDs.

.. code-block:: python

  @ray.remote
  def f(x):
      return x + 1

  x_id = f.remote(0)
  ray.get(x_id)  # 1

  y_id = f.remote(x_id)
  ray.get(y_id)  # 2

If you want a remote function to return multiple object IDs, you can do that by
passing the ``num_return_vals`` argument into the remote decorator.

.. code-block:: python

  @ray.remote(num_return_vals=2)
  def f():
      return 1, 2

  x_id, y_id = f.remote()
  ray.get(x_id)  # 1
  ray.get(y_id)  # 2

.. autofunction:: ray.remote

Getting values from object IDs
------------------------------

Object IDs can be converted into objects by calling ``ray.get`` on the object
ID. Note that ``ray.get`` accepts either a single object ID or a list of object
IDs.

.. code-block:: python

  @ray.remote
  def f():
      return {'key1': ['value']}

  # Get one object ID.
  ray.get(f.remote())  # {'key1': ['value']}

  # Get a list of object IDs.
  ray.get([f.remote() for _ in range(2)])  # [{'key1': ['value']}, {'key1': ['value']}]

Numpy arrays
~~~~~~~~~~~~

Numpy arrays are handled more efficiently than other data types, so **use numpy
arrays whenever possible**.

Any numpy arrays that are part of the serialized object will not be copied out
of the object store. They will remain in the object store and the resulting
deserialized object will simply have a pointer to the relevant place in the
object store's memory.

Since objects in the object store are immutable, this means that if you want to
mutate a numpy array that was returned by a remote function, you will have to
first copy it.

.. autofunction:: ray.get

Putting objects in the object store
-----------------------------------

The primary way that objects are placed in the object store is by being returned
by a task. However, it is also possible to directly place objects in the object
store using ``ray.put``.

.. code-block:: python

  x_id = ray.put(1)
  ray.get(x_id)  # 1

The main reason to use ``ray.put`` is that you want to pass the same large
object into a number of tasks. By first doing ``ray.put`` and then passing the
resulting object ID into each of the tasks, the large object is copied into the
object store only once, whereas when we directly pass the object in, it is
copied multiple times.

.. code-block:: python

  import numpy as np

  @ray.remote
  def f(x):
      pass

  x = np.zeros(10 ** 6)

  # Alternative 1: Here, x is copied into the object store 10 times.
  [f.remote(x) for _ in range(10)]

  # Alternative 2: Here, x is copied into the object store once.
  x_id = ray.put(x)
  [f.remote(x_id) for _ in range(10)]

Note that ``ray.put`` is called under the hood in a couple situations.

- It is called on the values returned by a task.
- It is called on the arguments to a task, unless the arguments are Python
  primitives like integers or short strings, lists, tuples, or dictionaries.

.. autofunction:: ray.put

Waiting for a subset of tasks to finish
---------------------------------------

It is often desirable to adapt the computation being done based on when
different tasks finish. For example, if a bunch of tasks each take a variable
length of time, and their results can be processed in any order, then it makes
sense to simply process the results in the order that they finish. In other
settings, it makes sense to discard straggler tasks whose results may not be
needed.

To do this, we introduce the ``ray.wait`` primitive, which takes a list of
object IDs and returns when a subset of them are available. By default it blocks
until a single object is available, but the ``num_returns`` value can be
specified to wait for a different number. If a ``timeout`` argument is passed
in, it will block for at most that many milliseconds and may return a list with
fewer than ``num_returns`` elements.

The ``ray.wait`` function returns two lists. The first list is a list of object
IDs of available objects (of length at most ``num_returns``), and the second
list is a list of the remaining object IDs, so the combination of these two
lists is equal to the list passed in to ``ray.wait`` (up to ordering).

.. code-block:: python

  import time
  import numpy as np

  @ray.remote
  def f(n):
      time.sleep(n)
      return n

  # Start 3 tasks with different durations.
  results = [f.remote(i) for i in range(3)]
  # Block until 2 of them have finished.
  ready_ids, remaining_ids = ray.wait(results, num_returns=2)

  # Start 5 tasks with different durations.
  results = [f.remote(i) for i in range(3)]
  # Block until 4 of them have finished or 2.5 seconds pass.
  ready_ids, remaining_ids = ray.wait(results, num_returns=4, timeout=2500)

It is easy to use this construct to create an infinite loop in which multiple
tasks are executing, and whenever one task finishes, a new one is launched.

.. code-block:: python

  @ray.remote
  def f():
      return 1

  # Start 5 tasks.
  remaining_ids = [f.remote() for i in range(5)]
  # Whenever one task finishes, start a new one.
  for _ in range(100):
      ready_ids, remaining_ids = ray.wait(remaining_ids)
      # Get the available object and do something with it.
      print(ray.get(ready_ids))
      # Start a new task.
      remaining_ids.append(f.remote())

.. autofunction:: ray.wait

Viewing errors
--------------

Keeping track of errors that occur in different processes throughout a cluster
can be challenging. There are a couple mechanisms to help with this.

1. If a task throws an exception, that exception will be printed in the
   background of the driver process.

2. If ``ray.get`` is called on an object ID whose parent task threw an exception
   before creating the object, the exception will be re-raised by ``ray.get``.

The errors will also be accumulated in Redis and can be accessed with
``ray.error_info``. Normally, you shouldn't need to do this, but it is possible.

.. code-block:: python

  @ray.remote
  def f():
      raise Exception("This task failed!!")

  f.remote()  # An error message will be printed in the background.

  # Wait for the error to propagate to Redis.
  import time
  time.sleep(1)

  ray.error_info()  # This returns a list containing the error message.

.. autofunction:: ray.error_info
