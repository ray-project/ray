Ray Core Walkthrough
====================

This walkthrough will overview the core concepts of Ray:

1. Starting Ray
2. Using remote functions (tasks) [``ray.remote``]
3. Fetching results (object refs) [``ray.put``, ``ray.get``, ``ray.wait``]
4. Using remote classes (actors) [``ray.remote``]

With Ray, your code will work on a single machine and can be easily scaled to large cluster.

Installation
------------

To run this walkthrough, install Ray with ``pip install -U ray``. For the latest wheels (for a snapshot of ``master``), you can use these instructions at :ref:`install-nightlies`.

Starting Ray
------------

You can start Ray on a single machine by adding this to your python script.

.. code-block:: python

  import ray

  # Start Ray. If you're connecting to an existing cluster, you would use
  # ray.init(address=<cluster-address>) instead.
  ray.init()

  ...

Ray will then be able to utilize all cores of your machine. Find out how to configure the number of cores Ray will use at :ref:`configuring-ray`.

To start a multi-node Ray cluster, see the :ref:`cluster setup page <cluster-index>`.

.. _ray-remote-functions:

Remote functions (Tasks)
------------------------

Ray enables arbitrary Python functions to be executed asynchronously. These asynchronous Ray functions are called "remote functions". The standard way to turn a Python function into a remote function is to add the ``@ray.remote`` decorator. Here is an example.

.. code:: python

    # A regular Python function.
    def regular_function():
        return 1

    # A Ray remote function.
    @ray.remote
    def remote_function():
        return 1

This causes a few changes in behavior:

    1. **Invocation:** The regular version is called with ``regular_function()``, whereas the remote version is called with ``remote_function.remote()``.
    2. **Return values:** ``regular_function`` immediately executes and returns ``1``, whereas ``remote_function`` immediately returns an object ref (a future) and then creates a task that will be executed on a worker process. The result can be retrieved with ``ray.get``.

    .. code:: python

        assert regular_function() == 1

        object_ref = remote_function.remote()

        # The value of the original `regular_function`
        assert ray.get(object_ref) == 1

3. **Parallelism:** Invocations of ``regular_function`` happen
   **serially**, for example

   .. code:: python

       # These happen serially.
       for _ in range(4):
           regular_function()

   whereas invocations of ``remote_function`` happen in **parallel**,
   for example

   .. code:: python

       # These happen in parallel.
       for _ in range(4):
           remote_function.remote()

The invocations are executed in parallel because the call to ``remote_function.remote()`` doesn't block.
All computation is performed in the background, driven by Ray's internal event loop.

See the `ray.remote package reference <package-ref.html>`__ page for specific documentation on how to use ``ray.remote``.

.. _ray-object-ids:

**Object refs** can also be passed into remote functions. When the function actually gets executed, **the argument will be a retrieved as a regular Python object**. For example, take this function:

.. code:: python

    @ray.remote
    def remote_chain_function(value):
        return value + 1


    y1_id = remote_function.remote()
    assert ray.get(y1_id) == 1

    chained_id = remote_chain_function.remote(y1_id)
    assert ray.get(chained_id) == 2


Note the following behaviors:

  -  The second task will not be executed until the first task has finished
     executing because the second task depends on the output of the first task.
  -  If the two tasks are scheduled on different machines, the output of the
     first task (the value corresponding to ``y1_id``) will be sent over the
     network to the machine where the second task is scheduled.

Oftentimes, you may want to specify a task's resource requirements (for example
one task may require a GPU). The ``ray.init()`` command will automatically
detect the available GPUs and CPUs on the machine. However, you can override
this default behavior by passing in specific resources, e.g.,
``ray.init(num_cpus=8, num_gpus=4, resources={'Custom': 2})``.

To specify a task's CPU and GPU requirements, pass the ``num_cpus`` and
``num_gpus`` arguments into the remote decorator. The task will only run on a
machine if there are enough CPU and GPU (and other custom) resources available
to execute the task. Ray can also handle arbitrary custom resources.

.. note::

    * If you do not specify any resources in the ``@ray.remote`` decorator, the
      default is 1 CPU resource and no other resources.
    * If specifying CPUs, Ray does not enforce isolation (i.e., your task is
      expected to honor its request).
    * If specifying GPUs, Ray does provide isolation in forms of visible devices
      (setting the environment variable ``CUDA_VISIBLE_DEVICES``), but it is the
      task's responsibility to actually use the GPUs (e.g., through a deep
      learning framework like TensorFlow or PyTorch).

.. code-block:: python

  @ray.remote(num_cpus=4, num_gpus=2)
  def f():
      return 1

The resource requirements of a task have implications for the Ray's scheduling
concurrency. In particular, the sum of the resource requirements of all of the
concurrently executing tasks on a given node cannot exceed the node's total
resources.

Below are more examples of resource specifications:

.. code-block:: python

  # Ray also supports fractional resource requirements
  @ray.remote(num_gpus=0.5)
  def h():
      return 1

  # Ray support custom resources too.
  @ray.remote(resources={'Custom': 1})
  def f():
      return 1

Further, remote functions can return multiple object refs.

.. code-block:: python

  @ray.remote(num_return_vals=3)
  def return_multiple():
      return 1, 2, 3

  a_id, b_id, c_id = return_multiple.remote()

Remote functions can be canceled by calling ``ray.cancel`` (:ref:`docstring <ray-cancel-ref>`) on the returned Object ref. Remote actor functions can be stopped by killing the actor using the ``ray.kill`` interface.

.. code-block:: python

  @ray.remote
  def blocking_operation():
      time.sleep(10e6)
      return 100

  obj_ref = blocking_operation.remote()
  ray.cancel(obj_ref)

Objects in Ray
--------------

In Ray, we can create and compute on objects. We refer to these objects as **remote objects**, and we use **object refs** to refer to them. Remote objects are stored in `shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ **object stores**, and there is one object store per node in the cluster. In the cluster setting, we may not actually know which machine each object lives on.

An **object ref** is essentially a unique ID that can be used to refer to a
remote object. If you're familiar with futures, our object refs are conceptually
similar.

Object refs can be created in multiple ways.

  1. They are returned by remote function calls.
  2. They are returned by ``ray.put`` (:ref:`docstring <ray-put-ref>`).

.. code-block:: python

    y = 1
    object_ref = ray.put(y)

.. note::

    Remote objects are immutable. That is, their values cannot be changed after
    creation. This allows remote objects to be replicated in multiple object
    stores without needing to synchronize the copies.


Fetching Results
----------------

The command ``ray.get(x_id, timeout=None)`` (:ref:`docstring <ray-get-ref>`) takes an object ref and creates a Python object
from the corresponding remote object. First, if the current node's object store
does not contain the object, the object is downloaded. Then, if the object is a `numpy array <https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html>`__
or a collection of numpy arrays, the ``get`` call is zero-copy and returns arrays backed by shared object store memory.
Otherwise, we deserialize the object data into a Python object.

.. code-block:: python

    y = 1
    obj_ref = ray.put(y)
    assert ray.get(obj_ref) == 1

You can also set a timeout to return early from a ``get`` that's blocking for too long.

.. code-block:: python

    from ray.exceptions import RayTimeoutError

    @ray.remote
    def long_running_function()
        time.sleep(8)

    obj_ref = long_running_function.remote()
    try:
        ray.get(obj_ref, timeout=4)
    except RayTimeoutError:
        print("`get` timed out.")


After launching a number of tasks, you may want to know which ones have
finished executing. This can be done with ``ray.wait`` (:ref:`ray-wait-ref`). The function
works as follows.

.. code:: python

    ready_ids, remaining_ids = ray.wait(object_refs, num_returns=1, timeout=None)

Object Eviction
---------------

When the object store gets full, objects will be evicted to make room for new objects.
This happens in approximate LRU (least recently used) order. To avoid objects from
being evicted, you can call ``ray.get`` and store their values instead. Numpy array
objects cannot be evicted while they are mapped in any Python process. You can also
configure `memory limits <memory-management.html>`__ to control object store usage by
actors.

.. note::

    Objects created with ``ray.put`` are pinned in memory while a Python reference
    to the object ref returned by the put exists. This only applies to the specific
    ID returned by put, not IDs in general or copies of that IDs.

Remote Classes (Actors)
-----------------------

Actors extend the Ray API from functions (tasks) to classes. The ``ray.remote``
decorator indicates that instances of the ``Counter`` class will be actors. An
actor is essentially a stateful worker. Each actor runs in its own Python
process.

.. code-block:: python

  @ray.remote
  class Counter(object):
      def __init__(self):
          self.value = 0

      def increment(self):
          self.value += 1
          return self.value

To create a couple actors, we can instantiate this class as follows:

.. code-block:: python

  a1 = Counter.remote()
  a2 = Counter.remote()

When an actor is instantiated, the following events happen.

1. A worker Python process is started on a node of the cluster.
2. A ``Counter`` object is instantiated on that worker.

You can specify resource requirements in Actors too (see the `Actors section
<actors.html>`__ for more details.)

.. code-block:: python

  @ray.remote(num_cpus=2, num_gpus=0.5)
  class Actor(object):
      pass

We can interact with the actor by calling its methods with the ``.remote``
operator. We can then call ``ray.get`` on the object ref to retrieve the actual
value.

.. code-block:: python

  obj_ref = a1.increment.remote()
  ray.get(obj_ref) == 1


Methods called on different actors can execute in parallel, and methods called on the same actor are executed serially in the order that they are called. Methods on the same actor will share state with one another, as shown below.

.. code-block:: python

  # Create ten Counter actors.
  counters = [Counter.remote() for _ in range(10)]

  # Increment each Counter once and get the results. These tasks all happen in
  # parallel.
  results = ray.get([c.increment.remote() for c in counters])
  print(results)  # prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

  # Increment the first Counter five times. These tasks are executed serially
  # and share state.
  results = ray.get([counters[0].increment.remote() for _ in range(5)])
  print(results)  # prints [2, 3, 4, 5, 6]


To learn more about Ray Actors, see the `Actors section <actors.html>`__.
