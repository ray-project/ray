Walkthrough
===========

This walkthrough will overview the core concepts of Ray:

1. Using remote functions (tasks) [``ray.remote``]
2. Fetching results (object IDs) [``ray.put``, ``ray.get``, ``ray.wait``]
3. Using remote classes (actors) [``ray.remote``]

With Ray, your code will work on a single machine and can be easily scaled to a
large cluster. To run this walkthrough, install Ray with ``pip install -U ray``.

.. code-block:: python

  import ray

  # Start Ray. If you're connecting to an existing cluster, you would use
  # ray.init(redis_address=<cluster-redis-address>) instead.
  ray.init()

See the `Configuration <configure.html>`__ documentation for the various ways to
configure Ray. To start a multi-node Ray cluster, see the `cluster setup page
<using-ray-on-a-cluster.html>`__. You can stop ray by calling
``ray.shutdown()``. To check if Ray is initialized, you can call
``ray.is_initialized()``.

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

This causes a few things changes in behavior:

    1. **Invocation:** The regular version is called with ``regular_function()``, whereas the remote version is called with ``remote_function.remote()``.
    2. **Return values:** ``regular_function`` immediately executes and returns ``1``, whereas ``remote_function`` immediately returns an object ID (a future) and then creates a task that will be executed on a worker process. The result can be retrieved with ``ray.get``.

    .. code:: python

        >>> regular_function()
        1

        >>> remote_function.remote()
        ObjectID(1c80d6937802cd7786ad25e50caf2f023c95e350)

        >>> ray.get(remote_function.remote())
        1

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

See the `ray.remote package reference <package-ref.html>`__ page for specific documentation on how to use ``ray.remote``.

**Object IDs** can also be passed into remote functions. When the function actually gets executed, **the argument will be a retrieved as a regular Python object**.

.. code:: python

    >>> y1_id = f.remote(x1_id)
    >>> ray.get(y1_id)
    1

    >>> y2_id = f.remote(x2_id)
    >>> ray.get(y2_id)
    [1, 2, 3]


Note the following behaviors:

  -  The second task will not be executed until the first task has finished
     executing because the second task depends on the output of the first task.
  -  If the two tasks are scheduled on different machines, the output of the
     first task (the value corresponding to ``x1_id``) will be sent over the
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
      expected to honor its request.)
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

Further, remote function can return multiple object IDs.

.. code-block:: python

  @ray.remote(num_return_vals=3)
  def return_multiple():
      return 1, 2, 3

  a_id, b_id, c_id = return_multiple.remote()


Objects in Ray
--------------

In Ray, we can create and compute on objects. We refer to these objects as **remote objects**, and we use **object IDs** to refer to them. Remote objects are stored in **object stores**, and there is one object store per node in the cluster. In the cluster setting, we may not actually know which machine each object lives on.

An **object ID** is essentially a unique ID that can be used to refer to a
remote object. If you're familiar with futures, our object IDs are conceptually
similar.

Object IDs can be created in multiple ways.

  1. They are returned by remote function calls.
  2. They are returned by ``ray.put``.

.. code-block:: python

    >>> y = 1
    >>> y_id = ray.put(y)
    >>> print(y_id)
    ObjectID(0369a14bc595e08cfbd508dfaa162cb7feffffff)

Here is the docstring for ``ray.put``:

.. autofunction:: ray.put
    :noindex:


.. important::

    Remote objects are immutable. That is, their values cannot be changed after
    creation. This allows remote objects to be replicated in multiple object
    stores without needing to synchronize the copies.


Fetching Results
----------------

The command ``ray.get(x_id)`` takes an object ID and creates a Python object
from the corresponding remote object. For some objects like arrays, we can use
shared memory and avoid copying the object.

.. code-block:: python

    >>> y = 1
    >>> obj_id = ray.put(y)
    >>> print(obj_id)
    ObjectID(0369a14bc595e08cfbd508dfaa162cb7feffffff)
    >>> ray.get(obj_id)
    1

Here is the docstring for ``ray.get``:

.. autofunction:: ray.get
    :noindex:


After launching a number of tasks, you may want to know which ones have
finished executing. This can be done with ``ray.wait``. The function
works as follows.

.. code:: python

    ready_ids, remaining_ids = ray.wait(object_ids, num_returns=1, timeout=None)

Here is the docstring for ``ray.wait``:

.. autofunction:: ray.wait
    :noindex:


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
operator. We can then call ``ray.get`` on the object ID to retrieve the actual
value.

.. code-block:: python

  obj_id = a1.increment.remote()
  ray.get(obj_id) == 1


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
