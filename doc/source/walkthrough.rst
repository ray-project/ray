Walkthrough
===========

This walkthrough will overview the core concepts of Ray:

   1. Launching a remote function
   2. Fetching results.
   3. Using remote classes (actors).

Suppose we've already started Ray.

.. code-block:: python

  import ray
  ray.init()

Launching a function
--------------------

The standard way to turn a Python function into a remote function is to
add the ``@ray.remote`` decorator. Here is an example.

.. code:: python

    # A regular Python function.
    def regular_function():
        return 1

    # A Ray remote function.
    @ray.remote
    def remote_function():
        return 1

The differences are the following:

    1. **Invocation:** The regular version is called with ``regular_function()``, whereas the remote version is called with ``remote_function.remote()``.
    2. **Return values:** ``regular_function`` immediately executes and returns ``1``, whereas ``remote_function`` immediately returns an object ID (a future) and then creates a task that will be executed on a worker process. The result can be obtained with ``ray.get``.

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

Objects in Ray
--------------

In Ray, we can create and compute on objects. We refer to these objects as **remote objects**, and we use **object IDs** to refer to them. Remote objects are stored in **object stores**, and there is one object store per node in the cluster. In the cluster setting, we may not actually know which machine each object lives on.

An **object ID** is essentially a unique ID that can be used to refer to a
remote object. If you're familiar with Futures, our object IDs are conceptually
similar.

Object IDs can be created in multiple ways.

  1. They are returned by remote function calls.
  2. They are returned by ``ray.put``.

.. code-block:: python

    >>> y = 6
    >>> obj_id = ray.put(y)
    >>> print(obj_id)
    ObjectID(0369a14bc595e08cfbd508dfaa162cb7feffffff)
    >>> ray.get(obj_id)
    6

We assume that remote objects are immutable. That is, their values cannot be
changed after creation. This allows remote objects to be replicated in multiple
object stores without needing to synchronize the copies.


Fetching Results
----------------

The command ``ray.get(x_id)`` takes an object ID and creates a Python object from
the corresponding remote object. For some objects like arrays, we can use shared
memory and avoid copying the object.

After launching a number of tasks, you may want to know which ones have
finished executing. This can be done with ``ray.wait``. The function
works as follows.

.. code:: python

    ready_ids, remaining_ids = ray.wait(object_ids, num_returns=1, timeout=None)


Remote Classes (Actors)
-----------------------

Actors extend the Ray API from functions (tasks) to classes. The ``ray.remote`` decorator indicates that instances of the ``Counter`` class will be actors.  An actor is essentially a stateful worker.

.. code-block:: python

  @ray.remote
  class Counter(object):
      def __init__(self):
          self.value = 0

      def increment(self):
          self.value += 1
          return self.value

To actually create an actor, we can instantiate this class by calling ``Counter.remote()``.

.. code-block:: python

  a1 = Counter.remote()
  a2 = Counter.remote()

When an actor is instantiated, the following events happen.

1. A node in the cluster is chosen and a worker process is created on that node
   (by the raylet on that node) for the purpose of running methods
   called on the actor.
2. A ``Counter`` object is created on that worker and the ``Counter``
   constructor is run.

We can interact with the actor by calling its methods with the ``.remote`` operator.

.. code-block:: python

  a1.increment.remote()  # ray.get returns 1
  a2.increment.remote()  # ray.get returns 1

We can then call ``ray.get`` on the object ID to retrieve the actual value.

Since these two tasks run on different actors, they can be executed in parallel.  On the other hand, methods called on the same ``Counter`` actor are executed serially in the order that they are called. They can thus share state with
one another, as shown below.

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


To learn more about Ray's API and advanced usage, take a look at the Advanced Usage guide.
