.. meta::
   :description: Ray remote objects and ObjectRefs: fetching data, passing objects as arguments, closure capture, nested objects, and fault tolerance.

.. _objects-in-ray:

Objects
=======

In Ray, tasks and actors create and compute on objects. We refer to these objects as **remote objects** because they can be stored anywhere in a Ray cluster, and we use **object refs** to refer to them. Remote objects are cached in Ray's distributed `shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ **object store**, and there is one object store per node in the cluster. In the cluster setting, a remote object can live on one or many nodes, independent of who holds the object ref(s).

An **object ref** is essentially a pointer or a unique ID that can be used to refer to a
remote object without seeing its value. If you're familiar with futures, Ray object refs are conceptually
similar.

Object refs can be created in two ways.

  1. They are returned by remote function calls.
  2. They are returned by :func:`ray.put() <ray.put>`.

.. tab-set::

    .. tab-item:: Python

      .. testcode::

        import ray

        # Put an object in Ray's object store.
        y = 1
        object_ref = ray.put(y)

.. note::

    Remote objects are immutable. That is, their values cannot be changed after
    creation. This allows remote objects to be replicated in multiple object
    stores without needing to synchronize the copies.


Fetching Object Data
--------------------

You can use the :func:`ray.get() <ray.get>` method to fetch the result of a remote object from an object ref.
If the current node's object store does not contain the object, the object is downloaded.

.. tab-set::

    .. tab-item:: Python

        If the object is a `numpy array <https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html>`__
        or a collection of numpy arrays, the ``get`` call is zero-copy and returns arrays backed by shared object store memory.
        Otherwise, we deserialize the object data into a Python object.

        .. testcode::

          import ray
          import time

          # Get the value of one object ref.
          obj_ref = ray.put(1)
          assert ray.get(obj_ref) == 1

          # Get the values of multiple object refs in parallel.
          assert ray.get([ray.put(i) for i in range(3)]) == [0, 1, 2]

          # You can also set a timeout to return early from a ``get``
          # that's blocking for too long.
          from ray.exceptions import GetTimeoutError
          # ``GetTimeoutError`` is a subclass of ``TimeoutError``.

          @ray.remote
          def long_running_function():
              time.sleep(8)

          obj_ref = long_running_function.remote()
          try:
              ray.get(obj_ref, timeout=4)
          except GetTimeoutError:  # You can capture the standard "TimeoutError" instead
              print("`get` timed out.")

        .. testoutput::

          `get` timed out.

Passing Object Arguments
------------------------

Ray object references can be freely passed around a Ray application. This means that they can be passed as arguments to tasks, actor methods, and even stored in other objects. Objects are tracked via *distributed reference counting*, and their data is automatically freed once all references to the object are deleted.

There are two different ways one can pass an object to a Ray task or method. Depending on the way an object is passed, Ray will decide whether to *de-reference* the object prior to task execution.

**Passing an object as a top-level argument**: When an object is passed directly as a top-level argument to a task, Ray will de-reference the object. This means that Ray will fetch the underlying data for all top-level object reference arguments, not executing the task until the object data becomes fully available.

.. literalinclude:: doc_code/obj_val.py

**Passing an object as a nested argument**: When an object is passed within a nested object, for example, within a Python list, Ray will *not* de-reference it. This means that the task will need to call ``ray.get()`` on the reference to fetch the concrete value. However, if the task never calls ``ray.get()``, then the object value never needs to be transferred to the machine the task is running on. We recommend passing objects as top-level arguments where possible, but nested arguments can be useful for passing objects on to other tasks without needing to see the data.

.. literalinclude:: doc_code/obj_ref.py

The top-level vs not top-level passing convention also applies to actor constructors and actor method calls:

.. testcode::

    @ray.remote
    class Actor:
      def __init__(self, arg):
        pass

      def method(self, arg):
        pass

    obj = ray.put(2)

    # Examples of passing objects to actor constructors.
    actor_handle = Actor.remote(obj)  # by-value
    actor_handle = Actor.remote([obj])  # by-reference

    # Examples of passing objects to actor method calls.
    actor_handle.method.remote(obj)  # by-value
    actor_handle.method.remote([obj])  # by-reference

Closure Capture of Objects
--------------------------

You can also pass objects to tasks via *closure-capture*. This can be convenient when you have a large object that you want to share verbatim between many tasks or actors, and don't want to pass it repeatedly as an argument. Be aware however that defining a task that closes over an object ref will pin the object via reference-counting, so the object will not be evicted until the job completes.

.. literalinclude:: doc_code/obj_capture.py

Nested Objects
--------------

Ray also supports nested object references. This allows you to build composite objects that themselves hold references to further sub-objects.

.. testcode::

    # Objects can be nested within each other. Ray will keep the inner object
    # alive via reference counting until all outer object references are deleted.
    object_ref_2 = ray.put([object_ref])

Fault Tolerance
---------------

Ray can automatically recover from object data loss
via :ref:`lineage reconstruction <fault-tolerance-objects-reconstruction>`
but not :ref:`owner <fault-tolerance-ownership>` failure.
See :ref:`Ray fault tolerance <fault-tolerance>` for more details.

More about Ray Objects
----------------------

.. toctree::
    :maxdepth: 1

    objects/serialization.rst
    objects/object-spilling.rst
