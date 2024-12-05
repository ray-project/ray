.. _objects-in-ray:

Objects
=======

Ray tasks and actors create and compute on objects. These objects are **remote objects** 
because Ray can store them anywhere in a Ray cluster. You can use **object refs** to refer
to them. Ray caches remote objects in its distributed
`shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ **object store**, and 
there is one object store per node in the cluster. In the cluster setting,
a remote object can live on one or many nodes, independent of which node holds the 
object refs.

An **object ref** is a pointer or a unique ID that you can use to refer to a
remote object without seeing its value. If you're familiar with futures, Ray object
refs are conceptually similar.

Ray creates object refs. These two calls return object refs:

* remote function calls
* :func:`ray.put() <ray.put>`

For example:

.. testcode::

  import ray

  # Put an object in Ray's object store.
  y = 1
  object_ref = ray.put(y)

.. note::

    Remote objects are immutable. You can't change their values after
    creation. This restriction allows Ray to replicate remote objects
    in multiple object stores without needing to synchronize the copies.


Fetching object data
--------------------

You can use the :func:`ray.get() <ray.get>` method to fetch the result of a remote
object from an object ref.
If the current node's object store doesn't contain the object, Ray downloads the
object.

If the object is a
`numpy array <https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html>`__
or a collection of numpy arrays, the ``get`` call is zero-copy and 
returns arrays backed by shared object store memory.
Otherwise, Ray deserializes the object data into a Python object.

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

Passing object arguments
------------------------

You can freely pass Ray object references in a Ray application. You can pass them as
arguments to tasks, actor methods, and even stored in other objects. Ray tracks objects
with *distributed reference counting*, and automatically frees an object's data once it
deletes all references to the object.

You can pass an object to a Ray task or method in two different ways. Depending on
the way you pass an object, Ray decides whether to *de-reference* the object prior
to task execution.

**Passing an object as a top-level argument**: When you pass an object
directly as a top-level argument to a task, Ray de-references the object. Ray fetches
the underlying data for all top-level object reference arguments, without executing
the task until the object data becomes fully available.

.. literalinclude:: doc_code/obj_val.py

**Passing an object as a nested argument**: When you pass an object
within a nested object, for example, within a Python list, Ray *doesn't* de-reference it.
The task needs to call ``ray.get()`` on the reference to fetch the concrete value.
However, if the task never calls ``ray.get()``, then Ray never needs to transfer the
object value to the machine the task is running on. Pass objects as top-level arguments
where possible, but nested arguments can be useful for passing objects on to other 
tasks without needing to see the data.

.. literalinclude:: doc_code/obj_ref.py

The top-level versus not top-level passing convention also applies to actor
constructors and actor method calls:

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

Closure-capture of objects
--------------------------

You can also pass objects to tasks with *closure-capture*. This approach is convenient
when you want to share a large object verbatim between many tasks or actors, and
don't want to pass it repeatedly as an argument. However, be aware that defining
a task that closes over an object ref pins the object with reference-counting, so
Ray doesn't evict the object until the job completes.

.. literalinclude:: doc_code/obj_capture.py

Nested objects
--------------

Ray also supports nested object references. This support allows you to build
composite objects that themselves hold references to further sub-objects.

.. testcode::

    # You can nest objects. Ray keeps the inner object
    # alive with reference counting until it deletes all outer object 
    # references.
    object_ref_2 = ray.put([object_ref])

Fault tolerance
---------------

Ray can automatically recover from object data loss
with :ref:`lineage reconstruction <fault-tolerance-objects-reconstruction>`
but not :ref:`owner <fault-tolerance-ownership>` failure.
See :ref:`Ray fault tolerance <fault-tolerance>` for more details.

More about Ray objects
----------------------

.. toctree::
    :maxdepth: 1

    objects/serialization.rst
    objects/object-spilling.rst
