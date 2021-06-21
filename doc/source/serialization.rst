.. _serialization-guide:

Serialization
=============

Since Ray processes do not share memory space, data transferred between workers and nodes will need to **serialized** and **deserialized**. Ray uses the `Plasma object store <https://arrow.apache.org/docs/python/plasma.html>`_ to efficiently transfer objects across different processes and different nodes. Numpy arrays in the object store are shared between workers on the same node (zero-copy deserialization).

Overview
--------

Ray has decided to use a customized `Pickle protocol version 5 <https://www.python.org/dev/peps/pep-0574/>`_ backport to replace the original PyArrow serializer. This gets rid of several previous limitations (e.g. cannot serialize recursive objects).

Ray is currently compatible with Pickle protocol version 5, while Ray supports serialization of a wider range of objects (e.g. lambda & nested functions, dynamic classes) with the help of cloudpickle.

.. _plasma-store:

Plasma Object Store
~~~~~~~~~~~~~~~~~~~

Plasma is an in-memory object store that is being developed as part of Apache Arrow. Ray uses Plasma to efficiently transfer objects across different processes and different nodes. All objects in Plasma object store are **immutable** and held in shared memory. This is so that they can be accessed efficiently by many workers on the same node.

Each node has its own object store. When data is put into the object store, it does not get automatically broadcasted to other nodes. Data remains local to the writer until requested by another task or actor on another node.

Numpy Arrays
~~~~~~~~~~~~

Ray optimizes for numpy arrays by using Pickle protocol 5 with out-of-band data.
The numpy array is stored as a read-only object, and all Ray workers on the same node can read the numpy array in the object store without copying (zero-copy reads). Each numpy array object in the worker process holds a pointer to the relevant array held in shared memory. Any writes to the read-only object will require the user to first copy it into the local process memory.

.. tip:: You can often avoid serialization issues by using only native types (e.g., numpy arrays or lists/dicts of numpy arrays and other primitive types), or by using Actors hold objects that cannot be serialized.

Serialization notes
-------------------

- Ray is currently using Pickle protocol version 5. The default pickle protocol used by most python distributions is protocol 3. Protocol 4 & 5 are more efficient than protocol 3 for larger objects.

- For non-native objects, Ray will always keep a single copy even it is referred multiple times in an object:

  .. code-block:: python

    import numpy as np
    obj = [np.zeros(42)] * 99
    l = ray.get(ray.put(obj))
    assert l[0] is l[1]  # no problem!

- Whenever possible, use numpy arrays or Python collections of numpy arrays for maximum performance.

- Lock objects are mostly unserializable, because copying a lock is meaningless and could cause serious concurrency problems. You may have to come up with a workaround if your object contains a lock.

Customized Serialization
------------------------

Sometimes you may want to customize your serialization process because
the default serializer used by Ray (pickle5 + cloudpickle) does
not work for you (fail to serialize some objects, too slow for certain objects, etc.).

There are at least 3 ways to define your custom serialization process:

1. If you want to customize the serialization of a type of objects,
   and you have access to the code, you can define ``__reduce__``
   function inside the corresponding class. This is commonly done
   by most Python libraries. Example code:

   .. code-block:: python

     import ray
     import sqlite3

     ray.init()

     class DBConnection:
         def __init__(self, path):
             self.path = path
             self.conn = sqlite3.connect(path)

         # without '__reduce__', the instance is unserializable.
         def __reduce__(self):
             deserializer = DBConnection
             serialized_data = (self.path,)
             return deserializer, serialized_data

     original = DBConnection("/tmp/db")
     print(original.conn)

     copied = ray.get(ray.put(original))
     print(copied.conn)

2. If you want to customize the serialization of a type of objects,
   but you cannot access or modify the corresponding class, you can
   register the class with the serializer you use:

   .. code-block:: python

      import ray
      import threading

      class A:
          def __init__(self, x):
              self.x = x
              self.lock = threading.Lock()  # could not be serialized!

      ray.get(ray.put(A(1)))  # fail!

      def custom_serializer(a):
          return a.x

      def custom_deserializer(b):
          return A(b)

      # Register serializer and deserializer for class A:
      ray.util.register_serializer(
        A, serializer=custom_serializer, deserializer=custom_deserializer)
      ray.get(ray.put(A(1)))  # success!

      # You can deregister the serializer at any time.
      ray.util.deregister_serializer(A)
      ray.get(ray.put(A(1)))  # fail!

      # Nothing happens when deregister an unavailable serializer.
      ray.util.deregister_serializer(A)

   NOTE: Serializers are managed locally for each Ray worker. So for every Ray worker,
   if you want to use the serializer, you need to register the serializer. Deregister
   a serializer also only applies locally.

   If you register a new serializer for a class, the new serializer would replace
   the old serializer immediately in the worker. This API is also idempotent, there are
   no side effects caused by re-registering the same serializer.

3. We also provide you an example, if you want to customize the serialization
   of a specific object:

   .. code-block:: python

     import threading

     class A:
         def __init__(self, x):
             self.x = x
             self.lock = threading.Lock()  # could not serialize!

     ray.get(ray.put(A(1)))  # fail!

     class SerializationHelperForA:
         """A helper class for serialization."""
         def __init__(self, a):
             self.a = a

         def __reduce__(self):
             return A, (self.a.x,)

     ray.get(ray.put(SerializationHelperForA(A(1))))  # success!
     # the serializer only works for a specific object, not all A
     # instances, so we still expect failure here.
     ray.get(ray.put(A(1)))  # still fail!


Troubleshooting
---------------

Use ``ray.util.inspect_serializability`` to identify tricky pickling issues. This function can be used to trace a potential non-serializable object within any Python object -- whether it be a function, class, or object instance.

Below, we demonstrate this behavior on a function with a non-serializable object (threading lock):

.. code-block:: python

    from ray.util import inspect_serializability
    import threading

    lock = threading.Lock()

    def test():
        print(lock)

    inspect_serializability(test, name="test")

The resulting output is:


.. code-block:: bash

    =============================================================
    Checking Serializability of <function test at 0x7f9ca9843950>
    =============================================================
    !!! FAIL serialization: can't pickle _thread.lock objects
    Detected 1 global variables. Checking serializability...
        Serializing 'lock' <unlocked _thread.lock object at 0x7f9cb83fb210>...
        !!! FAIL serialization: can't pickle _thread.lock objects
        WARNING: Did not find non-serializable object in <unlocked _thread.lock object at 0x7f9cb83fb210>. This may be an oversight.
    =============================================================
    Variable:

        lock [obj=<unlocked _thread.lock object at 0x7f9cb83fb210>, parent=<function test at 0x7f9ca9843950>]

    was found to be non-serializable. There may be multiple other undetected variables that were non-serializable.
    Consider either removing the instantiation/imports of these variables or moving the instantiation into the scope of the function/class.
    If you have any suggestions on how to improve this error message, please reach out to the Ray developers on github.com/ray-project/ray/issues/
    =============================================================

Known Issues
------------

Users could experience memory leak when using certain python3.8 & 3.9 versions. This is due to `a bug in python's pickle module <https://bugs.python.org/issue39492>`_.

This issue has been solved for Python 3.8.2rc1, Python 3.9.0 alpha 4 or late versions.

