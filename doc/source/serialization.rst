.. _serialization-guide:

Serialization
=============

Since Ray processes do not share memory space, data transferred between workers and nodes will need to **serialized** and **deserialized**. Ray uses the `Plasma object store <https://arrow.apache.org/docs/python/plasma.html>`_ to efficiently transfer objects across different processes and different nodes. Numpy arrays in the object store are shared between workers on the same node (zero-copy deserialization).

Plasma Object Store
-------------------

Plasma is an in-memory object store that is being developed as part of `Apache Arrow`_. Ray uses Plasma to efficiently transfer objects across different processes and different nodes. All objects in Plasma object store are **immutable** and held in shared memory. This is so that they can be accessed efficiently by many workers on the same node.

Each node has its own object store. When data is put into the object store, it does not get automatically broadcasted to other nodes. Data remains local to the writer until requested by another task or actor on another node.

Overview
--------

Ray has decided to use a customed `Pickle protocol version 5 <https://www.python.org/dev/peps/pep-0574/>`_ backport to replace the original PyArrow serializer. This gets rid of several previous limitations (e.g. cannot serialize recursive objects).

Ray is currently compatible with Pickle protocol version 5, while Ray supports serialization of a wilder range of objects (e.g. lambda & nested functions, dynamic classes) with the support of cloudpickle.

Numpy Arrays
------------

Ray optimizes for numpy arrays by using Pickle protocol 5 with out-of-band data.
The numpy array is stored as a read-only object, and all Ray workers on the same node can read the numpy array in the object store without copying (zero-copy reads). Each numpy array object in the worker process holds a pointer to the relevant array held in shared memory. Any writes to the read-only object will require the user to first copy it into the local process memory.

.. tip:: You can often avoid serialization issues by using only native types (e.g., numpy arrays or lists/dicts of numpy arrays and other primitive types), or by using Actors hold objects that cannot be serialized.

Serialization notes
-------------------

- Ray is currently using Pickle protocol version 5. The default pickle protocol used by most python distributions is protocol 3. Protocol 4 & 5 are more efficient than protocol 3 for larger objects.

- Ray may create extra copies of simple native objects (e.g. list, and this is also the default behavior of Pickle Protocol 4 & 5), but recursive objects are treated carefully without any issues:

  .. code-block:: python

    l1 = [0]
    l2 = [l1, l1]
    l3 = ray.get(ray.put(l2))

    assert l2[0] is l2[1]
    assert l3[0] is l3[1]  # will raise AssertionError for protocol 4 & 5, but not protocol 3

    l = []
    l.append(l)

    # Try to put this list that recursively contains itself in the object store.
    ray.put(l)  # ok

- For non-native objects, Ray will always keep a single copy even it is referred multiple times in an object:

  .. code-block:: python

    import numpy as np
    obj = [np.zeros(42)] * 99
    l = ray.get(ray.put(obj))
    assert l[0] is l[1]  # no problem!

- Whenever possible, use numpy arrays or Python collections of numpy arrays for maximum performance.

- Lock objects are mostly unserializable, because copying a lock is meaningless and could cause serious concurrency problems. You may have to come up with a workaround if your object contains a lock.

Last resort: Custom Serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If none of these options work, you can try registering a custom serializer with ``ray.register_custom_serializer`` (:ref:`docstring <ray-register_custom_serializer-ref>`):

.. code-block:: python

      import ray

      ray.init()

      class Foo(object):
          def __init__(self, value):
              self.value = value

      def custom_serializer(obj):
          return obj.value

      def custom_deserializer(value):
          object = Foo()
          object.value = value
          return object

      ray.register_custom_serializer(
          Foo, serializer=custom_serializer, deserializer=custom_deserializer)

      object_ref = ray.put(Foo(100))
      assert ray.get(object_ref).value == 100


If you find cases where Ray serialization doesn't work or does something unexpected, please `let us know`_ so we can fix it.

.. _`let us know`: https://github.com/ray-project/ray/issues

Advanced: Huge Pages
~~~~~~~~~~~~~~~~~~~~

On Linux, it is possible to increase the write throughput of the Plasma object store by using huge pages. See the `Configuration page <configure.html#using-the-object-store-with-huge-pages>`_ for information on how to use huge pages in Ray.


.. _`Apache Arrow`: https://arrow.apache.org/


Known Issues
------------

Users could experience memory leak when using certain python3.8 & 3.9 versions. This is due to `a bug in python's pickle module <https://bugs.python.org/issue39492>`_.

This issue has been solved for Python 3.8.2rc1, Python 3.9.0 alpha 4 or late versions.

