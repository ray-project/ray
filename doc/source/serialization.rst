Serialization
=============

Since Ray processes do not share memory space, data transferred between workers and nodes will need to **serialized** and **deserialized**. Ray uses the `Plasma object store <https://arrow.apache.org/docs/python/plasma.html>`_ to efficiently transfer objects across different processes and different nodes. Numpy arrays in the object store are shared between workers on the same node (zero-copy deserialization).

Plasma Object Store
-------------------

Plasma is an in-memory object store that is being developed as part of `Apache Arrow`_. Ray uses Plasma to efficiently transfer objects across different processes and different nodes. All objects in Plasma object store are **immutable** and held in shared memory. This is so that they can be accessed efficiently by many workers on the same node.

Each node has its own object store. When data is put into the object store, it does not get automatically broadcasted to other nodes. Data remains local to the writer until requested by another task or actor on another node.

Overview
--------

Objects that are serialized for transfer among Ray processes go through three stages:

**1. Serialize directly**: Below is the set of Python objects that Ray can serialize using ``memcpy``:

1. Primitive types: ints, floats, longs, bools, strings, unicode, and numpy arrays.

2. Any list, dictionary, or tuple whose elements can be serialized by Ray.

**2. ``__dict__`` serialization**: If a direct usage is not possible, Ray will recursively extract the object’s ``__dict__`` and serialize that directly. This behavior is not correct in all cases.

**3. Cloudpickle**:  Ray falls back to ``cloudpickle`` as a final attempt for serialization. This may be slow.


Numpy Arrays
------------

Ray optimizes for numpy arrays by using the `Apache Arrow`_ data format.
The numpy array is stored as a read-only object, and all Ray workers on the same node can read the numpy array in the object store without copying (zero-copy reads). Each numpy array object in the worker process holds a pointer to the relevant array held in shared memory. Any writes to the read-only object will require the user to first copy it into the local process memory.

.. tip:: You can often avoid serialization issues by using only native types (e.g., numpy arrays or lists/dicts of numpy arrays and other primitive types), or by using Actors hold objects that cannot be serialized.

Serialization notes and limitations
-----------------------------------

- Ray currently handles certain patterns incorrectly, according to Python
  semantics. For example, a list that contains two copies of the same list will
  be serialized as if the two lists were distinct.

  .. code-block:: python

    l1 = [0]
    l2 = [l1, l1]
    l3 = ray.get(ray.put(l2))

    assert l2[0] is l2[1]
    assert not l3[0] is l3[1]

- For reasons similar to the above example, we also do not currently handle
  objects that recursively contain themselves (this may be common in graph-like
  data structures).

  .. code-block:: python

    l = []
    l.append(l)

    # Try to put this list that recursively contains itself in the object store.
    ray.put(l)

  This will throw an exception with a message like the following.

  .. code-block:: bash

    This object exceeds the maximum recursion depth. It may contain itself recursively.

- Whenever possible, use numpy arrays or Python collections of numpy arrays for maximum performance.

Last resort: Custom Serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If none of these options work, you can try registering a custom serializer.

.. autofunction:: ray.register_custom_serializer
  :noindex:

Below is an example of using ``ray.register_custom_serializer``:

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

      object_id = ray.put(Foo(100))
      assert ray.get(object_id).value == 100


If you find cases where Ray serialization doesn't work or does something unexpected, please `let us know`_ so we can fix it.

.. _`let us know`: https://github.com/ray-project/ray/issues

Advanced: Huge Pages
~~~~~~~~~~~~~~~~~~~~

On Linux, it is possible to increase the write throughput of the Plasma object store by using huge pages. See the `Configuration page <configure.html#using-the-object-store-with-huge-pages>`_ for information on how to use huge pages in Ray.


.. _`Apache Arrow`: https://arrow.apache.org/

