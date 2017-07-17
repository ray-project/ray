Serialization in the Object Store
=================================

This document describes what Python objects Ray can and cannot serialize into
the object store. Once an object is placed in the object store, it is immutable.

There are a number of situations in which Ray will place objects in the object
store.

1. The return values of a remote function.
2. The value ``x`` in a call to ``ray.put(x)``.
3. Arguments to remote functions (except for simple arguments like ints or
   floats).

A Python object may have an arbitrary number of pointers with arbitrarily deep
nesting. To place an object in the object store or send it between processes,
it must first be converted to a contiguous string of bytes. This process is
known as serialization. The process of converting the string of bytes back into a
Python object is known as deserialization. Serialization and deserialization
are often bottlenecks in distributed computing.

Pickle is one example of a library for serialization and deserialization in
Python.

.. code-block::python

  import pickle

  pickle.dumps([1, 2, 3])  # prints b'\x80\x03]q\x00(K\x01K\x02K\x03e.'
  pickle.loads(b'\x80\x03]q\x00(K\x01K\x02K\x03e.')  # prints [1, 2, 3]

Pickle (and the variant we use, cloudpickle) is general-purpose. It can
serialize a large variety of Python objects. However, for numerical workloads,
pickling and unpickling can be inefficient. For example, if multiple processes
want to access a Python list of numpy arrays, each process must unpickle the
list and create its own new copies of the arrays. This can lead to high memory
overheads, even when all processes are read-only and could easily share memory.

In Ray, we optimize for numpy arrays by using the `Apache Arrow`_ data format.
When we deserialize a list of numpy arrays from the object store, we still
create a Python list of numpy array objects. However, rather than copy each
numpy array, each numpy array object holds a pointer to the relevant array held
in shared memory. There are some advantages to this form of serialization.

- Deserialization can be very fast.
- Memory is shared between processes so worker processes can all read the same
  data without having to copy it.

.. _`Apache Arrow`: https://arrow.apache.org/

What Objects Does Ray Handle
----------------------------

Ray does not currently support serialization of arbitrary Python objects.  The
set of Python objects that Ray can serialize using Arrow includes the following.

1. Primitive types: ints, floats, longs, bools, strings, unicode, and numpy
   arrays.
2. Any list, dictionary, or tuple whose elements can be serialized by Ray.

For a more general object, Ray will first attempt to serialize the object by
unpacking the object as a dictionary of its fields. This behavior is not
correct in all cases. If Ray cannot serialize the object as a dictionary of its
fields, Ray will fall back to using pickle. However, using pickle will likely
be inefficient.

Notes and limitations
---------------------

- We currently handle certain patterns incorrectly, according to Python
  semantics. For example, a list that contains two copies of the same list will
  be serialized as if the two lists were distinct.

  .. code-block:: python

    l1 = [0]
    l2 = [l1, l1]
    l3 = ray.get(ray.put(l2))

    l2[0] is l2[1]  # True.
    l3[0] is l3[1]  # False.

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

- Whenever possible, use numpy arrays for maximum performance.

Last Resort Workaround
----------------------

If you find cases where Ray serialization doesn't work or does something
unexpected, please `let us know`_ so we can fix it. In the meantime, you may
have to resort to writing custom serialization and deserialization code (e.g.,
calling pickle by hand).

.. _`let us know`: https://github.com/ray-project/ray/issues

.. code-block:: python

  import pickle

  @ray.remote
  def f(complicated_object):
      # Deserialize the object manually.
      obj = pickle.loads(complicated_object)
      return "Successfully passed {} into f.".format(obj)

  # Define a complicated object.
  l = []
  l.append(l)

  # Manually serialize the object and pass it in as a string.
  ray.get(f.remote(pickle.dumps(l)))  # prints 'Successfully passed [[...]] into f.'

**Note:** If you have trouble with pickle, you may have better luck with
cloudpickle.
