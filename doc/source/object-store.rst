Object Store (Plasma)
=====================

Plasma is an in-memory object store that is being developed as part of `Apache Arrow`_. Ray uses Plasma to efficiently transfer objects across different processes and different nodes.

All objects in Plasma object store are **immutable** and held in shared memory. This is so that they can be accessed efficiently by many workers on the same node.

Note that Plasma does not support all data-types, and in certain cases it may be necessary to either write your own serialization protocol or use Actors to hold objects and transfer object state (i.e., weight matrices) among Ray workers.

Overview
--------

Ray will place objects in a node object store in the following situations:

1. Calling ``ray.put``

.. code:: python

    y = 2
    # This places `2` into the object store.
    object_id = ray.put(y)

2. The return values of a remote function.

.. code:: python

    @ray.remote
    def remote_function():
        return 1

    # This places `1` into the object store.
    object_id = remote_function.remote()


3. Arguments to remote functions (except for simple arguments like ints or
   floats).

.. code:: python

    @ray.remote
    def remote_function(y):
        # Note that inside the remote function, the actual argument is provided.
        return len(y)

    argument = [1, 2, 3, 4]
    # This implicitly places `argument` into the object store.
    remote_function.remote(argument)

In the single node setting, all Ray workers can read the same object in the object store without copying (zero-copy reads). Once an object is placed in the object store, it is immutable. Any writes to the read-only object will result in a copy into the local process memory.

Distributed Setting
~~~~~~~~~~~~~~~~~~~

Each node has its own object store. When data is put into a node object store, it
does not get automatically broadcasted; it remains local until requested by another
task on another node.


Numpy Arrays
------------

Ray optimizes for numpy arrays by using the `Apache Arrow`_ data format.
Each numpy array object holds a pointer to the relevant array held in shared memory.

There are some advantages to this form of serialization:

- Deserialization can be very fast.
- Memory is shared between processes so worker processes can all read the same
  data without having to copy it.

**Nested Numpy Arrays:** When we deserialize a list of numpy arrays from the object store, we still create a Python list of numpy array objects. However, rather than copy each numpy array,

.. _`Apache Arrow`: https://arrow.apache.org/

Comparison to Pickle
~~~~~~~~~~~~~~~~~~~~

Pickle is standard Python serialization library. However, for numerical workloads, pickling and unpickling can be inefficient.

For example, if multiple processes want to access a Python list of numpy arrays, each process must unpickle the list and create its own new copies of the arrays. This can lead to high memory overheads, even when all processes are read-only and could easily share memory.


Supported Datatypes
-------------------

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

- Whenever possible, use numpy arrays for maximum performance.

Last Resort Workaround
~~~~~~~~~~~~~~~~~~~~~~

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

Advanced: Huge Pages
--------------------

On Linux, it is possible to increase the write throughput of the Plasma object store by using huge pages. See the `Configuration page <configure.html#using-the-object-store-with-huge-pages>`_ for information on how to use huge pages in Ray.
