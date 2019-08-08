Advanced Usage
==============

This page will cover some more advanced examples of using Ray's flexible programming model.

Nested Remote Functions
-----------------------

Remote functions can call other remote functions, resulting in nested tasks.
For example, consider the following.

.. code:: python

    @ray.remote
    def f():
        return 1

    @ray.remote
    def g():
        # Call f 4 times and return the resulting object IDs.
        return [f.remote() for _ in range(4)]

    @ray.remote
    def h():
        # Call f 4 times, block until those 4 tasks finish,
        # retrieve the results, and return the values.
        return ray.get([f.remote() for _ in range(4)])

Then calling ``g`` and ``h`` produces the following behavior.

.. code:: python

    >>> ray.get(g.remote())
    [ObjectID(b1457ba0911ae84989aae86f89409e953dd9a80e),
     ObjectID(7c14a1d13a56d8dc01e800761a66f09201104275),
     ObjectID(99763728ffc1a2c0766a2000ebabded52514e9a6),
     ObjectID(9c2f372e1933b04b2936bb6f58161285829b9914)]

    >>> ray.get(h.remote())
    [1, 1, 1, 1]

**One limitation** is that the definition of ``f`` must come before the
definitions of ``g`` and ``h`` because as soon as ``g`` is defined, it
will be pickled and shipped to the workers, and so if ``f`` hasn't been
defined yet, the definition will be incomplete.

Circular Dependencies
---------------------

Consider the following remote function.

.. code-block:: python

  @ray.remote(num_cpus=1, num_gpus=1)
  def g():
      return ray.get(f.remote())

When a ``g`` task is executing, it will release its CPU resources when it gets
blocked in the call to ``ray.get``. It will reacquire the CPU resources when
``ray.get`` returns. It will retain its GPU resources throughout the lifetime of
the task because the task will most likely continue to use GPU memory.

Cython Code in Ray
------------------

To use Cython code in Ray, run the following from directory ``$RAY_HOME/examples/cython``:

.. code-block:: bash

   pip install scipy # For BLAS example
   pip install -e .
   python cython_main.py --help

You can import the ``cython_examples`` module from a Python script or interpreter.

Notes
~~~~~

* You **must** include the following two lines at the top of any ``*.pyx`` file:

.. code-block:: python

   #!python
   # cython: embedsignature=True, binding=True

* You cannot decorate Cython functions within a ``*.pyx`` file (there are ways around this, but creates a leaky abstraction between Cython and Python that would be very challenging to support generally). Instead, prefer the following in your Python code:

.. code-block:: python

   some_cython_func = ray.remote(some_cython_module.some_cython_func)

* You cannot transfer memory buffers to a remote function (see ``example8``, which currently fails); your remote function must return a value
* Have a look at ``cython_main.py``, ``cython_simple.pyx``, and ``setup.py`` for examples of how to call, define, and build Cython code, respectively. The Cython `documentation <http://cython.readthedocs.io/>`_ is also very helpful.
* Several limitations come from Cython's own `unsupported <https://github.com/cython/cython/wiki/Unsupported>`_ Python features.
* We currently do not support compiling and distributing Cython code to ``ray`` clusters. In other words, Cython developers are responsible for compiling and distributing any Cython code to their cluster (much as would be the case for users who need Python packages like ``scipy``).
* For most simple use cases, developers need not worry about Python 2 or 3, but users who do need to care can have a look at the ``language_level`` Cython compiler directive (see `here <http://cython.readthedocs.io/en/latest/src/reference/compilation.html>`_).

Serialization
-------------

There are a number of situations in which Ray will place objects in the object
store. Once an object is placed in the object store, it is immutable. Situations include:

1. The return values of a remote function.
2. The value ``x`` in a call to ``ray.put(x)``.
3. Arguments to remote functions (except for simple arguments like ints or
   floats).

A Python object may have an arbitrary number of pointers with arbitrarily deep
nesting. To place an object in the object store or send it between processes,
it must first be converted to a contiguous string of bytes. Serialization and deserialization can often be a bottleneck.

Pickle is standard Python serialization library. However, for numerical workloads, pickling and unpickling can be inefficient. For example, if multiple processes want to access a Python list of numpy arrays, each process must unpickle the list and create its own new copies of the arrays. This can lead to high memory overheads, even when all processes are read-only and could easily share memory.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~

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
