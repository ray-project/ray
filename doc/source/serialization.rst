Serialization in the Object Store
=================================

This document describes what Python objects Ray can and cannot serialize into
the object store. Once an object is placed in the object store, it is immutable.

There are a number of situations in which Ray will place objects in the object
store.

1. The return values of a remote function.
   store.
2. ``x`` in a call to ``ray.put(x)``.
3. Large objects or objects other than simple primitive types that are passed
   as arguments into remote functions.

A Python object may have an arbitrary number of pointers with arbitrarily deep
nesting. To place an object in the object store or send it between processes,
it must first be converted to a contiguous string of bytes. This process is
known as serialization. The process of converting the string of bytes back into a
Python object is known as deserialization. Serialization and deserialization
are often bottlenecks in distributed computing, if the time needed to compute
on the data is relatively low.

Pickle is one example of a library for serialization and deserialization in
Python.

.. code-block::python

  import pickle

  pickle.dumps([1, 2, 3])  # prints b'\x80\x03]q\x00(K\x01K\x02K\x03e.'
  pickle.loads(b'\x80\x03]q\x00(K\x01K\x02K\x03e.')  # prints [1, 2, 3]

Pickle (and the variant we use, cloudpickle) is general-purpose. They can
serialize a large variety of Python objects. However, for numerical workloads,
pickling and unpickling can be inefficient. For example, if multiple processes
want to access a Python list of numpy arrays, each process must unpickle the
list and create its own new copies of the arrays. This can lead to high memory
overheads, even when all processes are read-only and could easily share memory.

In Ray, we optimize for numpy arrays by using the `Apache Arrow`_ data format.
When we deserialize a list of numpy arrays from the object store, we still
create a Python list of numpy array objects.  However, rather than copy each
numpy array over again, each numpy array object is essentially a pointer to its
address in shared memory. There are some advantages to this form of
serialization.

- Deserialization can be very fast.
- Memory is shared between processes so worker processes can all read the same
  data without having to copy it.

.. _`Apache Arrow`: https://arrow.apache.org/

What Objects Does Ray Handle
----------------------------

Ray does not currently support serialization of arbitrary Python objects.  The
set of Python objects that Ray can serialize includes the following.

1. Primitive types: ints, floats, longs, bools, strings, unicode, and numpy
   arrays.
2. Any list, dictionary, or tuple whose elements can be serialized by Ray.
3. Objects whose classes can be registered with ``ray.register_class``. This
   point is described below.

Registering Custom Classes
--------------------------

We currently support serializing a limited subset of custom classes. For
example, suppose you define a new class ``Foo`` as follows.

.. code-block:: python

  class Foo(object):
    def __init__(self, a, b):
      self.a = a
      self.b = b

Simply calling ``ray.put(Foo(1, 2))`` will fail with a message like

.. code-block:: python

  Ray does not know how to serialize the object <__main__.Foo object at 0x1077d7c50>.

This can be addressed by calling ``ray.register_class(Foo)``.

.. code-block:: python

  import ray

  ray.init(num_workers=10)

  # Define a custom class.
  class Foo(object):
    def __init__(self, a, b):
      self.a = a
      self.b = b

  # Calling ray.register_class(Foo) ships the class definition to all of the
  # workers so that workers know how to construct new Foo objects.
  ray.register_class(Foo)

  # Create a Foo object, place it in the object store, and retrieve it.
  f = Foo(1, 2)
  f_id = ray.put(f)
  ray.get(f_id)  # prints <__main__.Foo at 0x1078128d0>

Under the hood, ``ray.put`` places ``f.__dict__``, the dictionary of attributes
of ``f``, into the object store instead of ``f`` itself. In this case, this is
the dictionary, ``{"a": 1, "b": 2}``. Then during deserialization, ``ray.get``
constructs a new ``Foo`` object from the dictionary of fields.

This naive substitution won't work in all cases. For example, this scheme does
not support Python objects of type ``function`` (e.g., ``f = lambda x: x +
1``). In these cases, the call to ``ray.register_class`` will give an error
message, and you should fall back to pickle.

.. code-block:: python

  # This call tells Ray to fall back to using pickle when it encounters objects
  # of type function.
  f = lambda x: x + 1
  ray.register_class(type(f), pickle=True)

  f_new = ray.get(ray.put(f))
  f_new(0)  # prints 1

However, it's best to avoid using pickle for the efficiency reasons described
above. If you find yourself needing to pickle certain objects, consider trying
to use more efficient data structures like arrays.

**Note:** Another setting where the naive replacement of an object with its
``__dict__`` attribute fails is recursion, e.g., an object contains itself or
multiple objects contain each other. To see more examples of this, see the
section `Notes and Limitations`_.

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

- If you need to pass a custom class into a remote function, you should call
  ``ray.register_class`` on the class **before defining the remote function**.

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
