# Serialization in the Object Store

This document describes what Python objects Ray can and cannot serialize into
the object store. Once an object is placed in the object store, it is immutable.

There are a number of situations in which Ray will place objects in the object
store.

1. When a remote function returns, its return values are stored in the object
store.
2. A call to `ray.put(x)` places `x` in the object store.
3. When large objects or objects other than simple primitive types are passed as
arguments into remote functions, they will be placed in the object store.

A normal Python object may have pointers all over the place, so to place an
object in the object store or send it between processes, it must first be
converted to a contiguous string of bytes. This process is known as
serialization. The process of turning the string of bytes back into a Python
object is known as deserialization. The processes of serialization and
deserialization are often bottlenecks in distributed computing.

Pickle is one example of a library for serialization and deserialization in
Python.

```python
import pickle

pickle.dumps([1, 2, 3])  # prints '(lp0\nI1\naI2\naI3\na.'
pickle.loads("(lp0\nI1\naI2\naI3\na.")  # prints [1, 2, 3]
```

Pickle (and its variants) are pretty general. They can successfully serialize a
large variety of Python objects. However, for numerical workloads, pickling and
unpickling can be inefficient. For example, when unpickling a list of numpy
arrays, pickle will create completely new arrays in memory. In Ray, when we
deserialize a list of numpy arrays from the object store, we will create a list
of numpy array objects in Python, but each numpy array object is essentially
just a pointer to the relevant location in the object store's memory. There are
some advantages to this form of serialization.

- Deserialization can be very fast.
- Memory is shared between processes so worker processes can all read the same
data without having to copy it.

## What Objects Does Ray Handle

However, Ray is not currently capable of serializing arbitrary Python objects.
The set of Python objects that Ray can serialize includes the following.

1. Primitive types: ints, floats, longs, bools, strings, unicode, and numpy
arrays.
2. Any list, dictionary, or tuple whose elements can be serialized by Ray.
3. Objects whose classes can be registered with `ray.register_class`. This point
is described below.

## Registering Custom Classes

We currently support serializing a limited subset of custom classes. For
example, suppose you define a new class `Foo` as follows.

```python
class Foo(object):
  def __init__(self, a, b):
    self.a = a
    self.b = b
```

Simply calling `ray.put(Foo(1, 2))` will fail with a message like

```
Ray does not know how to serialize the object <__main__.Foo object at 0x1077d7c50>.
```

This can be addressed by calling `ray.register_class(Foo)`.

```python
import ray

ray.init(start_ray_local=True, num_workers=1)

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
```

Under the hood, `ray.put` essentially replaces `f` with `f.__dict__`, which is
just the dictionary `{"a": 1, "b": 2}`. Then during deserialization, `ray.get`
constructs a new `Foo` object from the dictionary of fields.

This naive substitution won't work in all cases. For example if we want to
serialize Python objects of type `function` (for example `f = lambda x: x + 1`),
this simple scheme doesn't quite work, and `ray.register_class(type(f))` will
give an error message. In these cases, we can fall back to pickle (actually we
use cloudpickle).

```python
# This call tells Ray to fall back to using pickle when it encounters objects of
# type function.
f = lambda x: x + 1
ray.register_class(type(f), pickle=True)

f_new = ray.get(ray.put(f))
f_new(0)  # prints 1
```

However, it's best to avoid using pickle for efficiency reasons. If you find
yourself needing to pickle certain objects, consider trying to use more
efficient data structures like arrays.

**Note:** Another setting where the naive replacement of an object with its
`__dict__` attribute fails is where an object recursively contains itself (or
multiple objects recursively contain each other). For example, consider the code
below.

```python
l = []
l.append(l)
```

It will throw an exception with a message like the following.

```
This object exceeds the maximum recursion depth. It may contain itself recursively.
```

# Last Resort Workaround

If you find cases where Ray doesn't work or does the wrong thing, please let us
know so we can fix it. In the meantime, you can do your own custom serialization
and deserialization (for example by calling pickle by hand). Or by writing your
own custom serializer and deserializer.

```python
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
```

**Note:** If you have trouble with pickle, you may have better luck with
cloudpickle.
