---
myst:
  html_meta:
    description: "How Ray serializes data for the Plasma object store, with NumPy zero-copy reads, read-only array pitfalls, and custom serializers."
---

(serialization-guide)=

# Serialization

Ray processes don't share memory space, so Ray must *serialize* and *deserialize* any data that moves between workers and nodes. Ray uses the [Plasma object store](https://arrow.apache.org/blog/2017/08/08/plasma-in-memory-object-store/) to transfer objects efficiently between processes and nodes. Workers on the same node share NumPy arrays in the object store without copying them, through zero-copy deserialization.

## Overview

Ray uses a customized backport of [Pickle protocol version 5](https://www.python.org/dev/peps/pep-0574/) in place of the original PyArrow serializer. It removes several limitations of the PyArrow serializer, such as its inability to serialize recursive objects.

Ray is compatible with Pickle protocol version 5. With the help of cloudpickle, Ray also serializes a wider range of objects, such as lambda functions, nested functions, and dynamic classes.

(plasma-store)=

### Plasma object store

Plasma is an in-memory object store that started as part of Apache Arrow. Before the Ray 1.0.0 release, the Ray project forked Arrow's Plasma code into the Ray code base to develop it independently for Ray's architecture and performance needs.

Ray uses Plasma to transfer objects efficiently between processes and nodes. All objects in the Plasma store are immutable and live in shared memory, so many workers on the same node can access them efficiently.

Each node has its own object store. Ray doesn't automatically broadcast data in an object store to other nodes. The data stays local to the writer until a task or actor on another node requests it.

### NumPy arrays

Ray optimizes for NumPy arrays by using Pickle protocol 5 with out-of-band data. Ray stores each NumPy array as a read-only object, and all Ray workers on the same node read the array from the object store without copying it. Each NumPy array object in a worker process holds a pointer to the array in shared memory. To write to the read-only object, you must first copy it into the local process memory.

:::{tip}
You can often avoid serialization issues by using only native types, such as NumPy arrays, lists or dictionaries of NumPy arrays, and other primitive types. For an object that Ray can't serialize, hold it in an actor instead.
:::

### Fixing "assignment destination is read-only"

Because Ray puts NumPy arrays in the object store, the arrays are read-only when Ray deserializes them as arguments to remote functions. For example, the following code crashes:

```{literalinclude} /core/doc_code/deser.py
```

If you need to mutate the array, copy it at the destination with `arr = arr.copy()`. Copying the array is effectively like disabling the zero-copy deserialization that Ray provides.

## Serialization notes

- Ray uses Pickle protocol version 5. Most Python distributions default to Pickle protocol 3. Protocols 4 and 5 are more efficient than protocol 3 for larger objects.

- For non-native objects, Ray always keeps a single copy, even if an object refers to it multiple times:

  ```{testcode}
  import ray
  import numpy as np

  obj = [np.zeros(42)] * 99
  l = ray.get(ray.put(obj))
  assert l[0] is l[1]  # no problem!
  ```

- Whenever possible, use NumPy arrays or Python collections of NumPy arrays for maximum performance.

- Lock objects are mostly unserializable, because copying a lock is meaningless and could cause serious concurrency problems. If your object contains a lock, you might need a workaround.

## Zero-copy serialization for read-only tensors

Ray provides optional zero-copy serialization for read-only PyTorch tensors. Ray converts these tensors to NumPy arrays and serializes them with pickle5's zero-copy buffer sharing. Skipping the copy of the underlying tensor data can improve performance when you pass large tensors between tasks or actors.

:::{caution}
PyTorch doesn't natively support read-only tensors, so use this feature with caution. With the feature enabled, Ray doesn't copy the tensor, so a write goes to shared memory. If two processes run on the same node, a change that one process makes to a tensor after `ray.get()` could appear in the other process.
:::

This feature works best under the following conditions:

- The tensor has `requires_grad = False`, meaning it's detached from the autograd graph.

- The tensor is contiguous in memory, so `tensor.is_contiguous()` returns `True`.

- The tensor resides in CPU memory, where the performance benefits are larger.

- You aren't using Ray Direct Transport.

This feature is off by default. To enable it, set the `RAY_ENABLE_ZERO_COPY_TORCH_TENSORS` environment variable. To enable zero-copy serialization in the driver process, set the variable outside your script before you run it:

```bash
export RAY_ENABLE_ZERO_COPY_TORCH_TENSORS=1
```

The following example uses zero-copy serialization to calculate the sum of a 1 GiB tensor with `ray.get()`:

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
import ray
import torch
import time

ray.init(runtime_env={"env_vars": {"RAY_ENABLE_ZERO_COPY_TORCH_TENSORS": "1"}})

@ray.remote
def process(tensor):
    return tensor.sum()

x = torch.ones(1024, 1024, 256)
start_time = time.perf_counter()
result = ray.get(process.remote(x))
elapsed_time = time.perf_counter() - start_time
print(f"Elapsed time: {elapsed_time}s")

assert result == x.sum()
```

In this example, enabling zero-copy serialization reduces end-to-end latency by 66.3%:

```bash
# Without Zero-Copy Serialization
Elapsed time: 23.53883756196592s
# With Zero-Copy Serialization
Elapsed time: 7.933729998010676s
```

## Customized serialization

Ray's default serializer combines pickle5 and cloudpickle. It might not work for you, because it fails to serialize some objects or is too slow for others. In those cases, customize the serialization process.

You can define a custom serialization process in at least three ways:

1. To customize serialization for a type of object whose code you can access, define a `__reduce__` function in the corresponding class. Most Python libraries use this approach. The following code shows an example:

   ```{testcode}
   import ray
   import sqlite3

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
   ```

   ```{testoutput}
   <sqlite3.Connection object at ...>
   <sqlite3.Connection object at ...>
   ```

1. To customize serialization for a type of object whose class you can't access or modify, register the class with the serializer you use:

   ```{testcode}
   import ray
   import threading

   class A:
       def __init__(self, x):
           self.x = x
           self.lock = threading.Lock()  # could not be serialized!

   try:
     ray.get(ray.put(A(1)))  # fail!
   except TypeError:
     pass

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
   try:
     ray.get(ray.put(A(1)))  # fail!
   except TypeError:
     pass

   # Nothing happens when deregister an unavailable serializer.
   ray.util.deregister_serializer(A)
   ```

   :::{note}
   Each Ray worker manages its serializers locally, so register the serializer in every Ray worker that uses it. Calling `ray.util.deregister_serializer` also applies only to the local worker.
   :::

   If you register a new serializer for a class, the new serializer replaces the old one in the worker immediately. The API is also idempotent, so re-registering the same serializer has no side effects.

1. To customize the serialization of a specific object, wrap it in a helper class that defines `__reduce__`, as in the following example:

   ```{testcode}
   import threading

   class A:
       def __init__(self, x):
           self.x = x
           self.lock = threading.Lock()  # could not serialize!

   try:
      ray.get(ray.put(A(1)))  # fail!
   except TypeError:
      pass

   class SerializationHelperForA:
       """A helper class for serialization."""
       def __init__(self, a):
           self.a = a

       def __reduce__(self):
           return A, (self.a.x,)

   ray.get(ray.put(SerializationHelperForA(A(1))))  # success!
   # the serializer only works for a specific object, not all A
   # instances, so we still expect failure here.
   try:
      ray.get(ray.put(A(1)))  # still fail!
   except TypeError:
      pass
   ```

(custom-exception-serializer)=

## Custom serializers for exceptions

When a task raises an exception that the default pickle mechanism can't serialize, register a custom serializer to handle it. Register the serializer in the driver and in all workers.

```{testcode}
import ray
import threading

class CustomError(Exception):
    def __init__(self, message, data):
        self.message = message
        self.data = data
        self.lock = threading.Lock() # Cannot be serialized

def custom_serializer(exc):
    return {"message": exc.message, "data": str(exc.data)}

def custom_deserializer(state):
    return CustomError(state["message"], state["data"])

# Register in the driver
ray.util.register_serializer(
    CustomError,
    serializer=custom_serializer,
    deserializer=custom_deserializer
)

@ray.remote
def task_that_registers_serializer_and_raises():
    # Register the custom serializer in the worker
    ray.util.register_serializer(
        CustomError,
        serializer=custom_serializer,
        deserializer=custom_deserializer
    )

    # Now raise the custom exception
    raise CustomError("Something went wrong", {"complex": "data"})

# The custom exception will be properly serialized across worker boundaries
try:
    ray.get(task_that_registers_serializer_and_raises.remote())
except ray.exceptions.RayTaskError as e:
    print(f"Caught exception: {e.cause}")  # This will be our CustomError
```

When a remote task raises a custom exception, Ray does the following:

1. Serializes the exception with your custom serializer.
1. Wraps it in a {class}`RayTaskError <ray.exceptions.RayTaskError>`.
1. Makes the deserialized exception available as `ray_task_error.cause`.

Whenever serialization fails, Ray throws an {class}`UnserializableException <ray.exceptions.UnserializableException>` containing the string representation of the original stack trace.

## Troubleshooting

Use `ray.util.inspect_serializability` to identify tricky pickling issues. The function traces a potential non-serializable object within any Python object, whether it's a function, a class, or an object instance.

The following example inspects a function that references a non-serializable threading lock:

```{testcode}
from ray.util import inspect_serializability
import threading

lock = threading.Lock()

def test():
    print(lock)

inspect_serializability(test, name="test")
```

The example produces the following output:

```{testoutput}
:options: +MOCK

  =============================================================
  Checking Serializability of <function test at 0x7ff130697e50>
  =============================================================
  !!! FAIL serialization: cannot pickle '_thread.lock' object
  Detected 1 global variables. Checking serializability...
      Serializing 'lock' <unlocked _thread.lock object at 0x7ff1306a9f30>...
      !!! FAIL serialization: cannot pickle '_thread.lock' object
      WARNING: Did not find non-serializable object in <unlocked _thread.lock object at 0x7ff1306a9f30>. This may be an oversight.
  =============================================================
  Variable:

  	FailTuple(lock [obj=<unlocked _thread.lock object at 0x7ff1306a9f30>, parent=<function test at 0x7ff130697e50>])

  was found to be non-serializable. There may be multiple other undetected variables that were non-serializable.
  Consider either removing the instantiation/imports of these variables or moving the instantiation into the scope of the function/class.
  =============================================================
  Check https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting for more information.
  If you have any suggestions on how to improve this error message, please reach out to the Ray developers on github.com/ray-project/ray/issues/
  =============================================================
```

For even more detailed information, set environmental variable `RAY_PICKLE_VERBOSE_DEBUG='2'` before importing Ray. This enables serialization with python-based backend instead of C-Pickle, so you can debug into python code at the middle of serialization. However, this would make serialization much slower.

## Known Issues

You might experience a memory leak with certain Python 3.8 and 3.9 versions, because of [a bug in Python's pickle module](https://bugs.python.org/issue39492).

Python 3.8.2rc1, Python 3.9.0 alpha 4, and later versions fix this issue.
