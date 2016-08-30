# Remote Functions

This document elaborates a bit on what can and cannot be done with remote
functions. Remote functions are written like regular Python functions, but with
the `@ray.remote` decorator on top.

```python
@ray.remote()
def increment(n):
  return n + 1
```

## Invocation and Execution

Normally, when a Python functions is invoked, it executes immediately and
returns once the execution has completed. For remote functions, we distinguish
between the invocation of the function (that is, when the user calls the
function) and the execution of the function (that is, when the function's
implementation is run).

**Invocation:** To invoke a remote function, the user calls the remote function.

```python
x_id = increment(1)
```

This line sends a message to the scheduler asking it to schedule the task of
executing the function `increment` with the argument `1`. It then returns an
object id for the eventual output of the task. This takes place almost
instantaneously and does not wait for the actual task to be executed.

When calling a remote function, the return value always consists of one or more
object ids. If you want the actual value, call `ray.get(x_id)`, which will
wait for the task to execute and will return the resulting value.

**Execution:** Eventually, the scheduler will schedule the task on a worker. The
worker will then run the increment function and store the function's output in
the worker's local object store.

At that point, the scheduler will be notified that the outputs of the task are
ready, and tasks that depend on those outputs can be scheduled.

## Serializable Types

It is not reasonable to store arbitrary Python objects in the object store or to
ship arbitrary Python objects between machines (for example a file handle on one
machine may not be meaningful on another). Currently, we serialize only specific
types in the object store. **The serializable types are:**

1. Primitive data types (for example, `1`, `1.0`, `"hello"`, `True`)
2. Numpy arrays
3. Object IDs
4. Lists, tuples, and dictionaries of other serializable types, but excluding
custom classes (for example, `[1, 1.0, "hello"]`, `{True: "hi", 1: ["hi"]}`)
5. Custom classes where the user has provided `serialize` and `desererialize`
methods

If you wish to define a custom class and to allow it to be serialized in the
object store, you must implement `serialize` and `deserialize` methods which
convert the object to and from primitive data types. A simple example is shown
below.

```python
BLOCK_SIZE = 1000

class ExampleClass(object):
  def __init__(self, field1, field2):
    # This example assumes that field1 and field2 are serializable types.
    self.field1 = field1
    self.field2 = field2

  @staticmethod
  def deserialize(primitives):
    (field1, field2) = primitives
    return ExampleClass(field1, field2)

  def serialize(self):
    return (self.field1, self.field2)
```
