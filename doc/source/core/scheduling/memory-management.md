---
myst:
  html_meta:
    description: "How Ray manages memory: ObjectRef reference counting, inspecting usage with ray memory, and memory-aware scheduling."
---

(memory)=

# Memory management

This page describes how memory management works in Ray.

To troubleshoot out-of-memory issues, see {ref}`Debugging out of memory <troubleshooting-out-of-memory>`.

## Concepts

Ray applications use memory in the following ways:

<!-- 
  https://docs.google.com/drawings/d/1wHHnAJZ-NsyIv3TUXQJTYpPz6pjB6PUm2M40Zbfb1Ak/edit -->

```{image} ../images/memory.svg
```

Ray system memory
: Ray uses this memory internally. It includes the following:

  - **GCS**: memory that stores the list of nodes and actors in the cluster. This amount is typically small.
  - **Raylet**: memory that the C++ raylet process on each node uses. You can't control this memory, but it's typically small.

Application memory
: Your application uses this memory. It includes the following:

  - **Worker heap**: memory that your application uses, for example in Python code or TensorFlow. The best measure of worker heap is the *resident set size (RSS)* of your application minus its *shared memory usage (SHR)* in commands such as `top`. Subtract SHR because the OS reports object store shared memory as shared with each worker. If you don't subtract SHR, you double count memory usage.
  - **Object store memory**: memory used when your application creates objects in the object store through `ray.put` and when it returns values from remote functions. Objects are reference counted and evicted when they fall out of scope. An object store server runs on each node. By default, when starting an instance, Ray reserves 30% of available memory. Control the size of the object store with [--object-store-memory](https://docs.ray.io/en/master/cluster/cli.html#cmdoption-ray-start-object-store-memory). On Linux, Ray allocates this memory in `/dev/shm`, which is shared memory, by default. On macOS, Ray uses `/tmp` on disk, which can affect performance compared to Linux. Because larger object stores degrade performance on macOS, Ray limits the default object store size there to 2 GiB. If you request a larger object store, Ray raises an error unless you set `RAY_ENABLE_MAC_LARGE_OBJECT_STORE=1`. Ray {ref}`spills objects to disk <object-spilling>` if the object store fills up. See {ref}`object-store-memory-size` for how Ray sizes the object store by default and how to change it.
  - **Object store shared memory**: memory used when your application reads objects through `ray.get`. If an object is already on the node, reading it doesn't cause additional allocations, so many actors and tasks can share large objects efficiently.

### `ObjectRef` reference counting

Ray implements distributed reference counting so that any object ref in scope in the cluster is pinned in the object store. This includes local Python references, arguments to pending tasks, and IDs serialized inside other objects.

(object-store-memory-size)=

## Object store memory size

By default, Ray sizes each node's object store at 30% of that node's available memory, up to a maximum of 200 GB. On Linux, Ray also caps the object store at 95% of the size of `/dev/shm`, because that's where it allocates object store memory. The 200 GB maximum means a large-memory node gets a 200 GB object store rather than 30% of its total memory. For example, a node with 2 TB of RAM gets a 200 GB object store, not 600 GB.

To set an absolute size, pass `--object-store-memory` to `ray start` or `object_store_memory` to `ray.init()`. Both take a number of bytes.

To change the defaults instead of setting a fixed size, set these environment variables before Ray starts:

- `RAY_DEFAULT_OBJECT_STORE_MEMORY_PROPORTION` overrides the 30% proportion. For example, set it to `0.5` to reserve half of available memory.
- `RAY_DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES` overrides the 200 GB maximum.

Ray reads both variables when it starts, so set them on every node before the Ray processes launch. Changing them after a cluster starts has no effect.

(debug-with-ray-memory)=

## Debugging using `ray memory`

Use the `ray memory` command to help track down which object refs are in scope and might be causing an `ObjectStoreFullError`.

Run `ray memory` from the command line while a Ray application runs to get a dump of all object refs that the driver, actors, and tasks in the cluster hold.

```text
======== Object references status: 2021-02-23 22:02:22.072221 ========
Grouping by node address...        Sorting by object size...


--- Summary for node address: 192.168.0.15 ---
Mem Used by Objects  Local References  Pinned Count  Pending Tasks  Captured in Objects  Actor Handles
287 MiB              4                 0             0              1                    0

--- Object references for node address: 192.168.0.15 ---
IP Address    PID    Type    Object Ref                                                Size    Reference Type      Call Site
192.168.0.15  6465   Driver  ffffffffffffffffffffffffffffffffffffffff0100000001000000  15 MiB  LOCAL_REFERENCE     (put object)
                                                                                                                  | test.py:
                                                                                                                  <module>:17

192.168.0.15  6465   Driver  a67dc375e60ddd1affffffffffffffffffffffff0100000001000000  15 MiB  LOCAL_REFERENCE     (task call)
                                                                                                                  | test.py:
                                                                                                                  :<module>:18

192.168.0.15  6465   Driver  ffffffffffffffffffffffffffffffffffffffff0100000002000000  18 MiB  CAPTURED_IN_OBJECT  (put object)  |
                                                                                                                   test.py:
                                                                                                                  <module>:19

192.168.0.15  6465   Driver  ffffffffffffffffffffffffffffffffffffffff0100000004000000  21 MiB  LOCAL_REFERENCE     (put object)  |
                                                                                                                   test.py:
                                                                                                                  <module>:20

192.168.0.15  6465   Driver  ffffffffffffffffffffffffffffffffffffffff0100000003000000  218 MiB  LOCAL_REFERENCE     (put object)  |
                                                                                                                  test.py:
                                                                                                                  <module>:20

--- Aggregate object store stats across all nodes ---
Plasma memory usage 0 MiB, 4 objects, 0.0% full
```


Each entry in this output corresponds to an object ref that's pinning an object in the object store. Each entry shows the following:

- Where the reference is, such as in the driver or in a worker.
- The type of reference. The following examples describe each type.
- The size of the object in bytes.
- The process ID and IP address where the object was instantiated.
- Where in the application the reference was created.

`ray memory` has options that help with memory debugging. For example, the `sort-by=OBJECT_SIZE` and `group-by=STACK_TRACE` arguments might help you track down the line of code where a memory leak occurs. To see all options, run `ray memory --help`.

Five types of references can keep an object pinned:

**1. Local object refs**

```{testcode}
import ray

@ray.remote
def f(arg):
    return arg

a = ray.put(None)
b = f.remote(None)
```

This example creates references to two objects. One is the object that `ray.put()` stores in the object store, and the other is the return value of `f.remote()`.

```text
--- Summary for node address: 192.168.0.15 ---
Mem Used by Objects  Local References  Pinned Count  Pending Tasks  Captured in Objects  Actor Handles
30 MiB               2                 0             0              0                    0

--- Object references for node address: 192.168.0.15 ---
IP Address    PID    Type    Object Ref                                                Size    Reference Type      Call Site
192.168.0.15  6867   Driver  ffffffffffffffffffffffffffffffffffffffff0100000001000000  15 MiB  LOCAL_REFERENCE     (put object)  |
                                                                                                                  test.py:
                                                                                                                  <module>:12

192.168.0.15  6867   Driver  a67dc375e60ddd1affffffffffffffffffffffff0100000001000000  15 MiB  LOCAL_REFERENCE     (task call)
                                                                                                                  | test.py:
                                                                                                                  :<module>:13
```

The `ray memory` output marks each of these as a `LOCAL_REFERENCE` in the driver process, but the annotation in the "Call Site" column indicates that the first was created as a "put object" and the second from a "task call."

**2. Objects pinned in memory**

```{testcode}
import numpy as np

a = ray.put(np.zeros(1))
b = ray.get(a)
del a
```

This example creates a NumPy array and stores it in the object store. Then it fetches the same NumPy array from the object store and deletes its object ref. The object is still pinned in the object store because the deserialized copy in `b` points directly to the memory in the object store.

```text
--- Summary for node address: 192.168.0.15 ---
Mem Used by Objects  Local References  Pinned Count  Pending Tasks  Captured in Objects  Actor Handles
243 MiB              0                 1             0              0                    0

--- Object references for node address: 192.168.0.15 ---
IP Address    PID    Type    Object Ref                                                Size    Reference Type      Call Site
192.168.0.15  7066   Driver  ffffffffffffffffffffffffffffffffffffffff0100000001000000  243 MiB  PINNED_IN_MEMORY   test.
                                                                                                                  py:<module>:19
```

The `ray memory` output shows the object as `PINNED_IN_MEMORY`. If you run `del b`, the reference can be freed.

**3. Pending task references**

```{testcode}
@ray.remote
def f(arg):
    while True:
        pass

a = ray.put(None)
b = f.remote(a)
```

This example creates an object with `ray.put()` and then submits a task that depends on the object.

```text
--- Summary for node address: 192.168.0.15 ---
Mem Used by Objects  Local References  Pinned Count  Pending Tasks  Captured in Objects  Actor Handles
25 MiB               1                 1             1              0                    0

--- Object references for node address: 192.168.0.15 ---
IP Address    PID    Type    Object Ref                                                Size    Reference Type      Call Site
192.168.0.15  7207   Driver  a67dc375e60ddd1affffffffffffffffffffffff0100000001000000  ?       LOCAL_REFERENCE     (task call)
                                                                                                                    | test.py:
                                                                                                                  :<module>:29

192.168.0.15  7241   Worker  ffffffffffffffffffffffffffffffffffffffff0100000001000000  10 MiB  PINNED_IN_MEMORY    (deserialize task arg)
                                                                                                                    __main__.f

192.168.0.15  7207   Driver  ffffffffffffffffffffffffffffffffffffffff0100000001000000  15 MiB  USED_BY_PENDING_TASK  (put object)  |
                                                                                                                  test.py:
                                                                                                                  <module>:28
```

While the task runs, `ray memory` shows both a `LOCAL_REFERENCE` and a `USED_BY_PENDING_TASK` reference for the object in the driver process. The worker process also holds a reference to the object because the Python `arg` references the memory in the Plasma store directly. The object can't be evicted, so it's `PINNED_IN_MEMORY`.

**4. Serialized object refs**

```{testcode}
@ray.remote
def f(arg):
    while True:
        pass

a = ray.put(None)
b = f.remote([a])
```

This example also creates an object with `ray.put()`, but then passes it to a task wrapped in another object, in this case a list.

```text
--- Summary for node address: 192.168.0.15 ---
Mem Used by Objects  Local References  Pinned Count  Pending Tasks  Captured in Objects  Actor Handles
15 MiB               2                 0             1              0                    0

--- Object references for node address: 192.168.0.15 ---
IP Address    PID    Type    Object Ref                                                Size    Reference Type      Call Site
192.168.0.15  7411   Worker  ffffffffffffffffffffffffffffffffffffffff0100000001000000  ?       LOCAL_REFERENCE     (deserialize task arg)
                                                                                                                    __main__.f

192.168.0.15  7373   Driver  a67dc375e60ddd1affffffffffffffffffffffff0100000001000000  ?       LOCAL_REFERENCE     (task call)
                                                                                                                  | test.py:
                                                                                                                  :<module>:38

192.168.0.15  7373   Driver  ffffffffffffffffffffffffffffffffffffffff0100000001000000  15 MiB  USED_BY_PENDING_TASK  (put object)
                                                                                                                  | test.py:
                                                                                                                  <module>:37
```

Both the driver and the worker process running the task hold a `LOCAL_REFERENCE` to the object, and the object is also `USED_BY_PENDING_TASK` on the driver. If this were an actor task, the actor could hold a `LOCAL_REFERENCE` after the task completes by storing the object ref in a member variable.

**5. Captured object refs**

```{testcode}
a = ray.put(None)
b = ray.put([a])
del a
```

This example creates an object with `ray.put()`, captures its object ref inside another `ray.put()` object, and deletes the first object ref. Both objects are still pinned.

```text
--- Summary for node address: 192.168.0.15 ---
Mem Used by Objects  Local References  Pinned Count  Pending Tasks  Captured in Objects  Actor Handles
233 MiB              1                 0             0              1                    0

--- Object references for node address: 192.168.0.15 ---
IP Address    PID    Type    Object Ref                                                Size    Reference Type      Call Site
192.168.0.15  7473   Driver  ffffffffffffffffffffffffffffffffffffffff0100000001000000  15 MiB  CAPTURED_IN_OBJECT  (put object)  |
                                                                                                                  test.py:
                                                                                                                  <module>:41

192.168.0.15  7473   Driver  ffffffffffffffffffffffffffffffffffffffff0100000002000000  218 MiB  LOCAL_REFERENCE     (put object)  |
                                                                                                                  test.py:
                                                                                                                  <module>:42
```

The `ray memory` output shows the second object as a normal `LOCAL_REFERENCE` and the first object as `CAPTURED_IN_OBJECT`.

(memory-aware-scheduling)=

## Memory-aware scheduling

By default, Ray doesn't take the potential memory usage of a task or actor into account when scheduling, because it can't estimate ahead of time how much memory the task or actor requires. If you know how much memory a task or actor requires, specify it in the resource requirements of its `ray.remote` decorator for memory-aware scheduling.

:::{important}
Specifying a memory requirement doesn't limit memory usage. Ray uses the requirement only for admission control during scheduling, similar to how CPU scheduling works in Ray. Make sure the task doesn't use more memory than it requests.
:::

To tell the Ray scheduler a task or actor requires a certain amount of available memory to run, set the `memory` argument. The Ray scheduler then reserves the specified amount of available memory during scheduling, similar to how it handles CPU and GPU resources:

```{testcode}
# reserve 500MiB of available memory to place this task
@ray.remote(memory=500 * 1024 * 1024)
def some_function(x):
    pass

# reserve 2.5GiB of available memory to place this actor
@ray.remote(memory=2500 * 1024 * 1024)
class SomeActor:
    def __init__(self, a, b):
        pass
```

The preceding example sets the memory quota statically in the decorator. To set it dynamically at runtime, use `.options()`:

```{testcode}
# override the memory quota to 100MiB when submitting the task
some_function.options(memory=100 * 1024 * 1024).remote(x=1)

# override the memory quota to 1GiB when creating the actor
SomeActor.options(memory=1000 * 1024 * 1024).remote(a=1, b=2)
```

### Questions or issues?

```{include} /_includes/_help-links.md
```
