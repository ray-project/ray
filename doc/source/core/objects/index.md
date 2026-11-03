---
myst:
  html_meta:
    description: "Ray remote objects and object refs: fetching data, passing objects as arguments, closure capture, nested objects, and fault tolerance."
---

(objects-in-ray)=

# Objects

Ray tasks and actors create and compute on objects. Ray calls these objects *remote objects* because they can live anywhere in a Ray cluster, and you refer to them with *object refs*. Ray caches remote objects in its distributed [shared-memory](https://en.wikipedia.org/wiki/Shared_memory) *object store*, with one object store per node in the cluster. In a cluster, a remote object can live on one or many nodes, regardless of who holds its object refs.

An object ref acts as a pointer or a unique ID that you use to refer to a remote object without seeing its value. Object refs are conceptually similar to futures.

You get object refs in two ways:

1. A remote function call returns them.
1. {func}`ray.put() <ray.put>` returns them.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray

# Put an object in Ray's object store.
y = 1
object_ref = ray.put(y)
```
:::

:::{tab-item} Java
```java
// Put an object in Ray's object store.
int y = 1;
ObjectRef<Integer> objectRef = Ray.put(y);
```
:::

:::{tab-item} C++
```c++
// Put an object in Ray's object store.
int y = 1;
ray::ObjectRef<int> object_ref = ray::Put(y);
```
:::
::::

:::{note}
Remote objects are immutable, so you can't change their values after creation. Because of this, Ray can replicate remote objects in multiple object stores without synchronizing the copies.
:::


## Fetching object data

Call the {func}`ray.get() <ray.get>` method to fetch the result of a remote object from an object ref. If the current node's object store doesn't contain the object, Ray downloads it.

::::{tab-set}
:::{tab-item} Python
If the object is a [NumPy array](https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html) or a collection of NumPy arrays, the `get` call is zero-copy and returns arrays backed by shared object store memory. Otherwise, Ray deserializes the object data into a Python object.

```{testcode}
import ray
import time

# Get the value of one object ref.
obj_ref = ray.put(1)
assert ray.get(obj_ref) == 1

# Get the values of multiple object refs in parallel.
assert ray.get([ray.put(i) for i in range(3)]) == [0, 1, 2]

# You can also set a timeout to return early from a ``get``
# that's blocking for too long.
from ray.exceptions import GetTimeoutError
# ``GetTimeoutError`` is a subclass of ``TimeoutError``.

@ray.remote
def long_running_function():
    time.sleep(8)

obj_ref = long_running_function.remote()
try:
    ray.get(obj_ref, timeout=4)
except GetTimeoutError:  # You can capture the standard "TimeoutError" instead
    print("`get` timed out.")
```

```{testoutput}
`get` timed out.
```
:::

:::{tab-item} Java
```java
// Get the value of one object ref.
ObjectRef<Integer> objRef = Ray.put(1);
Assert.assertTrue(objRef.get() == 1);
// You can also set a timeout(ms) to return early from a ``get`` that's blocking for too long.
Assert.assertTrue(objRef.get(1000) == 1);

// Get the values of multiple object refs in parallel.
List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
for (int i = 0; i < 3; i++) {
  objectRefs.add(Ray.put(i));
}
List<Integer> results = Ray.get(objectRefs);
Assert.assertEquals(results, ImmutableList.of(0, 1, 2));

// Ray.get timeout example: Ray.get will throw an RayTimeoutException if time out.
public class MyRayApp {
  public static int slowFunction() throws InterruptedException {
    TimeUnit.SECONDS.sleep(10);
    return 1;
  }
}
Assert.assertThrows(RayTimeoutException.class,
  () -> Ray.get(Ray.task(MyRayApp::slowFunction).remote(), 3000));
```
:::

:::{tab-item} C++
```c++
// Get the value of one object ref.
ray::ObjectRef<int> obj_ref = ray::Put(1);
assert(*obj_ref.Get() == 1);

// Get the values of multiple object refs in parallel.
std::vector<ray::ObjectRef<int>> obj_refs;
for (int i = 0; i < 3; i++) {
  obj_refs.emplace_back(ray::Put(i));
}
auto results = ray::Get(obj_refs);
assert(results.size() == 3);
assert(*results[0] == 0);
assert(*results[1] == 1);
assert(*results[2] == 2);
```
:::
::::

## Passing object arguments

You can pass object refs freely around a Ray application. Pass them as arguments to tasks and actor methods, or even store them in other objects. Ray tracks objects through *distributed reference counting* and automatically frees an object's data once all references to the object are deleted.

You can pass an object to a task or actor method in two ways. Depending on how you pass the object, Ray decides whether to *de-reference* it before the task runs.

**Passing an object as a top-level argument**: When you pass an object directly as a top-level argument to a task, Ray de-references the object. Ray fetches the underlying data for all top-level object ref arguments and doesn't run the task until the object data is fully available.

```{literalinclude} ../doc_code/obj_val.py
```

**Passing an object as a nested argument**: When you pass an object within a nested object, such as a Python list, Ray doesn't de-reference it. The task needs to call `ray.get()` on the reference to fetch the concrete value. If the task never calls `ray.get()`, Ray never needs to transfer the object value to the machine the task runs on. Pass objects as top-level arguments where possible. Nested arguments can be useful for passing objects on to other tasks without seeing the data.

```{literalinclude} ../doc_code/obj_ref.py
```

The same top-level and nested passing convention applies to actor constructors and actor method calls:

```{testcode}
@ray.remote
class Actor:
  def __init__(self, arg):
    pass

  def method(self, arg):
    pass

obj = ray.put(2)

# Examples of passing objects to actor constructors.
actor_handle = Actor.remote(obj)  # by-value
actor_handle = Actor.remote([obj])  # by-reference

# Examples of passing objects to actor method calls.
actor_handle.method.remote(obj)  # by-value
actor_handle.method.remote([obj])  # by-reference
```

## Closure capture of objects

You can also pass objects to tasks through *closure capture*. Closure capture can be convenient when you want to share a large object verbatim between many tasks or actors without passing it repeatedly as an argument.

:::{caution}
Defining a task that closes over an object ref pins the object through reference counting, so Ray doesn't evict the object until the job completes.
:::

```{literalinclude} ../doc_code/obj_capture.py
```

## Nested objects

Ray also supports nested object refs, so you can build composite objects that hold references to further sub-objects.

```{testcode}
# Objects can be nested within each other. Ray will keep the inner object
# alive via reference counting until all outer object references are deleted.
object_ref_2 = ray.put([object_ref])
```

## Fault tolerance

Ray can automatically recover from object data loss through {ref}`lineage reconstruction <fault-tolerance-objects-reconstruction>`, but not from {ref}`owner <fault-tolerance-ownership>` failure. See {ref}`Ray fault tolerance <fault-tolerance>` for more details.

## More about Ray objects

```{toctree}
:maxdepth: 1

serialization
object-spilling
```
