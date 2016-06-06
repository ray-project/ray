# Reference Counting

In Halo, each object is assigned a globally unique object reference by the
scheduler (starting with 0 and incrementing upward). The objects are stored in
object stores. In order to avoid running out of memory, the object stores must
know when it is ok to deallocate an object. Since a worker on one node may have
an object reference for an object that lives in an object store on a different
node, knowing when we can safely deallocate an object requires cluster-wide
information.

## Reference Counting

Two approaches to reclaiming memory are garbage collection and reference
counting. We choose to use a reference counting approach in Halo. There are a
couple of reasons for this. Reference counting allows us to reclaim memory as
early as possible. It also avoids pausing the system for garbage collection. We
also note that implementing reference counting at the cluster level plays nicely
with worker processes that use reference counting internally (currently our
worker processes are Python processes). However, this could be made to work with
worker processes that use garbage collection, for example, if each worker
process is a Java Virtual Machine.

At a high level, the scheduler keeps track of the number of object references
that exist on the cluster for each object reference. When the number of object
references reaches 0 for a particular object, the scheduler notifies all of the
object stores that contain that object to deallocate it.

Object references can exist in several places.

1. They can be Python objects on a worker.
2. They can be serialized within an object in an object store.
3. They can be in a message being sent between processes (e.g., as an argument
to a remote procedure call).

## When to Increment and Decrement the Reference Count

We handle these three cases by calling the SchedulerService methods
`IncrementRefCount` and `DecrementRefCount` as follows:

1. To handle the first case, we increment in the ObjRef constructor and
decrement in the ObjRef destructor.
2. To handle the second case, when an object is written to an object store with
a call to `put_object`, we call `IncrementRefCount` for each object reference
that is contained internally in the serialized object (for example, if we
serialize a `DistArray`, we increment the reference counts for its blocks). This
will notify the scheduler that those object references are in the object store.
Then when the scheduler deallocates the object, we call `DecrementRefCount` for
the object references that it holds internally (the scheduler keeps track of
these internal object references in the `contained_objrefs_` data structure).
3. To handle the third case, we increment in the `serialize_task` method and
decrement in the `deserialize_task` method.

## How to Handle Aliasing
Reference counting interacts with aliasing. Since multiple object references
may refer to the same object, we cannot deallocate that object until all of the
object references that refer to it have reference counts of 0. We keep track of
the number of separate aliases separately. If two object references refer to the
same object, the scheduler keeps track the number of occurrences of each of
those object references separately. This simplifies the scheduler's job because
it may not always know if two object references refer to the same object or not
(since it assigns them before hearing back about what they refer to).

When we decrement the count for an object reference, if the count reaches 0,
we compute all object references that the scheduler knows to reference the same
object. If these object references all have count 0, then we deallocate the
object. Otherwise, we do not deallocate the object.

You may ask, what if there is some object reference with a nonzero count which
refers to the same object, but the scheduler does not know it? This cannot
happen because the following invariant holds. If `a` and `b` are object
references that will be aliased together (through a call to
`AliasObjRefs(a, b)`), then either the call has already happened, or both `a`
and `b` have positive reference counts (they must have positive reference counts
because they must be passed into `AliasObjRefs` at some point).

## Complications
The following problem has not yet been resolved. In the following code, the
result `x` will be garbage.
```python
x = halo.pull(ra.zeros([10, 10], "float"))
```
When `ra.zeros` is called, a worker will create an array of zeros and store
it in an object store. An object reference to the output is returned. The call
to `halo.pull` will not copy data from the object store process to the worker
process, but will instead give the worker process a pointer to shared memory.
After the `halo.pull` call completes, the object reference returned by
`ra.zeros` will go out of scope, and the object it refers to will be
deallocated from the object store. This will cause the memory that `x` points to
to be garbage.

This problem is currently unresolved.
