# Aliasing

An important feature of Halo is that a remote call sent to the scheduler
immediately returns object references to the outputs of the task, and the actual
outputs of the task are only associated with the relevant object references
after the task has been executed and the outputs have been computed. This allows
the worker to continue without blocking.

However, to provide a more flexible API, we allow tasks to not only return
values, but to also return object references to values. As an examples, consider
the following code.
```python
@halo.distributed([], [np.ndarray])
def f()
  return np.zeros(5)

@halo.distributed([], [np.ndarray])
def g()
  return f()

@halo.distributed([], [np.ndarray])
def h()
  return g()
```
A call to `h` will immediate return an object reference `ref_h` for the return
value of `h`. The task of executing `h` (call it `task_h`) will then be
scheduled for execution. When `task_h` is executed, it will call `g`, which will
immediately return an object reference `ref_g` for the output of `g`. Then two
things will happen and can happen in any order: `AliasObjRefs(ref_h, ref_g)`
will be called and `task_g` will be scheduled and executed. When `task_g` is
executed, it will call `f`, and immediately obtain an object reference `ref_f`
for the output of `f`. Then two things will happen and can happen in either
order, `AliasObjRefs(ref_g, ref_f)` will be called, and `f` will be executed.
When `f` is executed, it will create an actual array and `put_object` will be
called, which will store the array in the object store (it will also call
`SchedulerService::AddCanonicalObjRef(ref_f)`).

From the scheduler's perspective, there are three important calls,
`AliasObjRefs(ref_h, ref_g)`, `AliasObjRefs(ref_g, ref_f)`, and
`AddCanonicalObjRef(ref_f)`. These three calls can happen in any order.

The scheduler maintains a data structure called `target_objrefs_`, which keeps
track of which object references have been aliased together (`target_objrefs_`
is a vector, but we can think of it as a graph). The call
`AliasObjRefs(ref_h, ref_g)` updates `target_objrefs_` with `ref_h -> ref_g`.
The call `AliasObjRefs(ref_g, ref_f)` updates it with `ref_g -> ref_f`, and the
call `AddCanonicalObjRef(ref_f)` updates it with `ref_f -> ref_f`. The data
structure is initialized with `ref -> UNINITIALIZED_ALIAS` for each object
reference `ref`.

We refer to `ref_f` as a "canonical" object reference. And in a pair such as
`ref_h -> ref_g`, we refer to `ref_h` as the "alias" object reference and to
`ref_g` as the "target" object reference. These details are available to the
scheduler, but a worker process just has an object reference and doesn't know if
it is canonical or not.

We also maintain a data structure `reverse_target_objrefs_`, which maps in the
reverse direction (in the above example, we would have `ref_g -> ref_h`,
`ref_f -> ref_g`, and `ref_h -> UNINITIALIZED_ALIAS`). This data structure is
not particuarly important for the task of aliasing, but when we do reference
counting and attempt to deallocate an object, we need to be able to determine
all of the object references that refer to the same object, and this data
structure comes in handy for that purpose.

## Pulls and Remote Calls

When a worker calls `pull(ref)`, it first sends a message to the scheduler
asking the scheduler to ship the object referred to by `ref` to the worker's
local object store. Then the worker asks its local object store for the object
referred to by `ref`. If `ref` is a canonical object reference, then that's all
there is too it. However, if `ref` is not a canonical object reference but
rather is an alias for the canonical object reference `c_ref`, then the
scheduler also notifies the worker's local object store that `ref` is an
alias for `c_ref`. This is important because the object store does not keep
track of aliasing on its own (it only knows the bits about aliasing that the
scheduler tells it). Lastly, if the scheduler does not yet have enough
information to determine if `ref` is canonical, or if the scheduler cannot
yet determine what the canonical object reference for `ref` is, then the
scheduler will wait until it has the relevant information.

Similar things happen when a worker performs a remote call. If an object
reference is passed to a remote call, the object referred to by that object
reference will be shipped to the local object store of the worker that executes
the task. The scheduler will notify that object store about any aliasing that it
needs to be aware of.

## Passing Object References by Value
Currently, the graph of aliasing looks like a collection of chains, as in the
above example with `ref_h -> ref_g -> ref_f -> ref_f`. In the future, we will
allow object references to be passed by value to remote calls (so the worker
has access to the object reference object and not the object that the object
reference refers to). If an object reference that is passed by value is then
returned by the task, it is possible that a given object reference could be
the target of multiple alias object references. In this case, the graph of
aliasing will be a tree.
