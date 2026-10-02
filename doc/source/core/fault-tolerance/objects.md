---
myst:
  html_meta:
    description: "Object fault tolerance in Ray: lineage-based recovery from data loss, recovery from owner failure, and understanding ObjectLostError."
---

(fault-tolerance-objects)=
(object-fault-tolerance)=

# Object fault tolerance

A Ray object has both data and metadata. The data is the value that a call to `ray.get` returns, and the metadata includes details such as the location of the value. Ray stores the data in the Ray object store and stores the metadata at the object's *owner*. The owner of an object is the worker process that creates the original object ref, for example by calling `f.remote()` or `ray.put()`. This worker is usually a distinct process from the worker that creates the *value* of the object, except in cases of `ray.put`.

```{literalinclude} ../doc_code/owners.py
:language: python
:start-after: __owners_begin__
:end-before: __owners_end__
```


Ray can automatically recover from data loss but not from owner failure.

(fault-tolerance-objects-reconstruction)=

## Recovering from data loss

When an object value is lost from the object store, such as during node failures, Ray uses *lineage reconstruction* to recover the object. Ray first automatically attempts to recover the value by looking for copies of the same object on other nodes. If it finds none, Ray automatically recovers the value by {ref}`re-executing <fault-tolerance-tasks>` the task that previously created the value. Ray recursively reconstructs the task's arguments through the same mechanism.

Lineage reconstruction currently has the following limitations:

* A task, either an actor task or a non-actor task, must have generated the object and any of its transitive dependencies. As a result, objects that `ray.put` creates aren't recoverable.
* Lineage reconstruction assumes that tasks are deterministic and idempotent. As a result, objects that actor tasks create aren't reconstructable by default. To make actor task results reconstructable, set the `max_task_retries` parameter to a nonzero value. For more details, see {ref}`actor fault tolerance <fault-tolerance-actors>`.
* Ray re-executes a task only up to its maximum number of retries. By default, Ray can retry a non-actor task up to three times and can't retry an actor task. To override these defaults, use the `max_retries` parameter for {ref}`remote functions <fault-tolerance-tasks>` and the `max_task_retries` parameter for {ref}`actors <fault-tolerance-actors>`.
* The owner of the object must still be alive. See {ref}`Recovering from owner failure <fault-tolerance-ownership>`.

Lineage reconstruction can cause higher-than-usual driver memory usage because the driver keeps the descriptions of any tasks that might be re-executed in case of failure. To limit the memory that lineage uses, set the `RAY_max_lineage_bytes` environment variable, which defaults to 1 GiB. Ray evicts lineage when it exceeds this threshold.

To disable lineage reconstruction entirely, set the environment variable `RAY_TASK_MAX_RETRIES=0` when you run `ray start` or call `ray.init`. With this setting, Ray raises an `ObjectLostError` if no copies of an object remain.

(fault-tolerance-ownership)=

## Recovering from owner failure

The owner of an object can die because of node or worker process failure. Ray doesn't currently support recovery from owner failure. When the owner dies, Ray cleans up any remaining copies of the object's value to prevent a memory leak. Any worker that later tries to get the object's value receives an `OwnerDiedError` exception, which you can handle manually.

## Understanding `ObjectLostErrors`

Ray throws an `ObjectLostError` to the application when an object can't be retrieved because of an application or system error. This error can occur during a `ray.get()` call or when a task's arguments are fetched, and it has several possible causes. To understand the root cause, check the error type:

- `OwnerDiedError`: The owner of the object has died. The owner is the Python worker that first created the object ref by calling `.remote()` or `ray.put()`. The owner stores critical object metadata, so an object can't be retrieved if this process is lost.
- `ObjectReconstructionFailedError`: Ray throws this error if it can't reconstruct the object or another object that this object depends on, because of one of the limitations in {ref}`Recovering from data loss <fault-tolerance-objects-reconstruction>`.
- `ReferenceCountingAssertionError`: The object has already been deleted, so it can't be retrieved. Ray implements automatic memory management through distributed reference counting, so this error shouldn't happen in general. However, a [known edge case](https://github.com/ray-project/ray/issues/18456) can produce this error.
- `ObjectFetchTimedOutError`: A node timed out while trying to retrieve a copy of the object from a remote node. This error usually indicates a system-level bug. To configure the timeout period, set the `RAY_fetch_fail_timeout_milliseconds` environment variable, which defaults to 10 minutes.
- `ObjectLostError`: The object was successfully created, but no copy is reachable. Ray throws this generic error when lineage reconstruction is disabled and all copies of the object are lost from the cluster.
