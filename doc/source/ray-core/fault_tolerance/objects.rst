.. _fault-tolerance-objects:
.. _object-fault-tolerance:

Object Fault Tolerance
======================

A Ray object has both data (the value returned when calling ``ray.get``) and
metadata (e.g., the location of the value). Data is stored in the Ray object
store while the metadata is stored at the object's **owner**. The owner of an
object is the worker process that creates the original ``ObjectRef``, e.g., by
calling ``f.remote()`` or ``ray.put()``. Note that this worker is usually a
distinct process from the worker that creates the **value** of the object,
except in cases of ``ray.put``.

.. literalinclude:: ../doc_code/owners.py
  :language: python
  :start-after: __owners_begin__
  :end-before: __owners_end__


Ray can automatically recover from data loss but not owner failure.

.. _fault-tolerance-objects-reconstruction:

Recovering from data loss
-------------------------

When an object value is lost from the object store, such as during node
failures, Ray will use *lineage reconstruction* to recover the object.
Ray will first automatically attempt to recover the value by looking
for copies of the same object on other nodes. If none are found, then Ray will
automatically recover the value by :ref:`re-executing <fault-tolerance-tasks>`
the task that previously created the value.  Arguments to the task are
recursively reconstructed through the same mechanism.

Lineage reconstruction currently has the following limitations:

* The object, and any of its transitive dependencies, must have been generated
  by a task (actor or non-actor). This means that **objects created by
  ray.put are not recoverable**.
* Tasks are assumed to be deterministic and idempotent. Thus,
  **by default, objects created by actor tasks are not reconstructable**. To allow
  reconstruction of actor task results, set the ``max_task_retries`` parameter
  to a non-zero value (see :ref:`actor
  fault tolerance <fault-tolerance-actors>` for more details).
* Tasks will only be re-executed up to their maximum number of retries. By
  default, a non-actor task can be retried up to 3 times and an actor task
  cannot be retried.  This can be overridden with the ``max_retries`` parameter
  for :ref:`remote functions <fault-tolerance-tasks>` and the
  ``max_task_retries`` parameter for :ref:`actors <fault-tolerance-actors>`.
* The owner of the object must still be alive (see :ref:`below
  <fault-tolerance-ownership>`).

Lineage reconstruction can cause higher than usual driver memory
usage because the driver keeps the descriptions of any tasks that may be
re-executed in case of failure. To limit the amount of memory used by
lineage, set the environment variable ``RAY_max_lineage_bytes`` (default 1GB)
to evict lineage if the threshold is exceeded.

To disable lineage reconstruction entirely, set the environment variable
``RAY_TASK_MAX_RETRIES=0`` during ``ray start`` or ``ray.init``.  With this
setting, if there are no copies of an object left, an ``ObjectLostError`` will
be raised.

.. _fault-tolerance-ownership:

Recovering from owner failure
-----------------------------

The owner of an object can die because of node or worker process failure.
Currently, **Ray does not support recovery from owner failure**. In this case, Ray
will clean up any remaining copies of the object's value to prevent a memory
leak. Any workers that subsequently try to get the object's value will receive
an ``OwnerDiedError`` exception, which can be handled manually.

Understanding `ObjectLostErrors`
--------------------------------

Ray throws an ``ObjectLostError`` to the application when an object cannot be
retrieved due to application or system error. This can occur during a
``ray.get()`` call or when fetching a task's arguments, and can happen for a
number of reasons. Here is a guide to understanding the root cause for
different error types:

- ``OwnerDiedError``: The owner of an object, i.e., the Python worker that
  first created the ``ObjectRef`` via ``.remote()`` or ``ray.put()``, has died.
  The owner stores critical object metadata and an object cannot be retrieved
  if this process is lost.
- ``ObjectReconstructionFailedError``: This error is thrown if an object, or
  another object that this object depends on, cannot be reconstructed due to
  one of the limitations described :ref:`above
  <fault-tolerance-objects-reconstruction>`.
- ``ReferenceCountingAssertionError``: The object has already been deleted,
  so it cannot be retrieved. Ray implements automatic memory management through
  distributed reference counting, so this error should not happen in general.
  However, there is a `known edge case <https://github.com/ray-project/ray/issues/18456>`_ that can produce this error.
- ``ObjectFetchTimedOutError``: A node timed out while trying to retrieve a
  copy of the object from a remote node. This error usually indicates a
  system-level bug. The timeout period can be configured using the
  ``RAY_fetch_fail_timeout_milliseconds`` environment variable (default 10
  minutes).
- ``ObjectLostError``: The object was successfully created, but no copy is
  reachable.  This is a generic error thrown when lineage reconstruction is
  disabled and all copies of the object are lost from the cluster.
