.. _task-fault-tolerance:

===============
Fault Tolerance
===============

.. _task-retries:

Retries
=======

When a worker is executing a task, if the worker dies unexpectedly, either
because the process crashed or because the machine failed, Ray will rerun
the task until either the task succeeds or the maximum number of retries is
exceeded. The default number of retries is 3 and can be overridden by
specifying ``max_retries`` in the ``@ray.remote`` decorator. Specifying -1
allows infinite retries, and 0 disables retries. To override the default number
of retries for all tasks submitted, set the OS environment variable
``RAY_TASK_MAX_RETRIES``. e.g., by passing this to your driver script or by
using :ref:`runtime environments <runtime-environments>`.

You can experiment with this behavior by running the following code.

.. literalinclude:: ../doc_code/tasks_fault_tolerance.py
  :language: python
  :start-after: __tasks_fault_tolerance_retries_begin__
  :end-before: __tasks_fault_tolerance_retries_end__

You can also control whether application-level errors are retried, and even **which**
application-level errors are retried, via the ``retry_exceptions`` argument. This is
``False`` by default, so if your application code within the Ray task raises an
exception, this failure will **not** be retried. This is to ensure that Ray is not
retrying non-idempotent tasks when they have partially executed.
However, if your tasks are idempotent, then you can enable application-level error
retries with ``retry_exceptions=True``, or even retry a specific set of
application-level errors (such as a class of exception types that you know to be
transient) by providing an allowlist of exceptions:

.. literalinclude:: ../doc_code/tasks_fault_tolerance.py
  :language: python
  :start-after: __tasks_fault_tolerance_retries_exception_begin__
  :end-before: __tasks_fault_tolerance_retries_exception_end__

The semantics for each of the potential ``retry_exceptions`` values are as follows:

* ``retry_exceptions=False`` (default): Application-level errors are not retried.

* ``retry_exceptions=True``: All application-level errors are retried.

* ``retry_exceptions=[Exc1, Exc2]``: Application-level errors that are instances of
  either ``Exc1`` or ``Exc2`` are retried.

.. _object-reconstruction:

Lineage-based Object Reconstruction
===================================

Ray also implements *lineage reconstruction* to recover task outputs that are
lost from the distributed object store. This can occur during node failures.
Ray will first automatically attempt to recover the value by looking for copies
of the same object on other nodes. If none are found, then Ray will
automatically recover the value by re-executing the task that created the
value. Arguments to the task are recursively reconstructed with the same
method.

Note that lineage reconstruction can cause higher than usual driver memory
usage because the driver keeps the descriptions of any tasks that may be
re-executed in case of a failure. To limit the amount of memory used by
lineage, set the environment variable ``RAY_max_lineage_bytes`` (default 1GB)
to evict lineage if the threshold is exceeded.

To disable this behavior, set the environment variable
``RAY_lineage_pinning_enabled=0`` during ``ray start`` or ``ray.init``.  With
this setting, if there are no copies of an object left, an ``ObjectLostError``
will be raised.
