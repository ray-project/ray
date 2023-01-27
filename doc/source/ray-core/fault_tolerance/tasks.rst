.. _fault-tolerance-tasks:
.. _task-fault-tolerance:

Task Fault Tolerance
====================

Tasks can fail due to application-level errors, e.g., Python-level exceptions,
or system-level failures, e.g., a machine fails. Here, we describe the
mechanisms that an application developer can use to recover from these errors.

Catching application-level failures
-----------------------------------

Ray surfaces application-level failures as Python-level exceptions. When a task
on a remote worker or actor fails due to a Python-level exception, Ray wraps
the original exception in a ``RayTaskError`` and stores this as the task's
return value. This wrapped exception will be thrown to any worker that tries
to get the result, either by calling ``ray.get`` or if the worker is executing
another task that depends on the object.

.. literalinclude:: ../doc_code/task_exceptions.py
  :language: python
  :start-after: __task_exceptions_begin__
  :end-before: __task_exceptions_end__

.. _task-retries:

Retrying failed tasks
---------------------

When a worker is executing a task, if the worker dies unexpectedly, either
because the process crashed or because the machine failed, Ray will rerun
the task until either the task succeeds or the maximum number of retries is
exceeded. The default number of retries is 3 and can be overridden by
specifying ``max_retries`` in the ``@ray.remote`` decorator. Specifying -1
allows infinite retries, and 0 disables retries. To override the default number
of retries for all tasks submitted, set the OS environment variable
``RAY_TASK_MAX_RETRIES``. e.g., by passing this to your driver script or by
using :ref:`runtime environments<runtime-environments>`.

You can experiment with this behavior by running the following code.

.. literalinclude:: ../doc_code/tasks_fault_tolerance.py
  :language: python
  :start-after: __tasks_fault_tolerance_retries_begin__
  :end-before: __tasks_fault_tolerance_retries_end__

When a task returns a result in the Ray object store, it is possible for the
resulting object to be lost **after** the original task has already finished.
In these cases, Ray will also try to automatically recover the object by
re-executing the tasks that created the object. This can be configured through
the same ``max_retries`` option described here. See :ref:`object fault
tolerance <fault-tolerance-objects>` for more information.

By default, Ray will **not** retry tasks upon exceptions thrown by application
code. However, you may control whether application-level errors are retried,
and even **which** application-level errors are retried, via the
``retry_exceptions`` argument. This is ``False`` by default. To enable retries
upon application-level errors, set ``retry_exceptions=True`` to retry upon any
exception, or pass a list of retryable exceptions. An example is shown below.

.. literalinclude:: ../doc_code/tasks_fault_tolerance.py
  :language: python
  :start-after: __tasks_fault_tolerance_retries_exception_begin__
  :end-before: __tasks_fault_tolerance_retries_exception_end__

Cancelling misbehaving tasks
----------------------------

If a task is hanging, you may want to cancel the task to continue to make
progress. You can do this by calling ``ray.cancel`` on an ``ObjectRef``
returned by the task. By default, this will send a KeyboardInterrupt to the
task's worker if it is mid-execution.  Passing ``force=True`` to ``ray.cancel``
will force-exit the worker. See :ref:`the API reference <ray-cancel-ref>` for
``ray.cancel`` for more details.

Note that currently, Ray will not automatically retry tasks that have been
cancelled.

Sometimes, application-level code may cause memory leaks on a worker after
repeated task executions, e.g., due to bugs in third-party libraries.
To make progress in these cases, you can set the ``max_calls`` option in a
task's ``@ray.remote`` decorator. Once a worker has executed this many
invocations of the given remote function, it will automatically exit. By
default, ``max_calls`` is set to infinity.
