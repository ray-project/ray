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
using :ref:`runtime environments<runtime-environments>`.

You can experiment with this behavior by running the following code.

.. code-block:: python

    import numpy as np
    import os
    import ray
    import time

    ray.init(ignore_reinit_error=True)

    @ray.remote(max_retries=1)
    def potentially_fail(failure_probability):
        time.sleep(0.2)
        if np.random.random() < failure_probability:
            os._exit(0)
        return 0

    for _ in range(3):
        try:
            # If this task crashes, Ray will retry it up to one additional
            # time. If either of the attempts succeeds, the call to ray.get
            # below will return normally. Otherwise, it will raise an
            # exception.
            ray.get(potentially_fail.remote(0.5))
            print('SUCCESS')
        except ray.exceptions.WorkerCrashedError:
            print('FAILURE')

You can also control whether application-level errors are retried, and even **which**
application-level errors are retried, via the ``retry_exceptions`` argument. This is
``False`` by default, so if your application code within the Ray task raises an
exception, this failure will **not** be retried. This is to ensure that Ray is not
retrying non-idempotent tasks when they have partially executed.
However, if your tasks are idempotent, then you can enable application-level error
retries with ``retry_exceptions=True``, or even retry a specific set of
application-level errors (such as a class of exception types that you know to be
transient) by providing an allowlist of exceptions, or an exception predicate that
returns ``True`` when an exception should be retried:

.. code-block:: python

    import numpy as np
    import os
    import ray
    import time

    ray.init(ignore_reinit_error=True)

    class RandomError(Exception):
        pass

    @ray.remote(max_retries=1, retry_exceptions=True)
    def potentially_fail(failure_probability):
        if failure_probability < 0 or failure_probability > 1:
            raise ValueError(
                "failure_probability must be between 0 and 1, but got: "
                f"{failure_probability}"
            )
        time.sleep(0.2)
        if np.random.random() < failure_probability:
            raise RandomError("Failed!")
        return 0

    for _ in range(3):
        try:
            # If this task crashes, Ray will retry it up to one additional
            # time. If either of the attempts succeeds, the call to ray.get
            # below will return normally. Otherwise, it will raise an
            # exception.
            ray.get(potentially_fail.remote(0.5))
            print('SUCCESS')
        except RandomError:
            print('FAILURE')

    # Provide the exceptions that we want to retry. This can be a list of exceptions
    # or a function that takes an exception and returns whether we should retry on
    # that exception.
    retry_on_exception = potentially_fail.options(retry_exceptions=[RandomError])
    try:
        # This will fail since we're passing in -1 for the failure_probability,
        # which will raise a ValueError in the task and does not match the RandomError
        # exception that we provided.
        ray.get(retry_on_exception.remote(-1))
    except ValueError:
        print("FAILED AS EXPECTED")
    else:
        raise RuntimeError("An exception should be raised so this shouldn't be reached.")

    # These will retry on the RandomError exception.
    for _ in range(3):
        try:
            # If this task crashes, Ray will retry it up to one additional
            # time. If either of the attempts succeeds, the call to ray.get
            # below will return normally. Otherwise, it will raise an
            # exception.
            ray.get(retry_on_exception.remote(0.5))
            print('SUCCESS')
        except RandomError:
            print('FAILURE AFTER RETRIES')

The semantics for each of the potential ``retry_exceptions`` values are as follows:

* ``retry_exceptions=False`` (default): Application-level errors are not retried.

* ``retry_exceptions=True``: All application-level errors are retried.

* ``retry_exceptions=[Exc1, Exc2]``: Application-level errors that are instances of
  either ``Exc1`` or ``Exc2`` are retried.

* ``retry_exceptions=lambda e: isinstance(e, SomeException)``: Application-level errors
  for which ``predicate_fn(e)`` returns ``True`` are retried.

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
