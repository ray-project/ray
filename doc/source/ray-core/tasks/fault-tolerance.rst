.. _task-fault-tolerance:

Fault Tolerance
===============

When a worker is executing a task, if the worker dies unexpectedly, either
because the process crashed or because the machine failed, Ray will rerun
the task until either the task succeeds or the maximum number of retries is
exceeded. The default number of retries is 3 and can be overridden by
specifying ``max_retries`` in the ``@ray.remote`` decorator. Specifying -1
allows infinite retries, and 0 disables retries.

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

.. _object-reconstruction:

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
