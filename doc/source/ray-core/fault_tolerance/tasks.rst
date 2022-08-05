.. _fault-tolerance-tasks:

Task fault tolerance
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

.. code-block:: python

    import ray

    @ray.remote
    def f():
        raise Exception("the real error")

    @ray.remote
    def g(x):
        return


    try:
        ray.get(f.remote())
    except ray.exceptions.RayTaskError as e:
        print(e)
        # ray::f() (pid=71867, ip=XXX.XX.XXX.XX)
        #   File "errors.py", line 5, in f
        #     raise Exception("the real error")
        # Exception: the real error

    try:
        ray.get(g.remote(f.remote()))
    except ray.exceptions.RayTaskError as e:
        print(e)
        # ray::g() (pid=73085, ip=128.32.132.47)
        #   At least one of the input arguments for this task could not be computed:
        # ray.exceptions.RayTaskError: ray::f() (pid=73085, ip=XXX.XX.XXX.XX)
        #   File "errors.py", line 5, in f
        #     raise Exception("the real error")
        # Exception: the real error


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

.. code-block:: python

    import numpy as np
    import os
    import ray
    import time

    ray.init()

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


By default, Ray will not retry any failure in the application layer, for example,
the function itself throws some exceptions, unless ``retry_exceptions`` is set:

.. code-block:: python

    import ray

    ray.init()

    @ray.remote(max_retries=1)
    def raise_exception(tag):
        print(tag, "potentially_fail")
        raise Exception()

    try:
        # it can also be set with the @ray.remote decorator
        # Here the raise_exception will run 2 times
        ray.get(raise_exception.options(retry_exceptions=True).remote(
            "retry_exceptions=True"))
    except Exception:
        pass

    try:
        # raise_exception will run 1 time only.
        ray.get(raise_exception.remote("retry_exceptions=False"))
    except Exception:
        pass        


In the code above, once ``retry_exceptions`` is set to be ``True``, Ray will
also retry the failures caused by the user's code. In the end, if it still
fails, the error will be stored into the object ref and raised when ``ray.get``
is called.

When a task returns a result in the Ray object store, it is possible for the
resulting object to be lost **after** the original task has already finished.
In these cases, Ray will also try to automatically recover the object by
re-executing the tasks that created the object. This can be configured through
the same ``max_retries`` option described here. See :ref:`object fault
tolerance <fault-tolerance-objects>` for more information.

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
