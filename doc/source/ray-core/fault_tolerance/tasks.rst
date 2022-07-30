Task fault tolerance
====================

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