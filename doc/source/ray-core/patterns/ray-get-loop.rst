.. _ray-get-loop:

Anti-pattern: Calling ray.get in a loop harms parallelism
=========================================================

**TLDR:** Avoid calling :ref:`ray.get() <ray-get-ref>` in a loop since it's a blocking call; use ``ray.get()`` only for the final result.

A call to ``ray.get()`` fetches the results of remotely executed functions. However, it is a blocking call, which means that it always waits until the requested result is available.
If you call ``ray.get()`` in a loop, the loop will not continue to run until the call to ``ray.get()`` is resolved.

If you also spawn the remote function calls in the same loop, you end up with no parallelism at all, as you wait for the previous function call to finish (because of ``ray.get()``) and only spawn the next call in the next iteration of the loop.
The solution here is to separate the call to ``ray.get()`` from the call to the remote functions. That way all remote functions are spawned before we wait for the results and can run in parallel in the background. Additionally, you can pass a list of object references to ``ray.get()`` instead of calling it one by one to wait for all of the tasks to finish.

Code example
------------

.. literalinclude:: ../doc_code/anti_pattern_ray_get_loop.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

.. figure:: ../images/ray-get-loop.svg

    Calling ``ray.get()`` in a loop

When calling ``ray.get()`` right after scheduling the remote work, the loop blocks until the result is received. We thus end up with sequential processing.
Instead, we should first schedule all remote calls, which are then processed in parallel. After scheduling the work, we can then request all the results at once.

Other ``ray.get()`` related anti-patterns are:

- :doc:`unnecessary-ray-get`
- :doc:`ray-get-submission-order`
