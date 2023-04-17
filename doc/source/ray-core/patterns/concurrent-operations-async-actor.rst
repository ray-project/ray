Pattern: Using asyncio to run actor methods concurrently
========================================================

By default, a Ray :ref:`actor <ray-remote-classes>` runs in a single thread and
actor method calls are executed sequentially. This means that a long running method call blocks all the following ones.
In this pattern, we use ``await`` to yield control from the long running method call so other method calls can run concurrently.
Normally the control is yielded when the method is doing IO operations but you can also use ``await asyncio.sleep(0)`` to yield control explicitly.

.. note::
   You can also use :ref:`threaded actors <threaded-actors>` to achieve concurrency.

Example use case
----------------

You have an actor with a long polling method that continuously fetches tasks from the remote store and executes them.
You also want to query the number of tasks executed while the long polling method is running.

With the default actor, the code will look like this:

.. literalinclude:: ../doc_code/pattern_async_actor.py
    :language: python
    :start-after: __sync_actor_start__
    :end-before: __sync_actor_end__

This is problematic because ``TaskExecutor.run`` method runs forever and never yield the control to run other methods.
We can solve this problem by using :ref:`async actors <async-actors>` and use ``await`` to yield control:

.. literalinclude:: ../doc_code/pattern_async_actor.py
    :language: python
    :start-after: __async_actor_start__
    :end-before: __async_actor_end__

Here, instead of using the blocking :func:`ray.get() <ray.get>` to get the value of an ObjectRef, we use ``await`` so it can yield the control while we are waiting for the object to be fetched.

