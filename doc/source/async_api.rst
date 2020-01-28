Async API (Experimental)
========================

Since Python 3.5, it is possible to write concurrent code using the
``async/await`` `syntax <https://docs.python.org/3/library/asyncio.html>`__.

This document describes Ray's support for asyncio, which enables integration
with popular async frameworks (e.g., aiohttp, aioredis, etc.) for highly
concurrent workloads.

ObjectIDs as asyncio.Futures
--------------------------
ObjectIDs can be translated to asyncio.Future. This feature
make it possible to ``await`` on ray futures in existing concurrent
applications.

Instead of:

.. code-block:: python

    @ray.remote
    def some_task():
        return 1

    ray.get(some_task.remote())
    ray.wait([some_task.remote()])

you can do:

.. code-block:: python

    @ray.remote
    def some_task():
        return 1

    await some_task.remote()
    await asyncio.wait([some_task.remote()])

Please refer to `asyncio doc <https://docs.python.org/3/library/asyncio-task.html>`__
for more `asyncio` patterns including timeouts and ``asyncio.gather``.


Async Actor
-----------
Ray also supports concurrent multitasking by executing many actor tasks at once.
To do so, you can define an actor with async methods:

.. code-block:: python

    import asyncio

    @ray.remote
    class AsyncActor:
        async def run_task(self):
            print("started")
            await asyncio.sleep(1) # Network, I/O task here
            print("ended")

    actor = AsyncActor.remote()
    # All 50 tasks should start at once. After 1 second they should all finish.
    # they should finish at the same time
    ray.get([actor.run_task.remote() for _ in range(50)])

Under the hood, Ray runs all of the methods inside a single python event loop.
Please note that running blocking ``ray.get`` or ``ray.wait`` inside async
actor method is not allowed, because ``ray.get`` will block the execution
of the event loop.

You can limit the number of concurrent task running at once using the
``max_concurrency`` flag. By default, 1000 tasks can be running concurrently.

.. code-block:: python

    import asyncio

    @ray.remote
    class AsyncActor:
        async def run_task(self):
            print("started")
            await asyncio.sleep(1) # Network, I/O task here
            print("ended")

    actor = AsyncActor.options(max_concurreny=10).remote()

    # Only 10 tasks will be running concurrently. Once 10 finish, the next 10 should run.
    ray.get([actor.run_task.remote() for _ in range(50)])


Known Issues
------------

Async API support is experimental, and we are working to improve it.
Please `let us know <https://github.com/ray-project/ray/issues>`__
any issues you encounter.
