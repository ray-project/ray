AsyncIO / Concurrency for Actors
================================

Since Python 3.5, it is possible to write concurrent code using the
``async/await`` `syntax <https://docs.python.org/3/library/asyncio.html>`__.
Ray natively integrates with asyncio. You can use ray alongside with popular
async frameworks like aiohttp, aioredis, etc.

You can try it about by running the following snippet in ``ipython`` or a shell
that supports top level ``await``:

.. code-block:: python

    import ray
    import asyncio
    ray.init()

    @ray.remote
    class AsyncActor:
        # multiple invocation of this method can be running in
        # the event loop at the same time
        async def run_concurrent(self):
            print("started")
            await asyncio.sleep(2) # concurrent workload here
            print("finished")

    actor = AsyncActor.remote()

    # regular ray.get
    ray.get([actor.run_concurrent.remote() for _ in range(4)])

    # async ray.get
    await actor.run_concurrent.remote()


ObjectRefs as asyncio.Futures
-----------------------------
ObjectRefs can be translated to asyncio.Future. This feature
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

    actor = AsyncActor.options(max_concurrency=10).remote()

    # Only 10 tasks will be running concurrently. Once 10 finish, the next 10 should run.
    ray.get([actor.run_task.remote() for _ in range(50)])


Threaded Actors
---------------

Sometimes asyncio is not an ideal solution for your actor. For example, you may
have one method that perform some computation heavy task blocking the event loop.
In async actors, only one task can be running at once. To achieve threaded
concurrency, you can use the ``max_concurrency`` options without any async methods.


.. warning::
    When there is at least one ``async def`` method in actor definition, Ray
    will recognize the actor as AsyncActor instead of ThreadedActor. There will
    be only one thread in AsyncActor! Don't add ``async def`` if you want a threadpool.


.. code-block:: python

    @ray.remote
    class ThreadedActor:
        def task_1(self): print("I'm running in a thread!")
        def task_2(self): print("I'm running in another thread!")

    a = ThreadedActor.options(max_concurrency=2).remote()
    ray.get([a.task_1.remote(), a.task_2.remote()])


.. note::
    Each invocation of the threaded actor will be running in a thread pool.
    The ``max_concurrency`` limit the size of the threadpool. Furthermore,
    keep in mind the Python's `Global Interpreter Lock (GIL) <https://wiki.python.org/moin/GlobalInterpreterLock>`_
    will only allow one thread of Python code running at once. This means if you
    are just parallelizing Python code, you won't get true parallelism. If you
    calls Numpy, Cython, Tensorflow, or PyTorch code, these libraries will release
    the GIL when calling into C/C++ functions.

