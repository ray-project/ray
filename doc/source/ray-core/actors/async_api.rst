AsyncIO / Concurrency for Actors
================================

Within a single actor process, it is possible to execute concurrent threads.

Ray offers two types of concurrency within an actor:

 * :ref:`async execution <async-actors>`
 * :ref:`threading <threaded-actors>`


Keep in mind that the Python's `Global Interpreter Lock (GIL) <https://wiki.python.org/moin/GlobalInterpreterLock>`_ will only allow one thread of Python code running at once.

This means if you are just parallelizing Python code, you won't get true parallelism. If you call Numpy, Cython, Tensorflow, or PyTorch code, these libraries will release the GIL when calling into C/C++ functions.

**Neither the** :ref:`threaded-actors` nor :ref:`async-actors` **model will allow you to bypass the GIL.**

.. _async-actors:

AsyncIO for Actors
------------------

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ObjectRefs can be translated to asyncio.Futures. This feature
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

If you need to directly access the future object, you can call:

.. code-block:: python

    fut: asyncio.Future = asyncio.wrap_future(ref.future())

ObjectRefs as concurrent.futures.Futures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ObjectRefs can also be wrapped into ``concurrent.futures.Future`` objects. This
is useful for interfacing with existing ``concurrent.futures`` APIs:

.. code-block:: python

    refs = [fun.remote() for _ in range(4)]
    futs = [ref.future() for ref in refs]
    for fut in concurrent.futures.as_completed(futs):
        assert fut.done()
        print(fut.result())


Defining an Async Actor
~~~~~~~~~~~~~~~~~~~~~~~

By using `async` method definitions, Ray will automatically detect whether an actor support `async` calls or not.

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

In async actors, only one task can be running at any point in time (though tasks can be multi-plexed). There will be only one thread in AsyncActor! See :ref:`threaded-actors` if you want a threadpool.

Setting concurrency in Async Actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set the number of "concurrent" task running at once using the
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

.. _threaded-actors:

Threaded Actors
---------------

Ray provides a "threaded actor" functionality, which allows multiple actor method invocations to run in parallel in a thread pool. 
You can configure this by setting the ``max_concurrency`` of the actor, which defaults to 1 if no `async` methods are defined in the class.

As a reminder, this will not provide "true" parallel threading due to Python's `Global Interpreter Lock (GIL) <https://wiki.python.org/moin/GlobalInterpreterLock>`_, which will only allow one thread of Python code running at once.


.. warning::
    When there is at least one ``async def`` method in actor definition, Ray
    will recognize the actor as an async actor instead of a threaded actor.
    

When would I use this over Async Actors?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In certain situations, threaded actors can be more performant. For example, you may
have one method that performs some computation heavy task while blocking the event loop, not giving up control via ``await``. This would hurt the performance of an Async Actor because Async Actors can only execute 1 task at a time and rely on ``await`` to context switch.




.. code-block:: python

    @ray.remote
    class ThreadedActor:
        def task_1(self): print("I'm running in a thread!")
        def task_2(self): print("I'm running in another thread!")

    a = ThreadedActor.options(max_concurrency=2).remote()
    ray.get([a.task_1.remote(), a.task_2.remote()])



AsyncIO for Remote Tasks
------------------------

We don't support asyncio for remote tasks. The following snippet will fail:

.. code-block:: python

    @ray.remote
    async def f():
        pass

Instead, you can wrap the ``async`` function with a wrapper to run the task synchronously:

.. code-block:: python

    async def f():
        pass

    @ray.remote
    def wrapper():
        import asyncio
        asyncio.run(f())
        # For python < 3.7: 
        # asyncio.get_event_loop().run_until_complete(f())
    
    
