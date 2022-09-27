Pattern: Concurrent operations with async actor
===============================================

Sometimes, we'd like to have IO operations to other actors/tasks/components (e.g., DB) periodically within an actor (long polling). Imagine a process queue actor that needs to fetch data from other actors or DBs. 

This is problematic because actors are running within a single thread. One of the solutions is to use a background thread within an actor, but you can also achieve this by using Ray's async actors APIs. 

Let's see why it is difficult by looking at an example.

Code example
------------

.. code-block:: python

    @ray.remote
    class LongPollingActor:
        def __init__(self, data_store_actor):
            self.data_store_actor = data_store_actor

        def run(self):
            while True:
                data = ray.get(self.data_store_actor.fetch.remote())
                self._process(data)

        def other_task(self):
            return True

        def _process(self, data):
            # Do process here...
            pass

There are 2 issues here.

1) Since a long polling actor has a run method that runs forever with while True, it cannot run any other actor task (because the thread is occupied by the while loop). That says

.. code-block:: python

    l = LongPollingActor.remote(data_store_actor)
    # Actor runs a while loop
    l.run.remote()
    # This won't be processed forever because the actor thread is occupied by the run method.
    ray.get(l.other_task.remote())

2) Since we need to call :ref:`ray.get within a loop <ray-get-loop>`, the loop is blocked until ray.get returns (it is because ``ray.get`` is a blocking API).

We can make this better if we use Ray's async APIs. Here is a documentation about ray's async APIs and async actors.

First, let's create an async actor.

.. code-block:: python

    @ray.remote
    class LongPollingActorAsync:
        def __init__(self, data_store_actor):
            self.data_store_actor = data_store_actor

        async def run(self):
            while True:
                # Coroutine will switch context when "await" is called.
                data = await self.data_store_actor.fetch.remote()
                self._process(data)

        def _process(self, data):
            pass

        async def other_task(self):
            return True

Now, it will work if you run the same code we used before.

.. code-block:: python

    l = LongPollingActorAsync.remote(data_store_actor)
    l.run.remote()
    ray.get(l.other_task.remote())

Now, let's learn why this works. When an actor contains async methods, the actor will be converted to async actors. This means all the ray's tasks will run as a coroutine. That says, when it meets the ``await`` keyword, the actor will switch to a different coroutine, which is a coroutine that runs ``other_task`` method.

You can implement interesting actors using this pattern. Note that it is also possible to switch context easily if you use await ``asyncio.sleep(0)`` without any delay.
