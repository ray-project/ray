Pattern: Multi-node synchronization using an Actor
==================================================

When you have multiple tasks that need to wait on some condition or otherwise
need to synchronize across tasks & actors on a cluster, you can use a central
actor to coordinate among them. Below is an example of using a ``SignalActor``
that wraps an ``asyncio.Event`` for basic synchronization.

Code example
------------

.. code-block:: python

    import asyncio

    import ray

    ray.init()

    # We set num_cpus to zero because this actor will mostly just block on I/O.
    @ray.remote(num_cpus=0)
    class SignalActor:
        def __init__(self):
            self.ready_event = asyncio.Event()

        def send(self, clear=False):
            self.ready_event.set()
            if clear:
                self.ready_event.clear()

        async def wait(self, should_wait=True):
            if should_wait:
                await self.ready_event.wait()

    @ray.remote
    def wait_and_go(signal):
        ray.get(signal.wait.remote())

        print("go!")

    signal = SignalActor.remote()
    tasks = [wait_and_go.remote(signal) for _ in range(4)]
    print("ready...")
    # Tasks will all be waiting for the signals.
    print("set..")
    ray.get(signal.send.remote())

    # Tasks are unblocked.
    ray.get(tasks)

    ##  Output is:
    # ready...
    # get set..

    # (pid=77366) go!
    # (pid=77372) go!
    # (pid=77367) go!
    # (pid=77358) go!
