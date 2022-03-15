Actor Patterns
============


Actor Pool
~~~~~~~~~~

.. tabbed:: Python

    The ``ray.util`` module contains a utility class, ``ActorPool``.
    This class is similar to multiprocessing.Pool and lets you schedule Ray tasks over a fixed pool of actors.

    .. code-block:: python

        from ray.util import ActorPool

        @ray.remote
        class Actor
        def double(self, n):
            return n * 2

        a1, a2 = Actor.remote(), Actor.remote()
        pool = ActorPool([a1, a2])

        # pool.map(..) returns a Python generator object ActorPool.map
        gen = pool.map(lambda a, v: a.double.remote(v), [1, 2, 3, 4]))
        print([v for v in gen])
        # [2, 4, 6, 8]

    See the `package reference <package-ref.html#ray.util.ActorPool>`_ for more information.

.. tabbed:: Java

    Actor pool hasn't been implemented in Java yet.

.. tabbed:: C++

    Actor pool hasn't been implemented in C++ yet.


Message passing using Ray Queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sometimes just using one signal to synchronize is not enough. If you need to send data among many tasks or
actors, you can use :ref:`ray.util.queue.Queue <ray-queue-ref>`.

.. code-block:: python

    import ray
    from ray.util.queue import Queue

    ray.init()
    # You can pass this object around to different tasks/actors
    queue = Queue(maxsize=100)

    @ray.remote
    def consumer(queue):
        next_item = queue.get(block=True)
        print(f"got work {next_item}")

    consumers = [consumer.remote(queue) for _ in range(2)]

    [queue.put(i) for i in range(10)]

Ray's Queue API has similar API as Python's ``asyncio.Queue`` and ``queue.Queue``.



Multi-node synchronization using an Actor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you have multiple tasks that need to wait on some condition or otherwise
need to synchronize across tasks & actors on a cluster, you can use a central
actor to coordinate among them. Below is an example of using a ``SignalActor``
that wraps an ``asyncio.Event`` for basic synchronization.

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
