Utility Classes
===============

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
