Actor Task Execution Orders
===========================

Synchronous, Single-Threaded Actor
----------------------------------
In Ray, an actor receives tasks from multiple submitters (including driver and workers).
For tasks received from the same submitter, a synchronous, single-threaded actor executes
them following the submission order.
In other words, a given task will not be executed until previously submitted tasks from
the same submitter have finished execution.

.. tabbed:: Python

    .. code-block:: python

        import ray

        @ray.remote
        class Counter:
            def __init__(self):
                self.value = 0

            def add(self, addition):
                self.value += addition
                return self.value

        counter = Counter.remote()

        # For tasks from the same submitter,
        # they are executed according to submission order.
        value0 = counter.add.remote(1)
        value1 = counter.add.remote(2)

        # Output: 1. The first submitted task is executed first.
        print(ray.get(value0))
        # Output: 3. The later submitted task is executed later.
        print(ray.get(value1))


However, the actor does not guarantee the execution order of the tasks from different
submitters. For example, suppose an unfulfilled argument blocks a previously submitted
task. In this case, the actor can still execute tasks submitted by a different worker.

.. tabbed:: Python

    .. code-block:: python

        import time
        import ray

        @ray.remote
        class Counter:
            def __init__(self):
                self.value = 0

            def add(self, addition):
                self.value += addition
                return self.value

        counter = Counter.remote()

        # Submit task from a worker
        @ray.remote
        def submitter(value):
            return ray.get(counter.add.remote(value))

        # Simulate delayed result resolution.
        @ray.remote
        def delayed_resolution(value):
            time.sleep(5)
            return value

        # Submit tasks from different workers, with
        # the first submitted task waiting for
        # dependency resolution.
        value0 = submitter.remote(delayed_resolution.remote(1))
        value1 = submitter.remote(2)

        # Output: 3. The first submitted task is executed later.
        print(ray.get(value0))
        # Output: 2. The later submitted task is executed first.
        print(ray.get(value1))


Asynchronous or Threaded Actor
------------------------------
:ref:`Asynchronous or threaded actors <async-actors>` do not guarantee the
task execution order. This means the system might execute a task
even though previously submitted tasks are pending execution.

.. tabbed:: Python

    .. code-block:: python

        import time
        import ray

        @ray.remote
        class AsyncCounter:
            def __init__(self):
                self.value = 0

            async def add(self, addition):
                self.value += addition
                return self.value

        counter = AsyncCounter.remote()

        # Simulate delayed result resolution.
        @ray.remote
        def delayed_resolution(value):
            time.sleep(5)
            return value

        # Submit tasks from the driver, with
        # the first submitted task waiting for
        # dependency resolution.
        value0 = counter.add.remote(delayed_resolution.remote(1))
        value1 = counter.add.remote(2)

        # Output: 3. The first submitted task is executed later.
        print(ray.get(value0))
        # Output: 2. The later submitted task is executed first.
        print(ray.get(value1))
