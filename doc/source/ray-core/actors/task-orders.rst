Actor Tasks Execution Orders
============================

Synchronous, Single-Threaded Actor
----------------------------------
In Ray, an actor receives tasks from multiple submitters (including driver and workers).
Synchronous, single-threaded actors execute tasks following the submission order
if the same submitter submits the tasks.
Precisely, for tasks received from the same submitter,
any task will not be executed until previously submitted tasks have finished execution.

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
        # their are executed according to submission order.
        value0 = counter.add.remote(1)
        value1 = counter.add.remote(2)

        # Output: 1. The first submitted task is executed first.
        print(ray.get(value0))
        # Output: 3. The later submitted task is executed later.
        print(ray.get(value1))


However, the system could reorder actor tasks execution if they come from different
submitters. The reorder could happen when a previously submitted task is blocked
by unfulfilled dependence. In this case, the actor might execute a later
received task from a different submitter.

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
:ref:`Asynchronous or threaded actors <async-actors>` does not guarantee the
task execution order. For example, the system might execute a task
even though previously submitted tasks are pending execution.
