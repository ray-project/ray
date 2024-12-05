.. _ray-remote-classes:
.. _actor-guide:

Actors
======

Actors extend the Ray API from functions to classes.
An actor is essentially a stateful worker, or a service. When your code instantiates
a new actor, Ray creates a new worker and schedules methods of the actor on
that specific worker, allowing those methods to access and mutate the state of that worker.

The ``ray.remote`` decorator indicates that instances of the ``Counter`` class are actors. Each actor runs in its own Python process.

.. testcode::

    import ray

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

        def get_counter(self):
            return self.value

    # Create an actor from this class.
    counter = Counter.remote()

Use `ray list actors` from the :ref:`State API <state-api-overview-ref>` to see actors states:

.. code-block:: bash

  # This API is only available when you install Ray with `pip install "ray[default]"`.
  ray list actors

.. code-block:: bash

  ======== List: 2023-05-25 10:10:50.095099 ========
  Stats:
  ------------------------------
  Total: 1

  Table:
  ------------------------------
      ACTOR_ID                          CLASS_NAME    STATE      JOB_ID  NAME    NODE_ID                                                     PID  RAY_NAMESPACE
   0  9e783840250840f87328c9f201000000  Counter       ALIVE    01000000          13a475571662b784b4522847692893a823c78f1d3fd8fd32a2624923  38906  ef9de910-64fb-4575-8eb5-50573faa3ddf


Specifying required resources
-----------------------------

.. _actor-resource-guide:

You can specify resource requirements in actors, too. See :ref:`resource-requirements` for more details.

.. testcode::

    # Specify required resources for an actor.
    @ray.remote(num_cpus=2, num_gpus=0.5)
    class Actor:
        pass

Calling the actor
-----------------

Interact with the actor by calling its methods with the ``remote``
operator. Then call ``get`` on the object ref to retrieve the actual
value.

.. testcode::

    # Call the actor.
    obj_ref = counter.increment.remote()
    print(ray.get(obj_ref))

.. testoutput::

    1

Methods called on different actors can execute in parallel, and methods called on the same actor execute serially in the order that they are called. Methods on the same actor share state with one another, as shown below.

.. testcode::

    # Create ten Counter actors.
    counters = [Counter.remote() for _ in range(10)]

    # Increment each Counter once and get the results. These tasks all happen in
    # parallel.
    results = ray.get([c.increment.remote() for c in counters])
    print(results)

    # Increment the first Counter five times. Ray executes these tasks serially
    # while sharing state.
    results = ray.get([counters[0].increment.remote() for _ in range(5)])
    print(results)

.. testoutput::

    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
    [2, 3, 4, 5, 6]

Passing actor handles
---------------------

You can pass actor handles into other tasks. Define remote functions, which are actor methods, that use actor handles.

.. testcode::

    import time

    @ray.remote
    def f(counter):
        for _ in range(10):
            time.sleep(0.1)
            counter.increment.remote()

If you instantiate an actor, you can pass the handle to various tasks.

.. testcode::

    counter = Counter.remote()

    # Start some tasks that use the actor.
    [f.remote(counter) for _ in range(3)]

    # Print the counter value.
    for _ in range(10):
        time.sleep(0.1)
        print(ray.get(counter.get_counter.remote()))

.. testoutput::
    :options: +MOCK

    0
    3
    8
    10
    15
    18
    20
    25
    30
    30

Generators
----------
Ray is compatible with Python generator syntax. See :ref:`Ray Generators <generators>` for more details.

Cancelling actor tasks
----------------------

Cancel actor tasks by calling :func:`ray.cancel() <ray.cancel>` on the returned `ObjectRef`.

.. literalinclude:: doc_code/actors.py
    :language: python
    :start-after: __cancel_start__
    :end-before: __cancel_end__


In Ray, task cancellation behavior is contingent on the task's current state:

- **Unscheduled tasks**: If Ray hasn't scheduled the actor task yet, it attempt
  to cancel the scheduling. When Ray successfully cancels it at this stage, 
  invoking ``ray.get(actor_task_ref)`` produces a
  :class:`TaskCancelledError <ray.exceptions.TaskCancelledError>`.

- **Running actor tasks (regular actor, threaded actor)**: For tasks classified
  as a single-threaded actor or a multi-threaded actor, Ray offers no mechanism
  for interruption.

- **Running async actor tasks**: For tasks classified as `async actors <_async-actors>`,
  Ray tries to cancel the associated `asyncio.Task`. This cancellation approach aligns 
  with the standards presented in
  `asyncio task cancellation <https://docs.python.org/3/library/asyncio-task.html#task-cancellation>`__. Note that Ray won't interrupt `asyncio.Task` in the middle of execution if you don't `await` within the async function.

- **Cancellation guarantee**: Ray attempts to cancel tasks on a *best-effort* basis, 
  meaning cancellation isn't always guaranteed. For example, if the cancellation 
  request doesn't get through to the executor, then Ray might not cancel the task.
  To see if Ray successfully cancelled a task, use ``ray.get(actor_task_ref)``.

- **Recursive cancellation**:Ray tracks all child and actor tasks. When the you give
  the ``recursive=True`` argument, it cancels all child and actor tasks.

Scheduling
----------

For each actor, Ray chooses a node to run it
and bases the scheduling decision a few factors like the following:

- :ref:`the actor's resource requirements <ray-scheduling-resources>`
- :ref:`the specified scheduling strategy <ray-scheduling-strategies>`
See :ref:`Ray scheduling <ray-scheduling>` for more details.

Fault tolerance
---------------

By default, Ray doesn't :ref:`restart <fault-tolerance-actors>` actors and
won't retry actor tasks when actors crash unexpectedly.
You can change this behavior by setting
``max_restarts`` and ``max_task_retries`` options
in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.actor.ActorClass.options>`.
See :ref:`Ray fault tolerance <fault-tolerance>` for more details.

Actors, workers, and resources
------------------------------

Understanding the difference between a worker and an actor can help you
optimize resource utilization.

Each Ray worker is a Python process. Ray treats workers differently for
tasks and actors. A Ray worker either:

#. executes multiple Ray tasks
#. is a dedicated Ray actor

**Tasks**: When Ray starts on a machine, Ray starts a number of Ray workers automatically (1 per CPU by default). Ray uses them execute tasks like a process pool. If you execute 8 tasks with `num_cpus=2`, and total number of CPUs is 16 (`ray.cluster_resources()["CPU"] == 16`), 8 of the 16 workers idle.

**Actor**: A Ray actor is also a Ray worker but Ray instantiates it at runtime upon `actor_cls.remote()`. All of its methods run on the same process, using the same resources designated when defining the actor. Note that unlike tasks, the Ray doesn't reuse the Python processes that run Ray actors. Ray terminates them when it deletes the actor.

To maximally utilize your resources, maximize the time that
the workers are working. You also want to allocate enough cluster resources
so that all of the needed actors can run and any other tasks you
define can run. Ray schedules tasks more flexibly,
so if you don't need the stateful part of an actor, use tasks.

Task events
-----------

By default, Ray traces the execution of actor tasks, reporting task status events and profiling events
that Ray Dashboard and :ref:`State API <state-api-overview-ref>` use.

You can turn off task events for the actor by setting the `enable_task_events` option to `False` in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.actor.ActorClass.options>`, which reduces the overhead of task execution, and the amount of data the Ray sends to the Ray Dashboard.

You can also turn off task events for some actor methods by setting the `enable_task_events` option to `False` in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.remote_function.RemoteFunction.options>` on the actor method.
Method settings override the actor setting:

.. literalinclude:: doc_code/actors.py
    :language: python
    :start-after: __enable_task_events_start__
    :end-before: __enable_task_events_end__


More about Ray actors
---------------------

.. toctree::
    :maxdepth: 1

    actors/named-actors.rst
    actors/terminating-actors.rst
    actors/async_api.rst
    actors/concurrency_group_api.rst
    actors/actor-utils.rst
    actors/out-of-band-communication.rst
    actors/task-orders.rst
