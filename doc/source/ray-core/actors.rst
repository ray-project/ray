.. _ray-remote-classes:
.. _actor-guide:

Actors
======

Actors extend the Ray API from functions (tasks) to classes.
An actor is essentially a stateful worker (or a service).
When you instantiate a new actor, Ray creates a new worker and schedules methods of the actor on
that specific worker. The methods can access and mutate the state of that worker.

.. tab-set::

    .. tab-item:: Python

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

    .. tab-item:: Java

        ``Ray.actor`` is used to create actors from regular Java classes.

        .. code-block:: java

          // A regular Java class.
          public class Counter {

            private int value = 0;

            public int increment() {
              this.value += 1;
              return this.value;
            }
          }

          // Create an actor from this class.
          // `Ray.actor` takes a factory method that can produce
          // a `Counter` object. Here, we pass `Counter`'s constructor
          // as the argument.
          ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

    .. tab-item:: C++

        ``ray::Actor`` is used to create actors from regular C++ classes.

        .. code-block:: c++

          // A regular C++ class.
          class Counter {

          private:
              int value = 0;

          public:
            int Increment() {
              value += 1;
              return value;
            }
          };

          // Factory function of Counter class.
          static Counter *CreateCounter() {
              return new Counter();
          };

          RAY_REMOTE(&Counter::Increment, CreateCounter);

          // Create an actor from this class.
          // `ray::Actor` takes a factory method that can produce
          // a `Counter` object. Here, we pass `Counter`'s factory function
          // as the argument.
          auto counter = ray::Actor(CreateCounter).Remote();



Use `ray list actors` from :ref:`State API <state-api-overview-ref>` to see actors states:

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

Specify resource requirements in actors. See :ref:`resource-requirements` for more details.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            # Specify required resources for an actor.
            @ray.remote(num_cpus=2, num_gpus=0.5)
            class Actor:
                pass

    .. tab-item:: Java

        .. code-block:: java

            // Specify required resources for an actor.
            Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();

    .. tab-item:: C++

        .. code-block:: c++

            // Specify required resources for an actor.
            ray::Actor(CreateCounter).SetResource("CPU", 2.0).SetResource("GPU", 0.5).Remote();


Calling the actor
-----------------

You can interact with the actor by calling its methods with the ``remote``
operator. You can then call ``get`` on the object ref to retrieve the actual
value.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            # Call the actor.
            obj_ref = counter.increment.remote()
            print(ray.get(obj_ref))

        .. testoutput::

            1

    .. tab-item:: Java

        .. code-block:: java

            // Call the actor.
            ObjectRef<Integer> objectRef = counter.task(&Counter::increment).remote();
            Assert.assertTrue(objectRef.get() == 1);

    .. tab-item:: C++

        .. code-block:: c++

            // Call the actor.
            auto object_ref = counter.Task(&Counter::increment).Remote();
            assert(*object_ref.Get() == 1);

Methods called on different actors execute in parallel, and methods called on the same actor execute serially in the order you call them. Methods on the same actor share state with one another, as shown below.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            # Create ten Counter actors.
            counters = [Counter.remote() for _ in range(10)]

            # Increment each Counter once and get the results. These tasks all happen in
            # parallel.
            results = ray.get([c.increment.remote() for c in counters])
            print(results)

            # Increment the first Counter five times. These tasks are executed serially
            # and share state.
            results = ray.get([counters[0].increment.remote() for _ in range(5)])
            print(results)

        .. testoutput::

            [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
            [2, 3, 4, 5, 6]

    .. tab-item:: Java

        .. code-block:: java

            // Create ten Counter actors.
            List<ActorHandle<Counter>> counters = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                counters.add(Ray.actor(Counter::new).remote());
            }

            // Increment each Counter once and get the results. These tasks all happen in
            // parallel.
            List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
            for (ActorHandle<Counter> counterActor : counters) {
                objectRefs.add(counterActor.task(Counter::increment).remote());
            }
            // prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
            System.out.println(Ray.get(objectRefs));

            // Increment the first Counter five times. These tasks are executed serially
            // and share state.
            objectRefs = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                objectRefs.add(counters.get(0).task(Counter::increment).remote());
            }
            // prints [2, 3, 4, 5, 6]
            System.out.println(Ray.get(objectRefs));

    .. tab-item:: C++

        .. code-block:: c++

            // Create ten Counter actors.
            std::vector<ray::ActorHandle<Counter>> counters;
            for (int i = 0; i < 10; i++) {
                counters.emplace_back(ray::Actor(CreateCounter).Remote());
            }

            // Increment each Counter once and get the results. These tasks all happen in
            // parallel.
            std::vector<ray::ObjectRef<int>> object_refs;
            for (ray::ActorHandle<Counter> counter_actor : counters) {
                object_refs.emplace_back(counter_actor.Task(&Counter::Increment).Remote());
            }
            // prints 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
            auto results = ray::Get(object_refs);
            for (const auto &result : results) {
                std::cout << *result;
            }

            // Increment the first Counter five times. These tasks are executed serially
            // and share state.
            object_refs.clear();
            for (int i = 0; i < 5; i++) {
                object_refs.emplace_back(counters[0].Task(&Counter::Increment).Remote());
            }
            // prints 2, 3, 4, 5, 6
            results = ray::Get(object_refs);
            for (const auto &result : results) {
                std::cout << *result;
            }

Passing around actor handles
----------------------------

You can pass actor handles into other tasks. You can also define remote functions or actor methods that use actor handles.

.. tab-set::

    .. tab-item:: Python

        .. testcode::

            import time

            @ray.remote
            def f(counter):
                for _ in range(10):
                    time.sleep(0.1)
                    counter.increment.remote()

    .. tab-item:: Java

        .. code-block:: java

            public static class MyRayApp {

              public static void foo(ActorHandle<Counter> counter) throws InterruptedException {
                for (int i = 0; i < 1000; i++) {
                  TimeUnit.MILLISECONDS.sleep(100);
                  counter.task(Counter::increment).remote();
                }
              }
            }

    .. tab-item:: C++

        .. code-block:: c++

            void Foo(ray::ActorHandle<Counter> counter) {
                for (int i = 0; i < 1000; i++) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    counter.Task(&Counter::Increment).Remote();
                }
            }

If you instantiate an actor, you can pass the handle around to various tasks.

.. tab-set::

    .. tab-item:: Python

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

    .. tab-item:: Java

        .. code-block:: java

            ActorHandle<Counter> counter = Ray.actor(Counter::new).remote();

            // Start some tasks that use the actor.
            for (int i = 0; i < 3; i++) {
              Ray.task(MyRayApp::foo, counter).remote();
            }

            // Print the counter value.
            for (int i = 0; i < 10; i++) {
              TimeUnit.SECONDS.sleep(1);
              System.out.println(counter.task(Counter::getCounter).remote().get());
            }

    .. tab-item:: C++

        .. code-block:: c++

            auto counter = ray::Actor(CreateCounter).Remote();

            // Start some tasks that use the actor.
            for (int i = 0; i < 3; i++) {
              ray::Task(Foo).Remote(counter);
            }

            // Print the counter value.
            for (int i = 0; i < 10; i++) {
              std::this_thread::sleep_for(std::chrono::seconds(1));
              std::cout << *counter.Task(&Counter::GetCounter).Remote().Get() << std::endl;
            }


Type hints and static typing for actors
---------------------------------------

Ray supports Python type hints for both remote functions and actors, enabling better IDE support and static type checking. To get the best type inference and pass type checkers when working with actors, follow these patterns:

- **Prefer** ``ray.remote(MyClass)`` **over** ``@ray.remote`` **for actors**: Instead of decorating your class with ``@ray.remote``, use ``ActorClass = ray.remote(MyClass)``. This preserves the original class type and allows type checkers and IDEs to infer the correct types.

- **Use** ``@ray.method`` **for actor methods**: Decorate actor methods with ``@ray.method`` to enable type hints for remote method calls on actor handles.

- **Use the** ``ActorClass`` **and** ``ActorProxy`` **types**: When you instantiate an actor, annotate the handle as ``ActorProxy[MyClass]`` to get type hints for remote methods.

**Example:**

.. testcode::

    import ray
    from ray.actor import ActorClass, ActorProxy

    class Counter:
        def __init__(self):
            self.value = 0

        @ray.method
        def increment(self) -> int:
            self.value += 1
            return self.value

    CounterActor: ActorClass[Counter] = ray.remote(Counter)
    counter: ActorProxy[Counter] = CounterActor.remote()

    # Type checkers and IDEs will now provide type hints for remote methods
    obj_ref: ray.ObjectRef[int] = counter.increment.remote()
    print(ray.get(obj_ref))

For more details and advanced patterns, see :ref:`Type hints in Ray <core-type-hint>`.


Generators
----------
Ray is compatible with Python generator syntax. See :ref:`Ray Generators <generators>` for more details.

Cancelling actor tasks
----------------------

Cancel Actor Tasks by calling :func:`ray.cancel() <ray.cancel>` on the returned `ObjectRef`.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: doc_code/actors.py
            :language: python
            :start-after: __cancel_start__
            :end-before: __cancel_end__


In Ray, Task cancellation behavior is contingent on the Task's current state:

**Unscheduled tasks**:
If Ray hasn't scheduled an Actor Task yet, Ray attempts to cancel the scheduling.
When Ray successfully cancels at this stage, it invokes ``ray.get(actor_task_ref)``
which produces a :class:`TaskCancelledError <ray.exceptions.TaskCancelledError>`.

**Running actor tasks (regular actor, threaded actor)**:
For tasks classified as a single-threaded Actor or a multi-threaded Actor,
Ray sets a cancellation flag that can be checked via ``ray.get_runtime_context().is_canceled()``.
This allows for graceful cancellation by periodically checking the cancellation status within the task.

**Running async actor tasks**:
For Tasks classified as :ref:`async Actors <async-actors>`, Ray seeks to cancel the associated `asyncio.Task`.
This cancellation approach aligns with the standards presented in
`asyncio task cancellation <https://docs.python.org/3/library/asyncio-task.html#task-cancellation>`__.
Note that `asyncio.Task` won't be interrupted in the middle of execution if you don't `await` within the async function.
Note: ``ray.get_runtime_context().is_canceled()`` is not supported for async actors and will raise a ``RuntimeError``.

**Cancellation guarantee**:
Ray attempts to cancel Tasks on a *best-effort* basis, meaning cancellation isn't always guaranteed.
For example, if the cancellation request doesn't get through to the executor,
the Task might not be cancelled.
You can check if a Task was successfully cancelled using ``ray.get(actor_task_ref)``.

**Recursive cancellation**:
Ray tracks all child and Actor Tasks. When the ``recursive=True`` argument is given,
it cancels all child and Actor Tasks.

Detecting cancellation in running actor tasks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For non-async actor tasks, you can periodically check whether a cancellation has been requested
by calling ``ray.get_runtime_context().is_canceled()``. This allows tasks to detect cancellation
and perform cleanup operations before exiting gracefully.

.. tab-set::

    .. tab-item:: Python

        .. literalinclude:: doc_code/actors.py
            :language: python
            :start-after: __cancel_graceful_actor_start__
            :end-before: __cancel_graceful_actor_end__

**Important notes:**

- For **non-async actor tasks**, direct interruption is not supported. You need to check ``is_canceled()`` periodically to detect cancellation requests.
- ``is_canceled()`` is **not supported** for async actor tasks and will raise a ``RuntimeError``.

Scheduling
----------

For each actor, Ray chooses a node to run it on,
and bases the scheduling decision on a few factors like
:ref:`the actor's resource requirements <ray-scheduling-resources>`
and :ref:`the specified scheduling strategy <ray-scheduling-strategies>`.
See :ref:`Ray scheduling <ray-scheduling>` for more details.

Fault Tolerance
---------------

By default, Ray actors won't be :ref:`restarted <fault-tolerance-actors>` and
actor tasks won't be retried when actors crash unexpectedly.
You can change this behavior by setting
``max_restarts`` and ``max_task_retries`` options
in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.actor.ActorClass.options>`.
See :ref:`Ray fault tolerance <fault-tolerance>` for more details.

FAQ: Actors, Workers and Resources
----------------------------------

What's the difference between a worker and an actor?

Each "Ray worker" is a python process.

Ray treats a worker differently for tasks and actors. For tasks, Ray uses a "Ray worker" to execute multiple Ray tasks. For actors, Ray starts a "Ray worker" as a dedicated Ray actor.

* **Tasks**: When Ray starts on a machine, a number of Ray workers start automatically (1 per CPU by default). Ray uses them to execute tasks (like a process pool). If you execute 8 tasks with `num_cpus=2`, and total number of CPUs is 16 (`ray.cluster_resources()["CPU"] == 16`), you end up with 8 of your 16 workers idling.

* **Actor**: A Ray Actor is also a "Ray worker" but you instantiate it at runtime with `actor_cls.remote()`. All of its methods run on the same process, using the same resources Ray designates when you define the Actor. Note that unlike tasks, Ray doesn't reuse the Python processes that run Ray Actors. Ray terminates them when you delete the Actor.

To maximally utilize your resources, you want to maximize the time that
your workers work. You also want to allocate enough cluster resources
so Ray can run all of your needed actors and any other tasks you
define. This also implies that Ray schedules tasks more flexibly,
and that if you don't need the stateful part of an actor, it's better
to use tasks.

Task Events
-----------

By default, Ray traces the execution of actor tasks, reporting task status events and profiling events
that Ray Dashboard and :ref:`State API <state-api-overview-ref>` use.

You can disable task event reporting for the actor by setting the `enable_task_events` option to `False` in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.actor.ActorClass.options>`. This setting reduces the overhead of task execution by reducing the amount of data Ray sends to the Ray Dashboard.

You can also disable task event reporting for some actor methods by setting the `enable_task_events` option to `False` in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.remote_function.RemoteFunction.options>` on the actor method.
Method settings override the actor setting:

.. literalinclude:: doc_code/actors.py
    :language: python
    :start-after: __enable_task_events_start__
    :end-before: __enable_task_events_end__


More about Ray Actors
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
