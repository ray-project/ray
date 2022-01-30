.. _actor-guide:

Using Actors
============

An actor is essentially a stateful worker (or a service). When a new actor is
instantiated, a new worker is created, and methods of the actor are scheduled on
that specific worker and can access and mutate the state of that worker.

Java demo code in this documentation can be found `here <https://github.com/ray-project/ray/blob/master/java/test/src/main/java/io/ray/docdemo/UsingActorsDemo.java>`__.

Creating an actor
-----------------

.. tabbed:: Python

    You can convert a standard Python class into a Ray actor class as follows:

    .. code-block:: python

      @ray.remote
      class Counter(object):
          def __init__(self):
              self.value = 0

          def increment(self):
              self.value += 1
              return self.value

          def get_counter(self):
              return self.value

      counter_actor = Counter.remote()

    Note that the above is equivalent to the following:

    .. code-block:: python

      class Counter(object):
          def __init__(self):
              self.value = 0

          def increment(self):
              self.value += 1
              return self.value

          def get_counter(self):
              return self.value

      Counter = ray.remote(Counter)

      counter_actor = Counter.remote()

.. tabbed:: Java

    You can convert a standard Java class into a Ray actor class as follows:

    .. code-block:: java

      // A regular Java class.
      public class Counter {

        private int value = 0;

        public int increment() {
          this.value += 1;
          return this.value;
        }

        public int getCounter() {
          return this.value;
        }

        public void reset(int newValue) {
          this.value = newValue;
        }
      }

      public class CounterFactory {
        public static Counter createCounter() {
          return new Counter();
        }
      }

      // Create an actor with a constructor.
      Ray.actor(Counter::new).remote();
      // Create an actor with a factory method.
      Ray.actor(CounterFactory::createCounter).remote();

.. tabbed:: C++

    You can convert a standard C++ class into a Ray actor class as follows:

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

        int GetCounter() {
          return value;
        }

        void Reset(int new_value) {
          value = new_value;
        }
      };

      // Factory function of Counter class.
      Counter* CreateCounter() {
          return new Counter();
        }

      RAY_REMOTE(&Counter::Increment, &Counter::GetCounter,
                 &Counter::Reset, CreateCounter);

      // Create an actor with a factory method.
      ray::Actor(CreateCounter).Remote();

When the above actor is instantiated, the following events happen.

1. A node in the cluster is chosen and a worker process is created on that node
   for the purpose of running methods called on the actor.
2. A ``Counter`` object is created on that worker and the ``Counter``
   constructor is run.

Actor Methods
-------------

Methods of the actor can be called remotely.

.. tabbed:: Python

    .. code-block:: python

        counter_actor = Counter.remote()

        assert ray.get(counter_actor.increment.remote()) == 1

        @ray.remote
        class Foo(object):

            # Any method of the actor can return multiple object refs.
            @ray.method(num_returns=2)
            def bar(self):
                return 1, 2

        f = Foo.remote()

        obj_ref1, obj_ref2 = f.bar.remote()
        assert ray.get(obj_ref1) == 1
        assert ray.get(obj_ref2) == 2

.. tabbed:: Java

    .. code-block:: java

        ActorHandle<Counter> counterActor = Ray.actor(Counter::new).remote();
        // Call an actor method with a return value
        Assert.assertEquals((int) counterActor.task(Counter::increment).remote().get(), 1);
        // Call an actor method without return value. In this case, the return type of `remote()` is void.
        counterActor.task(Counter::reset, 10).remote();
        Assert.assertEquals((int) counterActor.task(Counter::increment).remote().get(), 11);

.. tabbed:: C++

    .. code-block:: c++

        ray::ActorHandle<Counter> counter_actor = ray::Actor(CreateCounter).Remote();
        // Call an actor method with a return value
        assert(*counter_actor.Task(&Counter::Increment).Remote().Get(), 1);
        // Call an actor method without return value. In this case, the return type of `Remote()` is void.
        counter_actor.Task(&Counter::Reset).Remote(10);
        assert(*counter_actor.Task(&Counter::Increment).Remote().Get(), 11);

.. _actor-resource-guide:

Specifying Resources
--------------------

You can specify that an actor requires CPUs or GPUs in the decorator. While Ray has built-in support for CPUs and GPUs, Ray can also handle custom resources.

.. tabbed:: Python

    When using GPUs, Ray will automatically set the environment variable ``CUDA_VISIBLE_DEVICES`` for the actor after instantiated. The actor will have access to a list of the IDs of the GPUs
    that it is allowed to use via ``ray.get_gpu_ids()``. This is a list of strings,
    like ``[]``, or ``['1']``, or ``['2', '5', '6']``. Under some circumstances, the IDs of GPUs could be given as UUID strings instead of indices (see the `CUDA programming guide <https://docs.nvidia.com/cuda/cuda-c-programming-guide/index.html#env-vars>`__).

    .. code-block:: python

      @ray.remote(num_cpus=2, num_gpus=1)
      class GPUActor(object):
          pass

.. tabbed:: Java

    In Java, we always specify resources when creating actors. There's no annotation available to act like the Python decorator ``@ray.remote(...)``.

    .. code-block:: java

        public class GpuActor {
        }

        Ray.actor(GpuActor::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();

.. tabbed:: C++

    In C++, we always specify resources when creating actors. There's no annotation available to act like the Python decorator ``@ray.remote(...)``.

    .. code-block:: c++

      class GpuActor {
        static GpuActor* CreateGpuActor() {
          return new GpuActor();
        }
      }

      ray::Actor(&GpuActor::CreateGpuActor).SetResource("CPU", 2.0).SetResource("GPU", 0.5).Remote();

When an ``GPUActor`` instance is created, it will be placed on a node that has
at least 1 GPU, and the GPU will be reserved for the actor for the duration of
the actor's lifetime (even if the actor is not executing tasks). The GPU
resources will be released when the actor terminates.

If you want to use custom resources, make sure your cluster is configured to
have these resources (see `configuration instructions
<configure.html#cluster-resources>`__):

.. note::

  * If you specify resource requirements in an actor class's remote decorator,
    then the actor will acquire those resources for its entire lifetime (if you
    do not specify CPU resources, the default is 0), even if it is not executing
    any methods. The actor will not acquire any additional resources when
    executing methods.
  * If you do not specify any resource requirements in the actor class's remote
    decorator, then by default, the actor will not acquire any resources for its
    lifetime.

.. tabbed:: Python

    .. code-block:: python

        @ray.remote(resources={'Resource2': 1})
        class GPUActor(object):
            pass

.. tabbed:: Java

    .. code-block:: java

        public class GpuActor {
        }

        Ray.actor(GpuActor::new).setResource("Resource2", 1.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        class GpuActor {
          static GpuActor* CreateGpuActor() {
            return new GpuActor();
          }
        }

        ray::Actor(&GpuActor::CreateGpuActor).SetResource("Resource2", 1.0).Remote();


If you need to instantiate many copies of the same actor with varying resource
requirements, you can do so as follows.

.. tabbed:: Python

    .. code-block:: python

        @ray.remote(num_cpus=4)
        class Counter(object):
            ...

        a1 = Counter.options(num_cpus=1, resources={"Custom1": 1}).remote()
        a2 = Counter.options(num_cpus=2, resources={"Custom2": 1}).remote()
        a3 = Counter.options(num_cpus=3, resources={"Custom3": 1}).remote()

.. tabbed:: Java

    .. code-block:: java

        public class Counter {
          ...
        }

        ActorHandle<Counter> a1 = Ray.actor(Counter::new).setResource("CPU", 1.0)
          .setResource("Custom1", 1.0).remote();
        ActorHandle<Counter> a2 = Ray.actor(Counter::new).setResource("CPU", 2.0)
          .setResource("Custom2", 1.0).remote();
        ActorHandle<Counter> a3 = Ray.actor(Counter::new).setResource("CPU", 3.0)
          .setResource("Custom3", 1.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        class Counter {
          ...
        }

        auto a1 = ray::Actor(&GpuActor::CreateGpuActor).SetResource("CPU", 1.0)
          .SetResource("Custom1", 1.0).Remote();
        auto a2 = ray::Actor(&GpuActor::CreateGpuActor).SetResource("CPU", 2.0)
          .SetResource("Custom2", 1.0).Remote();
        auto a3 = ray::Actor(&GpuActor::CreateGpuActor).SetResource("CPU", 3.0)
          .SetResource("Custom3", 1.0).Remote();

Note that to create these actors successfully, Ray will need to be started with
sufficient CPU resources and the relevant custom resources.

.. tip::

  Besides compute resources, you can also specify an environment for an actor to run in,
  which can include Python packages, local files, environment variables, and more---see :ref:`Runtime Environments <runtime-environments>` for details.


Terminating Actors
------------------

Automatic termination
^^^^^^^^^^^^^^^^^^^^^

.. tabbed:: Python

    Actor processes will be terminated automatically when the initial actor handle
    goes out of scope in Python. If we create an actor with ``actor_handle =
    Counter.remote()``, then when ``actor_handle`` goes out of scope and is
    destructed, the actor process will be terminated. Note that this only applies to
    the original actor handle created for the actor and not to subsequent actor
    handles created by passing the actor handle to other tasks.

.. tabbed:: Java

    Terminating an actor automatically when the initial actor handle goes out of scope hasn't been implemented in Java yet.

.. tabbed:: C++

    Terminating an actor automatically when the initial actor handle goes out of scope hasn't been implemented in C++ yet.

Manual termination within the actor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If necessary, you can manually terminate an actor from within one of the actor methods.
This will kill the actor process and release resources associated/assigned to the actor.

.. tabbed:: Python

    .. code-block:: python

        ray.actor.exit_actor()

    This approach should generally not be necessary as actors are automatically garbage
    collected. The ``ObjectRef`` resulting from the task can be waited on to wait
    for the actor to exit (calling ``ray.get()`` on it will raise a ``RayActorError``).

.. tabbed:: Java

    .. code-block:: java

        Ray.exitActor();

    Garbage collection for actors haven't been implemented yet, so this is currently the
    only way to terminate an actor gracefully. The ``ObjectRef`` resulting from the task
    can be waited on to wait for the actor to exit (calling ``ObjectRef::get`` on it will
    throw a ``RayActorException``).

.. tabbed:: C++

    .. code-block:: c++

        ray::ExitActor();

    Garbage collection for actors haven't been implemented yet, so this is currently the
    only way to terminate an actor gracefully. The ``ObjectRef`` resulting from the task
    can be waited on to wait for the actor to exit (calling ``ObjectRef::Get`` on it will
    throw a ``RayActorException``).

Note that this method of termination will wait until any previously submitted
tasks finish executing and then exit the process gracefully with sys.exit.

Manual termination via an actor handle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can terminate an actor forcefully.

.. tabbed:: Python

    .. code-block:: python

        ray.kill(actor_handle)

.. tabbed:: Java

    .. code-block:: java

        actorHandle.kill();

.. tabbed:: C++

    .. code-block:: c++

        actor_handle.Kill();

This will call the exit syscall from within the actor, causing it to exit
immediately and any pending tasks to fail.

.. tabbed:: Python

    This will not go through the normal
    Python sys.exit teardown logic, so any exit handlers installed in the actor using
    ``atexit`` will not be called.

.. tabbed:: Java

    This will not go through the normal Java System.exit teardown logic, so any
    shutdown hooks installed in the actor using ``Runtime.addShutdownHook(...)`` will
    not be called.

.. tabbed:: C++

    This will not go through the normal
    C++ std::exit teardown logic, so any exit handlers installed in the actor using
    ``std::atexit`` will not be called.

Passing Around Actor Handles
----------------------------

Actor handles can be passed into other tasks. We can define remote functions (or actor methods) that use actor handles.

.. tabbed:: Python

    .. code-block:: python

        import time

        @ray.remote
        def f(counter):
            for _ in range(1000):
                time.sleep(0.1)
                counter.increment.remote()

.. tabbed:: Java

    .. code-block:: java

        public static class MyRayApp {

          public static void foo(ActorHandle<Counter> counter) throws InterruptedException {
            for (int i = 0; i < 1000; i++) {
              TimeUnit.MILLISECONDS.sleep(100);
              counter.task(Counter::increment).remote();
            }
          }
        }

.. tabbed:: C++

    .. code-block:: c++

        void Foo(ray::ActorHandle<Counter> counter) {
            for (int i = 0; i < 1000; i++) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                counter.Task(&Counter::Increment).Remote();
            }
        }

If we instantiate an actor, we can pass the handle around to various tasks.

.. tabbed:: Python

    .. code-block:: python

        counter = Counter.remote()

        # Start some tasks that use the actor.
        [f.remote(counter) for _ in range(3)]

        # Print the counter value.
        for _ in range(10):
            time.sleep(1)
            print(ray.get(counter.get_counter.remote()))

.. tabbed:: Java

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

.. tabbed:: C++

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

Named Actors
------------

An actor can be given a unique name within their :ref:`namespace <namespaces-guide>`.
This allows you to retrieve the actor from any job in the Ray cluster.
This can be useful if you cannot directly
pass the actor handle to the task that needs it, or if you are trying to
access an actor launched by another driver.
Note that the actor will still be garbage-collected if no handles to it
exist. See :ref:`actor-lifetimes` for more details.

.. tabbed:: Python

    .. code-block:: python

        # Create an actor with a name
        counter = Counter.options(name="some_name").remote()

        ...

        # Retrieve the actor later somewhere
        counter = ray.get_actor("some_name")

.. tabbed:: Java

    .. code-block:: java

        // Create an actor with a name.
        ActorHandle<Counter> counter = Ray.actor(Counter::new).setName("some_name").remote();

        ...

        // Retrieve the actor later somewhere
        Optional<ActorHandle<Counter>> counter = Ray.getActor("some_name");
        Assert.assertTrue(counter.isPresent());

.. tabbed:: C++

    .. code-block:: c++

        // Create an actor with a globally unique name
        ActorHandle<Counter> counter = ray::Actor(CreateCounter).SetGlobalName("some_name").Remote();

        ...

        // Retrieve the actor later somewhere
        boost::optional<ray::ActorHandle<Counter>> counter = ray::GetGlobalActor("some_name");

    We also support non-global named actors in C++, which means that the actor name is only valid within the job and the actor cannot be accessed from another job

    .. code-block:: c++

        // Create an actor with a job-scope-unique name
        ActorHandle<Counter> counter = ray::Actor(CreateCounter).SetName("some_name").Remote();

        ...

        // Retrieve the actor later somewhere in the same job
        boost::optional<ray::ActorHandle<Counter>> counter = ray::GetActor("some_name");

.. note::

     Named actors are only accessible in the same namespace.

.. tabbed:: Python

    .. code-block:: python

        import ray

        @ray.remote
        class Actor:
          pass

        # driver_1.py
        # Job 1 creates an actor, "orange" in the "colors" namespace.
        ray.init(address="auto", namespace="colors")
        Actor.options(name="orange", lifetime="detached")

        # driver_2.py
        # Job 2 is now connecting to a different namespace.
        ray.init(address="auto", namespace="fruit")
        # This fails because "orange" was defined in the "colors" namespace.
        ray.get_actor("orange")

        # driver_3.py
        # Job 3 connects to the original "colors" namespace
        ray.init(address="auto", namespace="colors")
        # This returns the "orange" actor we created in the first job.
        ray.get_actor("orange")

.. tabbed:: Java

    .. code-block:: java

        import ray

        class Actor {
        }

        // Driver1.java
        // Job 1 creates an actor, "orange" in the "colors" namespace.
        System.setProperty("ray.job.namespace", "colors");
        Ray.init();
        Ray.actor(Actor::new).setName("orange").remote();

        // Driver2.java
        // Job 2 is now connecting to a different namespace.
        System.setProperty("ray.job.namespace", "fruits");
        Ray.init();
        // This fails because "orange" was defined in the "colors" namespace.
        Optional<ActorHandle<Actor>> actor = Ray.getActor("orange");
        Assert.assertFalse(actor.isPresent());  // actor.isPresent() is false.

        // Driver3.java
        System.setProperty("ray.job.namespace", "colors");
        Ray.init();
        // This returns the "orange" actor we created in the first job.
        Optional<ActorHandle<Actor>> actor = Ray.getActor("orange");
        Assert.assertTrue(actor.isPresent());  // actor.isPresent() is true.


.. _actor-lifetimes:

Actor Lifetimes
---------------

.. tabbed:: Python

    Separately, actor lifetimes can be decoupled from the job, allowing an actor to
    persist even after the driver process of the job exits.

    .. code-block:: python

        counter = Counter.options(name="CounterActor", lifetime="detached").remote()

    The CounterActor will be kept alive even after the driver running above script
    exits. Therefore it is possible to run the following script in a different
    driver:

    .. code-block:: python

        counter = ray.get_actor("CounterActor")
        print(ray.get(counter.get_counter.remote()))

    Note that the lifetime option is decoupled from the name. If we only specified
    the name without specifying ``lifetime="detached"``, then the CounterActor can
    only be retrieved as long as the original driver is still running.

.. tabbed:: Java

    Customizing lifetime of an actor hasn't been implemented in Java yet.

.. tabbed:: C++

    Customizing lifetime of an actor hasn't been implemented in C++ yet.

Actor Pool
----------

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


FAQ: Actors, Workers and Resources
----------------------------------

What's the difference between a worker and an actor?

Each "Ray worker" is a python process.

Workers are treated differently for tasks and actors. Any "Ray worker" is either 1. used to execute multiple Ray tasks or 2. is started as a dedicated Ray actor.

* **Tasks**: When Ray starts on a machine, a number of Ray workers will be started automatically (1 per CPU by default). They will be used to execute tasks (like a process pool). If you execute 8 tasks with `num_cpus=2`, and total number of CPUs is 16 (`ray.cluster_resources()["CPU"] == 16`), you will end up with 8 of your 16 workers idling.

* **Actor**: A Ray Actor is also a "Ray worker" but is instantiated at runtime (upon `actor_cls.remote()`). All of its methods will run on the same process, using the same resources (designated when defining the Actor). Note that unlike tasks, the python processes that runs Ray Actors are not reused and will be terminated when the Actor is deleted.

To maximally utilize your resources, you want to maximize the time that
your workers are working. You also want to allocate enough cluster resources
so that both all of your needed actors can run and any other tasks you
define can run. This also implies that tasks are scheduled more flexibly,
and that if you don't need the stateful part of an actor, you're mostly
better off using tasks.


Concurrency within an actor
---------------------------

.. tabbed:: Python

    Within a single actor process, it is possible to execute concurrent threads.

    Ray offers two types of concurrency within an actor:

    * :ref:`async execution <async-actors>`
    * :ref:`threading <threaded-actors>`

    See the above links for more details.

.. tabbed:: Java

    Actor-level concurrency hasn't been implemented in Java yet.

.. tabbed:: C++

    Actor-level concurrency hasn't been implemented in C++ yet.
