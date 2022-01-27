.. _core-walkthrough:

Ray Core Walkthrough
====================

This walkthrough will overview the core concepts of Ray:

1. Starting Ray
2. Using remote functions (tasks)
3. Fetching results (object refs)
4. Using remote classes (actors)

With Ray, your code will work on a single machine and can be easily scaled to large cluster.

Java demo code in this documentation can be found `here <https://github.com/ray-project/ray/blob/master/java/test/src/main/java/io/ray/docdemo/WalkthroughDemo.java>`__.

Installation
------------

.. tabbed:: Python

    To run this walkthrough, install Ray with ``pip install -U ray``. For the latest wheels (for a snapshot of ``master``), you can use these instructions at :ref:`install-nightlies`.

.. tabbed:: Java

    To run this walkthrough, add `Ray API <https://mvnrepository.com/artifact/io.ray/ray-api>`_ and `Ray Runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`_ as dependencies. Snapshot versions can be found in `sonatype repository <https://oss.sonatype.org/#nexus-search;quick~io.ray>`_.

    Note: To run your Ray Java application, you need to install Ray Python with `pip install -U ray` first. (For Ray Java snapshot versions, install nightly Ray Python wheels.) The versions of Ray Java and Ray Python must match.

.. tabbed:: C++

    To run this walkthrough, install Ray with ``pip install -U ray[cpp]``. For the latest wheels (for a snapshot of ``master``), you can use these instructions at :ref:`install-nightlies`.

Starting Ray
------------

You can start Ray on a single machine by adding this to your code.

.. note::

  In recent versions of Ray (>=1.5), ``ray.init()`` will automatically be called on the first use of a Ray remote API.

.. tabbed:: Python

    .. code-block:: python

        import ray

        # Start Ray. If you're connecting to an existing cluster, you would use
        # ray.init(address=<cluster-address>) instead.
        ray.init()

        ...

.. tabbed:: Java

    .. code-block:: java

        import io.ray.api.Ray;

        public class MyRayApp {

            public static void main(String[] args) {
                // Start Ray runtime. If you're connecting to an existing cluster, you can set
                // the `-Dray.address=<cluster-address>` java system property.
                Ray.init();
                ...
            }
        }

.. tabbed:: C++

    .. code-block:: c++

        // Run `ray cpp --show-library-path` to find headers and libraries.
        #include <ray/api.h>

        int main(int argc, char **argv) {
            // Start Ray runtime. If you're connecting to an existing cluster, you can set
            // the `RAY_ADDRESS` env var.
            ray::Init();
            ...
        }


Ray will then be able to utilize all cores of your machine. Find out how to configure the number of cores Ray will use at :ref:`configuring-ray`.

To start a multi-node Ray cluster, see the :ref:`cluster setup page <cluster-index>`.

.. _ray-remote-functions:

Remote functions (Tasks)
------------------------

Ray enables arbitrary functions to be executed asynchronously. These asynchronous Ray functions are called "remote functions". Here is an example.

.. tabbed:: Python

    .. code:: python

      # A regular Python function.
      def my_function():
          return 1

      # By adding the `@ray.remote` decorator, a regular Python function
      # becomes a Ray remote function.
      @ray.remote
      def my_function():
          return 1

      # To invoke this remote function, use the `remote` method.
      # This will immediately return an object ref (a future) and then create
      # a task that will be executed on a worker process.
      obj_ref = my_function.remote()

      # The result can be retrieved with ``ray.get``.
      assert ray.get(obj_ref) == 1

      @ray.remote
      def slow_function():
          time.sleep(10)
          return 1

      # Invocations of Ray remote functions happen in parallel.
      # All computation is performed in the background, driven by Ray's internal event loop.
      for _ in range(4):
          # This doesn't block.
          slow_function.remote()

    See the `ray.remote package reference <package-ref.html>`__ page for specific documentation on how to use ``ray.remote``.

.. tabbed:: Java

    .. code-block:: java

      public class MyRayApp {
        // A regular Java static method.
        public static int myFunction() {
          return 1;
        }
      }

      // Invoke the above method as a Ray remote function.
      // This will immediately return an object ref (a future) and then create
      // a task that will be executed on a worker process.
      ObjectRef<Integer> res = Ray.task(MyRayApp::myFunction).remote();

      // The result can be retrieved with ``ObjectRef::get``.
      Assert.assertTrue(res.get() == 1);

      public class MyRayApp {
        public static int slowFunction() throws InterruptedException {
          TimeUnit.SECONDS.sleep(10);
          return 1;
        }
      }

      // Invocations of Ray remote functions happen in parallel.
      // All computation is performed in the background, driven by Ray's internal event loop.
      for(int i = 0; i < 4; i++) {
        // This doesn't block.
        Ray.task(MyRayApp::slowFunction).remote();
      }

.. tabbed:: C++

    .. code-block:: c++

      // A regular C++ function.
      int MyFunction() {
        return 1;
      }
      // Register as a remote function by `RAY_REMOTE`.
      RAY_REMOTE(MyFunction);

      // Invoke the above method as a Ray remote function.
      // This will immediately return an object ref (a future) and then create
      // a task that will be executed on a worker process.
      auto res = ray::Task(MyFunction).Remote();

      // The result can be retrieved with ``ray::ObjectRef::Get``.
      assert(*res.Get() == 1);

      int SlowFunction() {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        return 1;
      }
      RAY_REMOTE(SlowFunction);

      // Invocations of Ray remote functions happen in parallel.
      // All computation is performed in the background, driven by Ray's internal event loop.
      for(int i = 0; i < 4; i++) {
        // This doesn't block.
        ray::Task(SlowFunction).Remote();
      }

.. _ray-object-refs:

Passing object refs to remote functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Object refs** can also be passed into remote functions. When the function actually gets executed, **the argument will be a retrieved as a regular object**. For example, take this function:

.. tabbed:: Python

    .. code-block:: python

        @ray.remote
        def function_with_an_argument(value):
            return value + 1


        obj_ref1 = my_function.remote()
        assert ray.get(obj_ref1) == 1

        # You can pass an object ref as an argument to another Ray remote function.
        obj_ref2 = function_with_an_argument.remote(obj_ref1)
        assert ray.get(obj_ref2) == 2

.. tabbed:: Java

    .. code-block:: java

        public class MyRayApp {
            public static int functionWithAnArgument(int value) {
                return value + 1;
            }
        }

        ObjectRef<Integer> objRef1 = Ray.task(MyRayApp::myFunction).remote();
        Assert.assertTrue(objRef1.get() == 1);

        // You can pass an object ref as an argument to another Ray remote function.
        ObjectRef<Integer> objRef2 = Ray.task(MyRayApp::functionWithAnArgument, objRef1).remote();
        Assert.assertTrue(objRef2.get() == 2);

.. tabbed:: C++

    .. code-block:: c++

        static int FunctionWithAnArgument(int value) {
            return value + 1;
        }
        RAY_REMOTE(FunctionWithAnArgument);

        auto obj_ref1 = ray::Task(MyFunction).Remote();
        assert(*obj_ref1.Get() == 1);

        // You can pass an object ref as an argument to another Ray remote function.
        auto obj_ref2 = ray::Task(FunctionWithAnArgument).Remote(obj_ref1);
        assert(*obj_ref2.Get() == 2);

Note the following behaviors:

  -  The second task will not be executed until the first task has finished
     executing because the second task depends on the output of the first task.
  -  If the two tasks are scheduled on different machines, the output of the
     first task (the value corresponding to ``obj_ref1/objRef1``) will be sent over the
     network to the machine where the second task is scheduled.

.. _resource-requirements:

Specifying required resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Oftentimes, you may want to specify a task's resource requirements (for example
one task may require a GPU). Ray will automatically
detect the available GPUs and CPUs on the machine. However, you can override
this default behavior by passing in specific resources.

.. tabbed:: Python

    .. code-block:: python

        ray.init(num_cpus=8, num_gpus=4, resources={'Custom': 2})

.. tabbed:: Java

    Set Java system property: ``-Dray.resources=CPU:8,GPU:4,Custom:2``.


.. tabbed:: C++
    .. code-block:: c++

        RayConfig config;
        config.num_cpus = 8;
        config.num_gpus = 4;
        config.resources = {{"Custom", 2}};
        ray::Init(config);

Ray also allows specifying a task's resources requirements (e.g., CPU, GPU, and custom resources).
The task will only run on a machine if there are enough resources
available to execute the task.

.. tabbed:: Python

    .. code-block:: python

        # Specify required resources.
        @ray.remote(num_cpus=4, num_gpus=2)
        def my_function():
            return 1

.. tabbed:: Java

    .. code-block:: java

        // Specify required resources.
        Ray.task(MyRayApp::myFunction).setResource("CPU", 1.0).setResource("GPU", 4.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Specify required resources.
        ray::Task(MyFunction).SetResource("CPU", 1.0).SetResource("GPU", 4.0).Remote();

.. note::

    * If you do not specify any resources, the default is 1 CPU resource and
      no other resources.
    * If specifying CPUs, Ray does not enforce isolation (i.e., your task is
      expected to honor its request).
    * If specifying GPUs, Ray does provide isolation in forms of visible devices
      (setting the environment variable ``CUDA_VISIBLE_DEVICES``), but it is the
      task's responsibility to actually use the GPUs (e.g., through a deep
      learning framework like TensorFlow or PyTorch).

The resource requirements of a task have implications for the Ray's scheduling
concurrency. In particular, the sum of the resource requirements of all of the
concurrently executing tasks on a given node cannot exceed the node's total
resources.

Below are more examples of resource specifications:

.. tabbed:: Python

    .. code-block:: python

        # Ray also supports fractional resource requirements.
        @ray.remote(num_gpus=0.5)
        def h():
            return 1

        # Ray support custom resources too.
        @ray.remote(resources={'Custom': 1})
        def f():
            return 1

.. tabbed:: Java

    .. code-block:: java

        // Ray aslo supports fractional and custom resources.
        Ray.task(MyRayApp::myFunction).setResource("GPU", 0.5).setResource("Custom", 1.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Ray aslo supports fractional and custom resources.
        ray::Task(MyFunction).SetResource("GPU", 0.5).SetResource("Custom", 1.0).Remote();

.. tip::

  Besides compute resources, you can also specify an environment for a task to run in,
  which can include Python packages, local files, environment variables, and more---see :ref:`Runtime Environments <runtime-environments>` for details.

Multiple returns
~~~~~~~~~~~~~~~~

.. tabbed:: Python

    Python remote functions can return multiple object refs.

    .. code-block:: python

      @ray.remote(num_returns=3)
      def return_multiple():
          return 1, 2, 3

      a, b, c = return_multiple.remote()

.. tabbed:: Java

    Java remote functions doesn't support returning multiple objects.

.. tabbed:: C++

    C++ remote functions doesn't support returning multiple objects.

Cancelling tasks
~~~~~~~~~~~~~~~~

.. tabbed:: Python

    Remote functions can be canceled by calling ``ray.cancel`` (:ref:`docstring <ray-cancel-ref>`) on the returned Object ref. Remote actor functions can be stopped by killing the actor using the ``ray.kill`` interface.

    .. code-block:: python

      @ray.remote
      def blocking_operation():
          time.sleep(10e6)

      obj_ref = blocking_operation.remote()
      ray.cancel(obj_ref)

      from ray.exceptions import TaskCancelledError

      try:
          ray.get(obj_ref)
      except TaskCancelledError:
          print("Object reference was cancelled.")

.. tabbed:: Java

    Task cancellation hasn't been implemented in Java yet.

.. tabbed:: C++

    Task cancellation hasn't been implemented in C++ yet.

.. _objects-in-ray:

Objects in Ray
--------------

In Ray, we can create and compute on objects. We refer to these objects as **remote objects**, and we use **object refs** to refer to them. Remote objects are stored in `shared-memory <https://en.wikipedia.org/wiki/Shared_memory>`__ **object stores**, and there is one object store per node in the cluster. In the cluster setting, we may not actually know which machine each object lives on.

An **object ref** is essentially a unique ID that can be used to refer to a
remote object. If you're familiar with futures, our object refs are conceptually
similar.

Object refs can be created in multiple ways.

  1. They are returned by remote function calls.
  2. They are returned by ``put`` (:ref:`docstring <ray-put-ref>`).

.. tabbed:: Python
  .. code-block:: python

    # Put an object in Ray's object store.
    y = 1
    object_ref = ray.put(y)

.. tabbed:: Java
  .. code-block:: java

    // Put an object in Ray's object store.
    int y = 1;
    ObjectRef<Integer> objectRef = Ray.put(y);

.. tabbed:: C++
  .. code-block:: c++

    // Put an object in Ray's object store.
    int y = 1;
    ray::ObjectRef<int> object_ref = ray::Put(y);

.. note::

    Remote objects are immutable. That is, their values cannot be changed after
    creation. This allows remote objects to be replicated in multiple object
    stores without needing to synchronize the copies.


Fetching Results
----------------

You can use the ``get`` method (:ref:`docstring <ray-get-ref>`) to fetch the result of a remote object from an object ref.
If the current node's object store does not contain the object, the object is downloaded.

.. tabbed:: Python

    If the object is a `numpy array <https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html>`__
    or a collection of numpy arrays, the ``get`` call is zero-copy and returns arrays backed by shared object store memory.
    Otherwise, we deserialize the object data into a Python object.

    .. code-block:: python

      # Get the value of one object ref.
      obj_ref = ray.put(1)
      assert ray.get(obj_ref) == 1

      # Get the values of multiple object refs in parallel.
      assert ray.get([ray.put(i) for i in range(3)]) == [0, 1, 2]

      # You can also set a timeout to return early from a ``get`` that's blocking for too long.
      from ray.exceptions import GetTimeoutError

      @ray.remote
      def long_running_function():
          time.sleep(8)

      obj_ref = long_running_function.remote()
      try:
          ray.get(obj_ref, timeout=4)
      except GetTimeoutError:
          print("`get` timed out.")

.. tabbed:: Java

    .. code-block:: java

      // Get the value of one object ref.
      ObjectRef<Integer> objRef = Ray.put(1);
      Assert.assertTrue(objRef.get() == 1);
      // You can also set a timeout(ms) to return early from a ``get`` that's blocking for too long.
      Assert.assertTrue(objRef.get(1000) == 1);

      // Get the values of multiple object refs in parallel.
      List<ObjectRef<Integer>> objectRefs = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
	objectRefs.add(Ray.put(i));
      }
      List<Integer> results = Ray.get(objectRefs);
      Assert.assertEquals(results, ImmutableList.of(0, 1, 2));

      // Ray.get timeout example: Ray.get will throw an RayTimeoutException if time out.
      public class MyRayApp {
        public static int slowFunction() throws InterruptedException {
          TimeUnit.SECONDS.sleep(10);
          return 1;
        }
      }
      Assert.assertThrows(RayTimeoutException.class, 
        () -> Ray.get(Ray.task(MyRayApp::slowFunction).remote(), 3000));

.. tabbed:: C++

    .. code-block:: c++

      // Get the value of one object ref.
      ray::ObjectRef<int> obj_ref = ray::Put(1);
      assert(*obj_ref.Get() == 1);

      // Get the values of multiple object refs in parallel.
      std::vector<ray::ObjectRef<int>> obj_refs;
      for (int i = 0; i < 3; i++) {
        obj_refs.emplace_back(ray::Put(i));
      }
      auto results = ray::Get(obj_refs);
      assert(results.size() == 3);
      assert(*results[0] == 0);
      assert(*results[1] == 1);
      assert(*results[2] == 2);

After launching a number of tasks, you may want to know which ones have
finished executing. This can be done with ``wait`` (:ref:`ray-wait-ref`). The function
works as follows.

.. tabbed:: Python
  .. code-block:: python

    ready_refs, remaining_refs = ray.wait(object_refs, num_returns=1, timeout=None)

.. tabbed:: Java
  .. code-block:: java

    WaitResult<Integer> waitResult = Ray.wait(objectRefs, /*num_returns=*/0, /*timeoutMs=*/1000);
    System.out.println(waitResult.getReady());  // List of ready objects.
    System.out.println(waitResult.getUnready());  // list of unready objects.

.. tabbed:: C++
  .. code-block:: c++

    ray::WaitResult<int> wait_result = ray::Wait(object_refs, /*num_objects=*/0, /*timeout_ms=*/1000);

Object Spilling
---------------

When the object store gets full, objects will be `spilled to disk <memory-management.html#object-spilling>`__.
This feature is available in Ray 1.3+.

Remote Classes (Actors)
-----------------------

Actors extend the Ray API from functions (tasks) to classes. An actor is essentially a stateful worker.

.. tabbed:: Python

    The ``ray.remote`` decorator indicates that instances of the ``Counter`` class will be actors. Each actor runs in its own Python process.

    .. code-block:: python

      @ray.remote
      class Counter(object):
          def __init__(self):
              self.value = 0

          def increment(self):
              self.value += 1
              return self.value

      # Create an actor from this class.
      counter = Counter.remote()

.. tabbed:: Java

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

.. tabbed:: C++

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

Specifying required resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can specify resource requirements in actors too (see the `Actors section
<actors.html>`__ for more details.)

.. tabbed:: Python

    .. code-block:: python

        # Specify required resources for an actor.
        @ray.remote(num_cpus=2, num_gpus=0.5)
        class Actor(object):
            pass

.. tabbed:: Java

    .. code-block:: java

        // Specify required resources for an actor.
        Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Specify required resources for an actor.
        ray::Actor(CreateCounter).SetResource("CPU", 2.0).SetResource("GPU", 0.5).Remote();


Calling the actor
~~~~~~~~~~~~~~~~~

We can interact with the actor by calling its methods with the ``remote``
operator. We can then call ``get`` on the object ref to retrieve the actual
value.

.. tabbed:: Python

    .. code-block:: python

        # Call the actor.
        obj_ref = counter.increment.remote()
        assert ray.get(obj_ref) == 1

.. tabbed:: Java

    .. code-block:: java

        // Call the actor.
        ObjectRef<Integer> objectRef = counter.task(&Counter::increment).remote();
        Assert.assertTrue(objectRef.get() == 1);

.. tabbed:: C++

    .. code-block:: c++

        // Call the actor.
        auto object_ref = counter.Task(&Counter::increment).Remote();
        assert(*object_ref.Get() == 1);

Methods called on different actors can execute in parallel, and methods called on the same actor are executed serially in the order that they are called. Methods on the same actor will share state with one another, as shown below.

.. tabbed:: Python

    .. code-block:: python

        # Create ten Counter actors.
        counters = [Counter.remote() for _ in range(10)]

        # Increment each Counter once and get the results. These tasks all happen in
        # parallel.
        results = ray.get([c.increment.remote() for c in counters])
        print(results)  # prints [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

        # Increment the first Counter five times. These tasks are executed serially
        # and share state.
        results = ray.get([counters[0].increment.remote() for _ in range(5)])
        print(results)  # prints [2, 3, 4, 5, 6]

.. tabbed:: Java

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

.. tabbed:: C++

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

To learn more about Ray Actors, see the `Actors section <actors.html>`__.
