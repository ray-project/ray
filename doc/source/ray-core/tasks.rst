.. _ray-remote-functions:

Tasks
=====

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

Waiting for Ready Results
~~~~~~~~~~~~~~~~~~~~~~~~~

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

More about Ray Tasks
--------------------

.. toctree::
    :maxdepth: 1

    tasks/nested-tasks.rst
    tasks/using-ray-with-gpus.rst
    tasks/fault-tolerance.rst
    tasks/patterns/index.rst
