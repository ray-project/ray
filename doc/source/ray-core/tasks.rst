.. _ray-remote-functions:

Tasks
=====

Ray enables arbitrary functions to be executed asynchronously on separate Python workers. These asynchronous Ray functions are called **Ray tasks** or **Ray remote functions**. Here is an example.

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py

    See the `ray.remote package reference <package-ref.html>`__ page for specific documentation on how to use ``ray.remote``.

.. tabbed:: Java

    .. code-block:: java

      public class MyRayApp {
        // A regular Java static method.
        public static int myFunction() {
          return 1;
        }
      }

      // Invoke the above method as a Ray task.
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

      // Invocations of Ray tasks happen in parallel.
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

      // Invoke the above method as a Ray task.
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

      // Invocations of Ray tasks happen in parallel.
      // All computation is performed in the background, driven by Ray's internal event loop.
      for(int i = 0; i < 4; i++) {
        // This doesn't block.
        ray::Task(SlowFunction).Remote();
      }

.. _ray-object-refs:

Passing object refs to Ray tasks 
---------------------------------------

In addition to values, `Object refs <objects.html>`__ can also be passed into remote functions. When the function actually gets executed, **the argument will be automatically dereferenced as the underlying value**. For example, take this function:

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks_and_objects.py

.. tabbed:: Java

    .. code-block:: java

        public class MyRayApp {
            public static int functionWithAnArgument(int value) {
                return value + 1;
            }
        }

        ObjectRef<Integer> objRef1 = Ray.task(MyRayApp::myFunction).remote();
        Assert.assertTrue(objRef1.get() == 1);

        // You can pass an object ref as an argument to another Ray task.
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

        // You can pass an object ref as an argument to another Ray task.
        auto obj_ref2 = ray::Task(FunctionWithAnArgument).Remote(obj_ref1);
        assert(*obj_ref2.Get() == 2);

Note the following behaviors:

  -  As the second task depends on the output of the first task, Ray will not execute the second task until the first task has finished.
  -  If the two tasks are scheduled on different machines, the output of the
     first task (the value corresponding to ``obj_ref1/objRef1``) will be sent over the
     network to the machine where the second task is scheduled.

Waiting for Partial Results
---------------------------

Calling **ray.get** on Ray remote task returns will block until the task finished execution. After launching a number of tasks, you may want to know which ones have
finished executing without blocking on all of them. This could be achieved by (:ref:`ray-wait-ref`). The function
works as follows.

.. tabbed:: Python

  .. code-block:: python

    # Return as soon as one of the tasks finished execution.
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
----------------

.. tabbed:: Python

    Ray tasks can return multiple object refs.

    .. code-block:: python

      @ray.remote(num_returns=3)
      def return_multiple():
          return 0, 1, 2

      a, b, c = return_multiple.remote()

    For tasks that return multiple objects, Ray also supports remote generators that allow a task to return one object at a time to reduce memory usage at the worker. See the :ref:`user guide <generators>` for more details on use cases.

    .. code-block:: python

      @ray.remote(num_returns=3)
      def return_multiple_as_generator():
          for i in range(3):
              yield i

      # NOTE: Similar to normal functions, these objects will not be available
      # until the full task is complete and all returns have been generated.
      a, b, c = return_multiple_as_generator.remote()


.. tabbed:: Java

    Java remote functions doesn't support returning multiple objects.

.. tabbed:: C++

    C++ remote functions doesn't support returning multiple objects.


Cancelling tasks
----------------

.. tabbed:: Python

    Remote functions can be canceled by calling ``ray.cancel`` (:ref:`docstring <ray-cancel-ref>`) on the returned Object ref.

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

    tasks/resources.rst
    tasks/using-ray-with-gpus.rst
    tasks/nested-tasks.rst
    tasks/fault-tolerance.rst
    tasks/scheduling.rst
    tasks/patterns/index.rst
