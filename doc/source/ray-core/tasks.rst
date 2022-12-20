.. _ray-remote-functions:

Tasks
=====

Ray enables arbitrary functions to be executed asynchronously on separate Python workers. Such functions are called **Ray remote functions** and their asynchronous invocations are called **Ray tasks**. Here is an example.

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __tasks_start__
        :end-before: __tasks_end__

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

      // Ray tasks are executed in parallel.
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

      // Ray tasks are executed in parallel.
      // All computation is performed in the background, driven by Ray's internal event loop.
      for(int i = 0; i < 4; i++) {
        // This doesn't block.
        ray::Task(SlowFunction).Remote();
      }


Specifying required resources
-----------------------------

You can specify resource requirements in tasks (see :ref:`resource-requirements` for more details.)

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __resource_start__
        :end-before: __resource_end__

.. tabbed:: Java

    .. code-block:: java

        // Specify required resources.
        Ray.task(MyRayApp::myFunction).setResource("CPU", 4.0).setResource("GPU", 2.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Specify required resources.
        ray::Task(MyFunction).SetResource("CPU", 4.0).SetResource("GPU", 2.0).Remote();

.. _ray-object-refs:

Passing object refs to Ray tasks
---------------------------------------

In addition to values, `Object refs <objects.html>`__ can also be passed into remote functions. When the task gets executed, inside the function body **the argument will be the underlying value**. For example, take this function:

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __pass_by_ref_start__
        :end-before: __pass_by_ref_end__

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

Calling **ray.get** on Ray task results will block until the task finished execution. After launching a number of tasks, you may want to know which ones have
finished executing without blocking on all of them. This could be achieved by (:ref:`ray-wait-ref`). The function
works as follows.

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __wait_start__
        :end-before: __wait_end__

.. tabbed:: Java

  .. code-block:: java

    WaitResult<Integer> waitResult = Ray.wait(objectRefs, /*num_returns=*/0, /*timeoutMs=*/1000);
    System.out.println(waitResult.getReady());  // List of ready objects.
    System.out.println(waitResult.getUnready());  // list of unready objects.

.. tabbed:: C++

  .. code-block:: c++

    ray::WaitResult<int> wait_result = ray::Wait(object_refs, /*num_objects=*/0, /*timeout_ms=*/1000);

.. _ray-task-returns:

Multiple returns
----------------

By default, a Ray task only returns a single Object Ref. However, you can configure Ray tasks to return multiple Object Refs, by setting the ``num_returns`` option.

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __multiple_returns_start__
        :end-before: __multiple_returns_end__

For tasks that return multiple objects, Ray also supports remote generators that allow a task to return one object at a time to reduce memory usage at the worker. Ray also supports an option to set the number of return values dynamically, which can be useful when the task caller does not know how many return values to expect. See the :ref:`user guide <generators>` for more details on use cases.

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __generator_start__
        :end-before: __generator_end__


Cancelling tasks
----------------

Ray tasks can be canceled by calling ``ray.cancel`` (:ref:`docstring <ray-cancel-ref>`) on the returned Object ref.

.. tabbed:: Python

    .. literalinclude:: doc_code/tasks.py
        :language: python
        :start-after: __cancel_start__
        :end-before: __cancel_end__


Scheduling
----------
For each task, Ray will choose a node to run it
and the scheduling decision is based on a few factors like
:ref:`the task's resource requirements <ray-scheduling-resources>`,
:ref:`the specified scheduling strategy <ray-scheduling-strategies>`
and :ref:`locations of task arguments <ray-scheduling-locality>`.
See :ref:`Ray scheduling <ray-scheduling>` for more details.


More about Ray Tasks
--------------------

.. toctree::
    :maxdepth: 1

    tasks/nested-tasks.rst
    tasks/generators.rst
    tasks/fault-tolerance.rst
