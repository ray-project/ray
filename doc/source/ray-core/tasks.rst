.. _ray-remote-functions:

Tasks
=====

Ray enables asynchronous execution of arbitrary functions on separate Python workers. These functions are **Ray remote functions** and their asynchronous invocations are **Ray tasks**. The following is an example.

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __tasks_start__
    :end-before: __tasks_end__

See the :func:`ray.remote<ray.remote>` API for more details.

Use `ray summary tasks` from the :ref:`State API <state-api-overview-ref>`  to see running and finished tasks and count:

.. code-block:: bash

  # This API is only available when you download Ray with `pip install "ray[default]"`.
  ray summary tasks


.. code-block:: bash

  ======== Tasks Summary: 2023-05-26 11:09:32.092546 ========
  Stats:
  ------------------------------------
  total_actor_scheduled: 0
  total_actor_tasks: 0
  total_tasks: 5


  Table (group by func_name):
  ------------------------------------
      FUNC_OR_CLASS_NAME    STATE_COUNTS    TYPE
  0   slow_function         RUNNING: 4      NORMAL_TASK
  1   my_function           FINISHED: 1     NORMAL_TASK

Specifying required resources
-----------------------------

You can specify resource requirements in tasks. See 
:ref:`resource-requirements` for more details.

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __resource_start__
    :end-before: __resource_end__

.. _ray-object-refs:

Passing object refs to Ray tasks
--------------------------------

In addition to values, you can also pass `object refs <objects.html>`__ into
remote functions. When Ray executes the task, **the argument is the
underlying value** inside the function body. Consider this example:

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __pass_by_ref_start__
    :end-before: __pass_by_ref_end__

Note the following behaviors:

* As the second task depends on the output of the first task, Ray doesn't
  execute the second task until the first task has finished.
* If Ray scheduled the two tasks on different machines, Ray sends the output of the
  first task, the value corresponding to ``obj_ref1/objRef1``, over the
  network to the machine where the second task is scheduled.

Waiting for partial results
---------------------------

Ray blocks `ray.get` calls on Ray task results until the task finishes executing. After
launching a number of tasks, you may want to know which ones have
finished executing without blocking on all of them. In this case, use
:func:`ray.wait() <ray.wait>`. The function works as follows.

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __wait_start__
    :end-before: __wait_end__

Generators
----------
Ray is compatible with Python generator syntax. See :ref:`Ray Generators <generators>`
for more details.

.. _ray-task-returns:

Multiple returns
----------------

By default, a Ray task only returns a single object ref. However, you can configure
Ray tasks to return multiple object refs, by setting the ``num_returns`` option.

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __multiple_returns_start__
    :end-before: __multiple_returns_end__

For tasks that return multiple objects, Ray also supports remote generators that
allow a task to return one object at a time to reduce memory usage at the worker.
Ray also supports an option to set the number of return values dynamically, which
can be useful when the task caller doesn't know how many return values to expect.
See the :ref:`user guide <generators>` for more details on use cases.

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __generator_start__
    :end-before: __generator_end__

.. _ray-task-cancel:

Cancelling tasks
----------------

Cancel Ray tasks by calling :func:`ray.cancel() <ray.cancel>` on the returned object ref.

.. literalinclude:: doc_code/tasks.py
    :language: python
    :start-after: __cancel_start__
    :end-before: __cancel_end__


Scheduling
----------

For each task, Ray chooses a node to run it
and bases the scheduling decision on a few factors like:

- :ref:`the task's resource requirements <ray-scheduling-resources>`
- :ref:`the specified scheduling strategy <ray-scheduling-strategies>`
- :ref:`locations of task arguments <ray-scheduling-locality>`

See :ref:`Ray scheduling <ray-scheduling>` for more details.

Fault tolerance
---------------

By default, Ray :ref:`retries <task-retries>` failed tasks
due to system failures and specified application-level failures.
You can change this behavior by setting
``max_retries`` and ``retry_exceptions`` options
in :func:`ray.remote() <ray.remote>` and :meth:`.options() <ray.remote_function.RemoteFunction.options>`.
See :ref:`Ray fault tolerance <fault-tolerance>` for more details.

Task events
-----------

By default, Ray traces the execution of tasks, reporting task status events and profiling events
that the Ray Dashboard and :ref:`State API <state-api-overview-ref>` use.

You can change this behavior by setting ``enable_task_events`` options in
:func:`ray.remote() <ray.remote>` and
:meth:`.options() <ray.remote_function.RemoteFunction.options>`
to turn off task events, which reduces the overhead of task execution, and the
amount of data the task sends to the Ray Dashboard.
Nested tasks don't inherit the task events settings from the parent task. 
You need to set the task events settings for each task separately.



More about Ray tasks
--------------------

.. toctree::
    :maxdepth: 1

    tasks/nested-tasks.rst
    tasks/generators.rst
