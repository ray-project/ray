Out-Of-Memory Prevention
========================

If application tasks or actors consume a large amount of heap space, it can cause the node to run out of memory (OOM). When that happens, the operating system will start killing worker or raylet processes, disrupting the application. OOM may also stall metrics and if this happens on the head node, it may stall the :ref:`dashboard <ray-dashboard>` or other control processes and cause the cluster to become unusable.

In this section we will go over:

- What is the memory monitor and how it works

- How to enable and configure it

- How to use the memory monitor to detect and resolve memory issues

What is the memory monitor?
---------------------------

The memory monitor is a component that runs within the :ref:`raylet <whitepaper>` process on each node. It periodically checks the memory usage, which includes the worker heap, the object store, and the raylet as described in :ref:`memory management <memory>`. If the combined usage exceeds a configurable threshold the raylet will kill a task or actor process to free up memory and prevent Ray from failing.

.. note::

    The memory monitor is in :ref:`beta <api-stability-beta>`. It is enabled by default and can be disabled by setting the environment variable ``RAY_memory_monitor_refresh_ms`` to zero when Ray starts. It is available on Linux and is tested with Ray running inside a container that is using cgroup v1. If you encounter issues when running the memory monitor outside of a container or the container is using cgroup v2, please :ref:`file an issue or post a question <oom-questions>`.

How do I configure the memory monitor?
--------------------------------------

The memory monitor is controlled by the following environment variables:

- ``RAY_memory_monitor_refresh_ms (int, defaults to 250)`` is the interval to check memory usage and kill tasks or actors if needed. It is disabled when this value is 0.

- ``RAY_memory_usage_threshold (float, defaults to 0.95)`` is the threshold when the node is beyond the memory
  capacity. If the memory usage is above this value and the free space is
  below min_memory_free_bytes then it will start killing processes to free up space. Ranges from [0, 1].

- ``RAY_task_oom_retries (int, defaults to -1):`` The number of retries for the task when
  it fails due to the process being killed by the memory monitor. By default it retries
  indefinitely. If the task is not retriable then this value is not used. This value is used
  only when the process is killed by the memory monitor, and the retry counter of the
  task or actor (:ref:`max_retries <task-fault-tolerance>` or :ref:`max_restarts <actor-fault-tolerance>`) is used when it fails 
  in other ways. If the process is killed by the operating system OOM killer it will use the task retry and not ``task_oom_retries``.
  It will retry indefinitely if this value is set to -1.

  When the task is retried due to OOM it applies a delay before re-executing the task. The delay is calculated as

  .. code-block:: bash

      delay_seconds = 2 ^ attempt

  Where the first retry will be delayed by 1 second as ``attempt`` starts from 0.
  
  There is a cap on the maximum delay, which is 60 seconds.

      
Using the Memory Monitor
------------------------

Retry policy
~~~~~~~~~~~~

When a task or actor is killed by the memory monitor, it will retry using a separate retry counter based off of ``RAY_task_oom_retries`` instead of the typical number of retries specified by :ref:`max_retries <task-fault-tolerance>` for tasks and :ref:`max_restarts <actor-fault-tolerance>` for actors. The number of memory monitor retries is the same for tasks and actors and defaults to ``15``. To override this value, the environment variable ``RAY_task_oom_retries`` should be set when starting Ray as well as the application.

Let's create an application oom.py that will trigger the out-of-memory condition.

.. literalinclude:: ../doc_code/ray_oom_prevention.py
      :language: python
      :start-after: __oom_start__
      :end-before: __oom_end__


Set ``RAY_event_stats_print_interval_ms=1000`` so it prints the worker kill summary every second, since by default it is every minute.

.. code-block:: bash

    (raylet) [2023-02-03 15:28:16,750 E 152312 152312] (raylet) node_manager.cc:3040: 1 Workers (tasks / actors) killed due to memory pressure (OOM), 0 Workers crashed due to other reasons at node (ID: 2c82620270df6b9dd7ae2791ef51ee4b5a9d5df9f795986c10dd219c, IP: 172.31.183.172) over the last time period. To see more information about the Workers killed on this node, use `ray logs raylet.out -ip 172.31.183.172`
    (raylet) 
    (raylet) Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero.
             task failed with OutOfMemoryError, which is expected
             Verify the task was indeed executed twice via ``task_oom_retry``:

.. note::

    Actors by default are not retriable since :ref:`max_restarts <actor-fault-tolerance>` defaults to 0, therefore tasks are preferred to actors when it comes to what gets killed first. Actors currently don't use ``RAY_task_oom_retries`` and instead use :ref:`max_restarts <actor-fault-tolerance>` when killed by the memory monitor.

Worker killing policy
~~~~~~~~~~~~~~~~~~~~~

The raylet prioritizes killing tasks that are retriable, i.e. when ``max_retries`` or ``max_restarts`` is > 0. This is done to minimize workload failure. It then looks for the last one to start executing and kills that worker process. It selects and kills one process at a time and waits for it to be killed before choosing another one, regardless of how frequent the monitor runs.

.. dropdown:: Example: memory monitor prefers to kill a retriable task

    Let's first start ray and specify the memory threshold.

    .. code-block:: bash

        RAY_memory_usage_threshold=0.4 ray start --head


    Let's create an application two_actors.py that submits two actors, where the first one is retriable and the second one is non-retriable.

    .. literalinclude:: ../doc_code/ray_oom_prevention.py
          :language: python
          :start-after: __two_actors_start__
          :end-before: __two_actors_end__


    Run the application to see that only the first actor was killed.

    .. code-block:: bash

        $ python two_actors.py
        
        first actor was killed by memory monitor
        finished second actor


Addressing memory issues
------------------------

When the application fails due to OOM, consider reducing the memory usage of the tasks and actors, increasing the memory capacity of the node, or :ref:`limit the number of concurrently running tasks <core-patterns-limit-running-tasks>`.


.. _oom-questions:

Questions or Issues?
--------------------

.. include:: /_includes/_help.rst
