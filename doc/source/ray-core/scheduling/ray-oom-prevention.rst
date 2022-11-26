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

- ``RAY_min_memory_free_bytes (int, defaults to 512 MiB)`` is the minimum amount of free space. If the memory usage is above
  ``memory_usage_threshold_fraction`` and the free space is below this value then it
  will start killing processes to free up space. This setting is unused if it is set to -1.

  This value is useful for larger hosts where the ``memory_usage_threshold_fraction`` could
  represent a large chunk of memory, e.g. a host with 64GB of memory and a 0.9 threshold
  means 6.4 GB of the memory will not be usable.

- ``RAY_task_oom_retries (int, defaults to 15):`` The number of retries for the task or actor when
  it fails due to the process being killed by the memory monitor.
  If the task or actor is not retriable then this value is zero. This value is used
  only when the process is killed by the memory monitor, and the retry counter of the
  task or actor is used when it fails in other ways. If the process is killed by the operating system OOM killer it will use the task retry and not this value.
  Infinite retries is not supported.

Enabling the memory monitor
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Enable the memory monitor by setting the environment variable when Ray starts.

.. code-block:: bash

    RAY_memory_monitor_interval_ms=100 ray start --head

Check the logs to see the monitor is now running:

.. code-block:: bash

    grep memory_monitor.cc /tmp/ray/session_latest/logs/raylet.out

Which should print

.. code-block:: bash

    (raylet) memory_monitor.cc MemoryMonitor initialized with usage threshold at 34664513536 bytes (0.97 system memory), total system memory bytes: 35738255360

If the memory monitor is not running (the default) it will print something like this:

.. code-block:: bash

    (raylet) memory_monitor.cc: MemoryMonitor disabled. Specify `memory_monitor_interval_ms` > 0 to enable the monitor.

Memory usage threshold
~~~~~~~~~~~~~~~~~~~~~~

The memory usage threshold is used by the memory monitor to determine when it should start killing processes to free up memory. The threshold is controlled by the two environment variables:

- ``RAY_memory_usage_threshold_fraction`` (default: 0.98)
- ``RAY_min_memory_free_bytes`` (default: 512 MiB)

When the node starts it computes the usage threshold as follows:

.. code-block:: bash

    usage_threshold = max(system_memory * RAY_memory_usage_threshold_fraction, system_memory - RAY_min_memory_free_bytes)

``RAY_min_memory_free_bytes`` can be disabled by setting its value to -1. In that case it only uses ``RAY_memory_usage_threshold_fraction`` to determine the usage threshold.


.. dropdown:: Example: Utilizing the thresholds

    Let's walk through an example of configuring the above threshold. Here we set the memory threshold to be 0.9 of system memory:

    .. code-block:: bash

        RAY_memory_monitor_interval_ms=100 RAY_memory_usage_threshold_fraction=0.9 RAY_min_memory_free_bytes=-1 ray start --head

    For a node with ~33 GiB of RAM the raylet log prints:

    .. code-block:: bash

        $ grep memory_monitor.cc /tmp/ray/session_latest/logs/raylet.out

        (raylet) memory_monitor.cc: MemoryMonitor initialized with usage threshold at 32164429824 bytes (0.90 system memory), total system memory bytes: 35738255360

    On the other hand, if we set ``RAY_min_memory_free_bytes`` to a low value it will limit the amount of free memory reserved by the memory monitor. This helps limit the free memory for a node with large amounts of RAM.

    .. code-block:: bash

        RAY_memory_monitor_interval_ms=100 RAY_memory_usage_threshold_fraction=0.9 RAY_min_memory_free_bytes=1000000000 ray start --head

    For a node with ~33 GiB of RAM it should have a threshold close to 32GiB by leaving only 1000000000 bytes of free memory.

    .. code-block:: bash

        $ grep memory_monitor.cc /tmp/ray/session_latest/logs/raylet.out

        (raylet) memory_monitor.cc: MemoryMonitor initialized with usage threshold at 34738255360 bytes (0.97 system memory), total system memory bytes: 35738255360

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


To speed up the example, set ``RAY_task_oom_retries=1`` on the application so the task will only retry once if it is killed by the memory monitor. Also set ``RAY_event_stats_print_interval_ms=1000`` so it prints the worker kill summary, which by default is every minute.

.. code-block:: bash

    RAY_event_stats_print_interval_ms=1000 RAY_task_oom_retries=1 python oom.py

    2022-11-17 09:16:40,792 INFO worker.py:1534 -- Started a local Ray instance. View the dashboard at http://127.0.0.1:8265

    (raylet) [2022-11-17 09:16:52,264 E 90996 90996] (raylet) node_manager.cc:3096: 1 Workers (tasks / actors) killed due to memory pressure (OOM), 0 Workers crashed due to other reasons at node (ID: 90efe99b630d4b1f6ac1504df64764732d555b526049638f9d86552f, IP: 172.17.0.2) over the last time period. To see more information about the Workers killed on this node, use `ray logs raylet.out -ip 172.17.0.2`
    (raylet) 
    (raylet) Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero.

    (raylet) [2022-11-17 09:17:03,461 E 90996 90996] (raylet) node_manager.cc:3096: 1 Workers (tasks / actors) killed due to memory pressure (OOM), 0 Workers crashed due to other reasons at node (ID: 90efe99b630d4b1f6ac1504df64764732d555b526049638f9d86552f, IP: 172.17.0.2) over the last time period. To see more information about the Workers killed on this node, use `ray logs raylet.out -ip 172.17.0.2`
    (raylet) 
    (raylet) Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero.

    Traceback (most recent call last):
      File "oom.py", line 11, in <module>
        ray.get(allocate_memory.remote())
      File "/home/ray/github/rayclarng/ray/python/ray/_private/client_mode_hook.py", line 105, in wrapper
        return func(*args, **kwargs)
      File "/home/ray/github/rayclarng/ray/python/ray/_private/worker.py", line 2310, in get
        raise value
    ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory.
    Memory on the node (IP: 172.17.0.2, ID: 90efe99b630d4b1f6ac1504df64764732d555b526049638f9d86552f) where the task (task ID: a6755d1708846b10007fda8a687eb57eb8a083c001000000, name=allocate_memory, pid=91085, memory used=24.99GB) was running was 32.62GB / 33.28GB (0.980175), which exceeds the memory usage threshold of 0.96. Ray killed this worker (ID: a8101629b7605f88776a08193f108adcc637248d976add819bbecbba) because it was the most recently scheduled task; to see more information about memory usage on this node, use `ray logs raylet.out -ip 172.17.0.2`. To see the logs of the worker, use `ray logs worker-a8101629b7605f88776a08193f108adcc637248d976add819bbecbba*out -ip 172.17.0.2.Top 10 memory users:
    PID     MEM(GB) COMMAND
    91085   24.99   ray::allocate_memory
    57330   2.63    /home/ray/.vscode-server/extensions/ms-vscode.cpptools-1.12.4-linux-x64/bin/cpptools
    48949   1.70    /home/ray/.vscode-server/bin/d045a5eda657f4d7b676dedbfa7aab8207f8a075/node /home/ray/.vscode-server/...
    54387   0.80    bazel(ray) -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/ray/.cache/bazel/_bazel_ray/8c472b...
    35099   0.66    /home/ray/.vscode-server/extensions/ms-vscode.cpptools-1.12.4-linux-x64/bin/cpptools-srv 57330 {1729...
    16821   0.23    /home/ray/.vscode-server/bin/d045a5eda657f4d7b676dedbfa7aab8207f8a075/node /home/ray/.vscode-server/...
    61800   0.17    /home/ray/.vscode-server/extensions/ms-vscode.cpptools-1.12.4-linux-x64/bin/cpptools-srv 57330 {54EF...
    91043   0.07    /home/ray/anaconda3/bin/python -u /home/ray/github/rayclarng/ray/python/ray/dashboard/agent.py --nod...
    90935   0.07    /home/ray/anaconda3/bin/python /home/ray/github/rayclarng/ray/python/ray/dashboard/dashboard.py --ho...
    90870   0.07    python oom.py
    Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero.

Verify the task was indeed executed twice via ``task_oom_retry``:

.. code-block:: bash

    $ grep -r "retries left" /tmp/ray/session_latest/logs/

    /tmp/ray/session_latest/logs/python-core-driver-01000000ffffffffffffffffffffffffffffffffffffffffffffffff_87487.log:[2022-11-15 13:50:27,653 I 87487 87703] task_manager.cc:458: task c8ef45ccd0112571ffffffffffffffffffffffff01000000 retries left: 3, oom retries left: 1, task failed due to oom: 1

    /tmp/ray/session_latest/logs/python-core-driver-01000000ffffffffffffffffffffffffffffffffffffffffffffffff_87487.log:[2022-11-15 13:50:36,671 I 87487 87703] task_manager.cc:458: task c8ef45ccd0112571ffffffffffffffffffffffff01000000 retries left: 3, oom retries left: 0, task failed due to oom: 1

    /tmp/ray/session_latest/logs/python-core-driver-01000000ffffffffffffffffffffffffffffffffffffffffffffffff_87487.log:[2022-11-15 13:50:36,671 I 87487 87703] task_manager.cc:466: No retries left for task c8ef45ccd0112571ffffffffffffffffffffffff01000000, not going to resubmit.

.. note::

    Task retries are executed immediately. If there is a long running process it is possible for a task to keep retrying and fail and exhaust the oom retries. Consider increasing ``RAY_task_oom_retries``, or :ref:`limit the number of concurrently running tasks <core-patterns-limit-running-tasks>`.

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
