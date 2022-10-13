Limiting the memory usage
=========================

The application may consume a large amount of heap space and causes the node to run out of memory. When that happens the operating system will start killing worker or raylet processes, which breaks Ray. It may stall the metrics and if this happens on the node head, it may stall the :ref:`dashboard <ray-dashboard>` or other control processes and causes Ray to fail.

Memory Monitor
--------------

The memory monitor is a component that runs on each node. It periodically checks the memory usage, which includes the worker heap, the object store, and the raylet as described in :ref:`memory management <memory>`. If the combined usage exceeds a configurable threshold the raylet will kill task or actor to free up memory and prevent Ray from failing.

.. note::

    The memory monitor is in :ref:`alpha <api-stability-alpha>`. It is disabled by default and needs to be enabled by setting the environment variable ``RAY_memory_monitor_interval_ms`` to a value greater than zero when Ray starts. It is available on Linux and is tested with Ray running inside a container that is using cgroup v1. If you encounter issues when running the memory monitor outside of a container or the container is using cgroup v2, please file an issue or post a question to X. 

Enabling the Memory Monitor
---------------------------

Verify the memory monitor is disabled by default:

.. code-block:: bash

    ray stop; ray start --head

The raylet logs will contain a line that tells us whether the monitor is running or not:

.. code-block:: bash

    grep memory_monitor.cc /tmp/ray/session_latest/logs/raylet.out

    (raylet) memory_monitor.cc:65: MemoryMonitor disabled. Specify `memory_monitor_interval_ms` > 0 to enable the monitor.

Now enable the memory monitor:

.. code-block:: bash

    ray stop; RAY_memory_monitor_interval_ms=250 ray start --head

Check the logs again to see the monitor is now running:

.. code-block:: bash

    grep memory_monitor.cc /tmp/ray/session_latest/logs/raylet.out

    (raylet) memory_monitor.cc:59: MemoryMonitor initialized

Retry policy
------------

When a task or actor is killed by the memory monitor it will retry using a separate retry counter called the task oom retry instead of the typical retry that is :ref:`max_retries <task-fault-tolerance>` for Tasks and :ref:`max_restarts <actor-fault-tolerance>` for Actors. The number of oom retries is global across task and actors and the default is 15. It is configured by passing the environment variable ``RAY_task_oom_retries`` when starting the driver process.

Let's create a python program oom.py that will trigger the out-of-memory condition.

.. code-block:: python

    import ray

    @ray.remote
    def allocate_memory():
        chunks = []
        bits_to_allocate = 8 * 1024 * 1024 * 1024 # 1 GiB
        while True:
            chunks.append([0] * bits_to_allocate)

    ray.get(allocate_memory.remote())

To speed up the demo, set ``RAY_task_oom_retries=1`` on the driver so the task will only retry once if it is killed by the memory monitor.

.. code-block:: bash

    RAY_task_oom_retries=1 python oom.py 

    INFO worker.py:1342 -- Connecting to existing Ray cluster at address: 172.17.0.2:6379...
    INFO worker.py:1525 -- Connected to Ray cluster. View the dashboard at http://127.0.0.1:8265 

    WARNING worker.py:1839 -- A worker died or was killed while executing a task by an unexpected system error. To troubleshoot the problem, check the logs for the dead worker. RayTask ID: 8ce7275b7a7953cc794f8c138a616d91cb907c1b01000000 Worker ID: 5c9ac30f8a9eda340f651a204de5d94f1ff965c5d9f72175579bd8dd Node ID: 3a4b60759256926fd0e84a9ff596dab3d7be854134107ef21b0e0260 Worker IP address: 172.17.0.2 Worker port: 10003 Worker PID: 69161 Worker exit type: SYSTEM_ERROR Worker exit detail: Task was killed due to the node running low on memory.
    Memory on the node (IP: 172.17.0.2, ID: 3a4b60759256926fd0e84a9ff596dab3d7be854134107ef21b0e0260) where the task was running was 32.91GB / 33.28GB (0.988698), which exceeds the memory usage threshold of 0.969955. Ray killed this worker (ID: 5c9ac30f8a9eda340f651a204de5d94f1ff965c5d9f72175579bd8dd) because it was the most recently scheduled task; to see more information about memory usage on this node, use `ray logs raylet.out -ip 172.17.0.2`. To see the logs of the worker, use `ray logs worker-5c9ac30f8a9eda340f651a204de5d94f1ff965c5d9f72175579bd8dd*out -ip 172.17.0.2`.
    Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the eviction threshold, set the environment variable `RAY_memory_usage_threshold_fraction` when starting Ray. To disable worker eviction, set the environment variable `RAY_memory_monitor_interval_ms` to zero.
    
    WARNING worker.py:1839 -- A worker died or was killed while executing a task by an unexpected system error. To troubleshoot the problem, check the logs for the dead worker. RayTask ID: b60ff970726d7cf526e74acc71310ecce51edb4c01000000 Worker ID: 39416ad98016ee6a63173856a9b4e4100625be22e6ee4192722388ba Node ID: 3a4b60759256926fd0e84a9ff596dab3d7be854134107ef21b0e0260 Worker IP address: 172.17.0.2 Worker port: 10004 Worker PID: 69160 Worker exit type: SYSTEM_ERROR Worker exit detail: Task was killed due to the node running low on memory.
    Memory on the node (IP: 172.17.0.2, ID: 3a4b60759256926fd0e84a9ff596dab3d7be854134107ef21b0e0260) where the task was running was 32.53GB / 33.28GB (0.977449), which exceeds the memory usage threshold of 0.969955. Ray killed this worker (ID: 39416ad98016ee6a63173856a9b4e4100625be22e6ee4192722388ba) because it was the most recently scheduled task; to see more information about memory usage on this node, use `ray logs raylet.out -ip 172.17.0.2`. To see the logs of the worker, use `ray logs worker-39416ad98016ee6a63173856a9b4e4100625be22e6ee4192722388ba*out -ip 172.17.0.2`.
    Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the eviction threshold, set the environment variable `RAY_memory_usage_threshold_fraction` when starting Ray. To disable worker eviction, set the environment variable `RAY_memory_monitor_interval_ms` to zero.
    
    Traceback (most recent call last):
      File "simple.py", line 11, in <module>
        ray.get(tasks)
      File "/home/ray/github/rayclarng/ray/python/ray/_private/client_mode_hook.py", line 105, in wrapper
        return func(*args, **kwargs)
      File "/home/ray/github/rayclarng/ray/python/ray/_private/worker.py", line 2291, in get
        raise value
    ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory.
    Memory on the node (IP: 172.17.0.2, ID: 3a4b60759256926fd0e84a9ff596dab3d7be854134107ef21b0e0260) where the task was running was 32.53GB / 33.28GB (0.977449), which exceeds the memory usage threshold of 0.969955. Ray killed this worker (ID: 39416ad98016ee6a63173856a9b4e4100625be22e6ee4192722388ba) because it was the most recently scheduled task; to see more information about memory usage on this node, use `ray logs raylet.out -ip 172.17.0.2`. To see the logs of the worker, use `ray logs worker-39416ad98016ee6a63173856a9b4e4100625be22e6ee4192722388ba*out -ip 172.17.0.2`.
    Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the eviction threshold, set the environment variable `RAY_memory_usage_threshold_fraction` when starting Ray. To disable worker eviction, set the environment variable `RAY_memory_monitor_interval_ms` to zero.

Verify the task was indeed executed twice via task_oom_retry:

.. code-block:: bash

    grep -r "retries left" /tmp/ray/session_latest/logs/

    /tmp/ray/session_latest/logs/python-core-driver-01000000ffffffffffffffffffffffffffffffffffffffffffffffff_60002.log:[2022-10-12 16:14:07,723 I 60002 60031] task_manager.cc:458: task c8ef45ccd0112571ffffffffffffffffffffffff01000000 retries left: 3, oom retries left: 1, task failed due to oom: 1
    
    /tmp/ray/session_latest/logs/python-core-driver-01000000ffffffffffffffffffffffffffffffffffffffffffffffff_60002.log:[2022-10-12 16:14:18,843 I 60002 60031] task_manager.cc:458: task c8ef45ccd0112571ffffffffffffffffffffffff01000000 retries left: 3, oom retries left: 0, task failed due to oom: 1
    
    /tmp/ray/session_latest/logs/python-core-driver-01000000ffffffffffffffffffffffffffffffffffffffffffffffff_60002.log:[2022-10-12 16:14:18,843 I 60002 60031] task_manager.cc:466: No retries left for task c8ef45ccd0112571ffffffffffffffffffffffff01000000, not going to resubmit.

.. note::

    When the task retries the driver resubmits the task immediately. If there is a long running process it is possible for the task to keep retrying and fail due to insufficient memory. Consider increasing ``RAY_task_oom_retries``, increase :ref:`num_cpus <resource-requirements>` to reduce the parallelism, or estimate the memory requirement and use :ref:`memory aware scheduling <memory-aware-scheduling>`.

Worker killing policy
---------------------

When the memory monitor detects the usage is above the threshold, the raylet will prioritize killing task that is retriable, i.e. ``max_retries`` or ``max_restarts`` > 0. This is done to minimize workload failure. It then looks for the last one submitted and kills that worker process. It selects and kills one process at a time regardless of how frequent the monitor runs as determined by ``RAY_memory_monitor_interval_ms``.

Let's first start ray and specify the memory threshold.

.. code-block:: bash

    ray stop; RAY_memory_monitor_interval_ms=250 RAY_memory_usage_threshold_fraction=0.7 RAY_min_memory_free_bytes=-1 ray start --head


Let's create a python program kill_priority.py that submits two tasks, where the first one is retriable and the second one is non-retriable.

.. code-block:: python

    import ray
    from ray._private.utils import get_system_memory # do not use outside of the example as these are private methods.
    from ray._private.utils import get_used_memory # do not use outside of the example as these are private methods.

    # estimates the number of bytes to allocate to reach the desired memory usage percentage.
    def get_additional_bytes_to_reach_memory_usage_pct(pct: float) -> int:
        used = get_used_memory()
        total = get_system_memory()
        bytes_needed = int(total * pct) - used
        assert bytes_needed > 0, "node has less memory than what is requested"
        return bytes_needed

    @ray.remote
    def allocate_memory_to(pct: float) -> None:
        chunks = []
        bytes_to_allocate = get_additional_bytes_to_reach_memory_usage_pct(pct)

        # each element occupies 8 bytes.
        chunks.append([0] * elements_to_allocate)
        
    first_task = allocate_memory.options(max_retries=1).remote(pct=0.65)
    second_task = allocate_memory.options(max_retries=0).remote(pct=0.65)

    # the first task will fail while the second task that is not-retriable will complete.
    with pytest.raises(ray.exceptions.OutOfMemoryError) as _:
        ray.get(first_task)

    ray.get(second_task)

Memory usage threshold
----------------------

The memory usage threshold used by the memory monitor is controlled by two environment variables RAY_memory_usage_threshold_fraction (default: 0.9) and min_memory_free_bytes (default: 1 GiB).


Configuration parameters
------------------------

The memory monitor is configured via environment variables.

`RAY_memory_usage_threshold_fraction [default_value=0.95, min=0.0, max=1.0]`: The usage fraction is calculated as (node memory used / node memory available). The memory here includes the shared memory allocated for the object store.

`RAY_memory_monitor_interval_ms [default_value=200]`: The monitor is disabled when this value is 0. This is the frequency in which the monitor computes the memory usage and notifies the raylet if the usage exceeds the threshold. Running the monitor more frequently allows it to respond faster to memory growth.

Detect and reduce worker evictions
-----------------------------------

The eviction events are exported as :ref:`application metrics <application-level-metrics>` under the name `memory_manager_worker_eviction_total`. The detailed messages of the memory monitor are written to the raylet log.

When the cluster is experiencing worker evictions the task or actor will retry and will fail when it runs out of retry. The options to reduce the number of evictions are

- Increase the memory-to-cpu ratio of the node
- Increase the cpu resource of the task and actor to reduce the parrallelism
- Estimate and set the memory resource of the task and actor

Questions or Issues?
--------------------

.. include:: /_includes/_help.rst
