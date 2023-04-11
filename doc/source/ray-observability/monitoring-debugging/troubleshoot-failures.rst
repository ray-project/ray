Troubleshooting Failures
========================

What Kind of Failures Exist in Ray?
-----------------------------------

Ray consists of 2 major APIs. :func:`ray.remote <ray.remote>` to create a task/actor and :func:`ray.get <ray.get>` to get the result. 
Debugging Ray means identifying and fixing failures from remote processes that run functions and classes (task and actor) created by the ``.remote`` API. 

Ray APIs are future APIs (indeed, it is :ref:`possible to convert Ray object references to standard Python future APIs <async-ref-to-futures>`), 
and the error handling model is the same. When any remote tasks or actors fail, the returned object ref will contain an exception. 
When you call ``get`` API to the object ref, it raises an exception.

.. code-block:: python

  import ray
  @ray.remote
  def f():
      raise ValueError
  
  # Raises a ValueError.
  ray.get(f.remote())

In Ray, there are 3 types of failures. See exception APIs for more details. 

- **Application failures**: This means the remote task/actor fails by the user code. In this case, ``get`` API will raise the ``RayTaskError`` which includes the exception raised from the remote process.
- **Intentional system failures**: This means Ray is failed, but the failure is intended. For example, when you call cancellation APIs like ``ray.cancel`` (for task) or ``ray.kill`` (for actors), the system fails remote tasks and actors, but it is intentional.
- **Unintended system failures**: This means the remote tasks and actors failed due to unexpected system failures such as processes crashing (for example, by out-of-memory error) or nodes failing. There are 4 common types.
  1. `Linux Out of Memory killer <https://www.kernel.org/doc/gorman/html/understand/understand016.html>`_ or :ref:`Ray OOM killer <ray-oom-monitor>` kills processes with high memory usages to avoid out-of-memory.
  2. The machine shuts down (e.g., spot instance termination) or a raylet is crashed (e.g., by an unexpected failure). 
  3. System is highly overloaded or stressed (either machine or system components like Raylet or GCS), which makes the system unstable and fail.

Debugging Application Failures
------------------------------

Ray distributes users' code to multiple processes across many machines. Application failures mean bugs in users' code.
Ray provides a debugging experience that's similar to debugging a single-process Python program.

print
~~~~~

``print`` debugging is one of the most common ways to debug Python programs. 
:ref:`Ray's task and actor logs are printed to the Ray driver <ray-worker-logs>` by default, 
which allows you to simply use the ``print`` function to debug the application failures.

Debugger
~~~~~~~~

Many Python developers use a debugger to debug Python programs, and `Python pdb <https://docs.python.org/3/library/pdb.html>`_) is one of the popular choices.
Ray has native integration to ``pdb``. You can simply add ``breakpoint()`` to actors and tasks code to enable ``pdb``. View :ref:`Ray Debugger <ray-debugger>` for more details.

Debugging Out of Memory
-----------------------

Before reading this section, it is recommended to understand Ray's :ref:`Memory Management <memory>` model.

What's the Out-of-Memory Error?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Memory is a limited resource. When a process requests memory and the OS fails to allocate memory, the OS executes a routine to free up memory
by killing a process that has high memory usage (via SIGKILL) to avoid the OS becoming unstable. It is called `Linux Out of Memory killer <https://www.kernel.org/doc/gorman/html/understand/understand016.html>`_.

One of the common problems of the Linux out-of-memory killer is that processes are killed by a SIGKILL without Ray noticing it. 
Since SIGKILL cannot be handled by processes, it makes Ray difficult to raise a proper error message
and take proper actions for fault tolerance.
To solve this problem, Ray built and enalbed (from Ray 2.2) an application-level :ref:`memory monitor <ray-oom-monitor>`,
which keeps monitoring the memory usage of the host and kills the Ray workers before the Linux out-of-memory killer kicks in. 

How to Detect Out-of-Memory Errors?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If tasks or actors are killed by the Linux out-of-memory killer, Ray worker processes are unable to catch and display an exact root cause
because SIGKILL cannot be handled by processes. If you call ``ray.get`` into the tasks and actors that were executed from the dead worker,
it will raise an exception with one of the following error messages (which indicates the worker is killed unexpectedly).

.. code-block:: bash

  Worker exit type: UNEXPECTED_SY STEM_EXIT Worker exit detail: Worker unexpectedly exits with a connection error code 2. End of file. There are some potential root causes. (1) The process is killed by SIGKILL by OOM killer due to high memory usage. (2) ray stop --force is called. (3) The worker is crashed unexpectedly due to SIGSEGV or other unexpected errors.

.. code-block:: bash

  Worker exit type: SYSTEM_ERROR Worker exit detail: The leased worker has unrecoverable failure. Worker is requested to be destroyed when it is returned.

If the worker is killed by Ray's memory monitor, it raises an exception with 
a much cleaner error message when you call ``ray.get`` to a task or actor that executed on a dead worker.

.. code-block:: bash

  ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory.

  Task was killed due to the node running low on memory.
  Memory on the node (IP: 10.0.62.231, ID: e5d953ef03e55e26f13973ea1b5a0fd0ecc729cd820bc89e4aa50451) where the task (task ID: 43534ce9375fa8e4cd0d0ec285d9974a6a95897401000000, name=allocate_memory, pid=11362, memory used=1.25GB) was running was 27.71GB / 28.80GB (0.962273), which exceeds the memory usage threshold of 0.95. Ray killed this worker (ID: 6f2ec5c8b0d5f5a66572859faf192d36743536c2e9702ea58084b037) because it was the most recently scheduled task; to see more information about memory usage on this node, use `ray logs raylet.out -ip 10.0.62.231`. To see the logs of the worker, use `ray logs worker-6f2ec5c8b0d5f5a66572859faf192d36743536c2e9702ea58084b037*out -ip 10.0.62.231.`
  Top 10 memory users:
  PID	MEM(GB)	COMMAND
  410728	8.47	510953	7.19	ray::allocate_memory
  610952	6.15	ray::allocate_memory
  711164	3.63	ray::allocate_memory
  811156	3.63	ray::allocate_memory
  911362	1.25	ray::allocate_memory
  107230	0.09	python test.py --num-tasks 2011327	0.08	/home/ray/anaconda3/bin/python /home/ray/anaconda3/lib/python3.9/site-packages/ray/dashboard/dashboa...

  Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html.

Ray memory monitor also periodically prints the aggregated out-of-memory killer summary to Ray drivers.

.. code-block:: bash

  (raylet) [2023-04-09 07:23:59,445 E 395 395] (raylet) node_manager.cc:3049: 10 Workers (tasks / actors) killed due to memory pressure (OOM), 0 Workers crashed due to other reasons at node (ID: e5d953ef03e55e26f13973ea1b5a0fd0ecc729cd820bc89e4aa50451, IP: 10.0.62.231) over the last time period. To see more information about the Workers killed on this node, use `ray logs raylet.out -ip 10.0.62.231`
  (raylet) 
  (raylet) Refer to the documentation on how to address the out of memory issue: https://docs.ray.io/en/latest/ray-core/scheduling/ray-oom-prevention.html. Consider provisioning more memory on this node or reducing task parallelism by requesting more CPUs per task. To adjust the kill threshold, set the environment variable `RAY_memory_usage_threshold` when starting Ray. To disable worker killing, set the environment variable `RAY_memory_monitor_refresh_ms` to zero.

ray.exceptions.OutOfMemoryError: Task was killed due to the node running low on memory.
Monitoring
When OOM happens, memory usage is steeply increasing, and eventually the OOM killer kicks in and kill processes. Here are tools to monitor the memory usage. 
Historical usage
Dashboard Metrics View: By default, Anyscale provides the built-in monitoring for memory usage. Users can see the “node memory” graph which shows the per node memory usage. Or “node memory per component” which shows the memory uage per component such as raylet or workers.
Snapshot
Htop: If you’d like to pinpoint the processes that use the high memory, it is recommended to use `htop` command. You can use this command using `sudo apt-get install htop`. Type htop from the terminal and see the RSS uage per process. Note that the actual memory usage per process is RES - SHR (SHR is a shared memory usage).



Dashboard Node View: Dashboard node view provides the snapshot of memory usage per worker. 
Workflow
Find the node that has the high memory usage via Ray Core Debugging Runbook.
See the memory usage of the node that has increasing memory usage. You can use htop or node view to see it.
Debug. There are several symptoms
Head node has high memory usage
Head node normally has higher memory usage than other nodes because it runs extra processes such as driver (which usually has higher memory usage than workers) or more core components (e.g., dashboard, GCS). In this case, try using a bigger head node or configure 0 CPU on a  head node, so that you avoid scheduling extra tasks and actors on a head node. 
Memory leak or unexpectedly high memory usage from each process.
In this case, find the processes that have unexpected memory usage and run a memory profiler. I recommend using `memray` that has the built-in Python API.
High memory usage for all processes
This usually indicates the concurrency control has failed. 
Decrease parallelism (increase num_cpus of tasks and actors). Ray’s number of workers are controlled by num_cpus. Increase the num_cpus of each task to reduce the concurrently executing workers
Decrease the per process memory usage. This is common when you have a train job that is run upon ray dataset that has a big partition. Reduce the partition size or find the inefficient memory allocation using memory profiler (3-(a))in this case.
By default, Ray doesn’t provide any hints on memory usage, which means the high-memory tasks can be scheduled on the same node. We can solve the problem by better scheduling.
Use memory aware scheduling.  Assign the peak memory usage to the `.remote` API. Note that to make this work, you should know the peak memory of all tasks/actors that you are using.
Use spread scheduling to spread out tasks / actors. By default, Ray prefers to pack tasks into the same node until it reaches to the threshold. It can cause issues when you have workloads like reading data in the beginning (since all data reading tasks will be scheduled on the same node). 
Library has high memory usage
If the high memory usage is from the library, each library should have their own guideline to debug out of memory issues.
Features coming up
Built-in memory profiling tool
Per func/class name memory usage

Relevant Materials
~~~~~~~~~~~~~~~~~~
- "Investigating OOM problems with the monitor and Ray Dashboard" section from the `blog <https://www.anyscale.com/blog/automatic-and-optimistic-memory-scheduling-for-ml-workloads-in-ray>`_.
- :ref:`Ray Out of Memory Monitor <ray-oom-monitor>`.
- `Linux Out of Memory killer <https://www.kernel.org/doc/gorman/html/understand/understand016.html>`_.


Starting many actors
--------------------

Workloads that start a large number of actors all at
  once may exhibit problems when the processes (or libraries that they use)
  contend for resources. Similarly, a script that starts many actors over the
  lifetime of the application will eventually cause the system to run out of
  file descriptors. 

Running out of file descriptors
-------------------------------

As a workaround, you may be able to
  increase the maximum number of file descriptors with a command like
  ``ulimit -n 65536``. If that fails, double check that the hard limit is
  sufficiently large by running ``ulimit -Hn``. If it is too small, you can
  increase the hard limit as follows (these instructions work on EC2).

    * Increase the hard ulimit for open file descriptors system-wide by running
      the following.

      .. code-block:: bash

        sudo bash -c "echo $USER hard nofile 65536 >> /etc/security/limits.conf"

    * Logout and log back in.

This document discusses some common problems that people run into when using Ray
as well as some known problems. If you encounter other problems, please
`let us know`_.

.. _`let us know`: https://github.com/ray-project/ray/issues
