Ray Dashboard
=============
Ray's built-in dashboard provides metrics, charts, and other features that help
Ray users to understand Ray clusters and libraries.

Through the dashboard, you can

- View cluster metrics.
- Visualize the actor relationships and statistics.
- Kill actors and profile your Ray jobs.
- See Tune jobs and trial information.
- Detect cluster anomalies and debug them.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Dashboard-overview.png
    :align: center

Getting Started
---------------
You can access the dashboard through its default URL, **localhost:8265**.
(Note that the port number increases if the default port is not available).
If you prefer to explicitly set the port on which the dashboard will run, you can pass
the ``--dashboard-port`` argument with ``ray start`` in the command line, or you can pass the
keyword argument ``dashboard_port`` in your call to ``ray.init()``.

The URL is printed when ``ray.init()`` is called.

.. code-block:: text

  INFO services.py:1093 -- View the Ray dashboard at localhost:8265

The dashboard is also available :ref:`when using the cluster launcher <monitor-cluster>`.

Views
-----

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-component-view.png
    :align: center

Machine View
~~~~~~~~~~~~
The machine view shows you:

- System resource usage for each machine and worker such as RAM, CPU, disk, and network usage information.
- Logs and error messages for each machine and worker.
- Actors or tasks assigned to each worker process.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Machine-view-basic.png
    :align: center

Logical View
~~~~~~~~~~~~
The logical view shows you:

- Created and killed actors.
- Actor statistics such as actor status, number of executed tasks, pending tasks, and memory usage.
- Actor hierarchy.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Logical-view-basic.png
    :align: center

Memory View
~~~~~~~~~~~~
The memory view shows you:

- The state of Ray objects, including their size, reference type, and call site.
- A summary of reference types and object sizes in use.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/Memory-view-basic.png
    :align: center

Ray Config
~~~~~~~~~~

The ray config tab shows you the current autoscaler configuration.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Ray-config-basic.png
    :align: center

Tune
~~~~
The Tune tab shows you:

- Tune jobs and their statuses.
- Hyperparameters for each job.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Tune-basic.png
    :align: center

Advanced Usage
--------------

Killing Actors
~~~~~~~~~~~~~~
You can kill actors when actors are hanging or not in progress.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/kill-actors.png
    :align: center

Debugging a Blocked Actor
~~~~~~~~~~~~~~~~~~~~~~~~~
You can find hanging actors through the Logical View tab.

If creating an actor requires resources (e.g., CPUs, GPUs, or other custom resources)
that are not currently available, the actor cannot be created until those resources are
added to the cluster or become available. This can cause an application to hang. To alert
you to this issue, infeasible tasks are shown in red in the dashboard, and pending tasks
are shown in yellow.

Below is an example.

.. code-block:: python

  import ray

  ray.init(num_gpus=2)

  @ray.remote(num_gpus=1)
  class Actor1:
      def __init__(self):
          pass

  @ray.remote(num_gpus=4)
  class Actor2:
      def __init__(self):
          pass

  actor1_list = [Actor1.remote() for _ in range(4)]
  actor2 = Actor2.remote()


.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-pending-infeasible-actors.png
    :align: center

This cluster has two GPUs, and so it only has room to create two copies of ``Actor1``.
As a result, the rest of ``Actor1`` will be pending.

You can also see it is infeasible to create ``Actor2`` because it requires 4 GPUs which
is bigger than the total gpus available in this cluster (2 GPUs).

Debugging ObjectStoreFullError and Memory Leak
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can view information for Ray objects in the memory tab. It is useful to debug memory leaks, especially `ObjectStoreFullError`.
Note that this is the same information as displayed in the `ray memory command <https://docs.ray.io/en/latest/memory-management.html#debugging-using-ray-memory>`_. For details about the information contained in the table, please see the `ray memory` documentation.

Inspect Memory Usage
~~~~~~~~~~~~~~~~~~~~
You can detect local memory anomalies through the Logical View tab. If NumObjectRefsInScope,
NumLocalObjects, or UsedLocalObjectMemory keeps growing without bound, it can lead to out
of memory errors or eviction of objectIDs that your program still wants to use.

Profiling (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~
Use profiling features when you want to find bottlenecks in your Ray applications.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-profiling-buttons.png
    :align: center

Clicking one of the profiling buttons on the dashboard launches py-spy, which will profile
your actor process for the given duration. Once the profiling has been done, you can click the "profiling result" button to visualize the profiling information as a flamegraph.

This visualization can help reveal computational bottlenecks.

.. note::

  The profiling button currently only works when you use **passwordless** ``sudo``.
  It is still experimental. Please report any issues you run into.

More information on how to interpret the flamegraph is available at https://github.com/jlfwong/speedscope#usage.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-profiling.png
    :align: center

References
----------

Machine View
~~~~~~~~~~~~

**Machine/Worker Hierarchy**: The dashboard visualizes hierarchical relationship of
workers (processes) and machines (nodes). Each host consists of many workers, and
you can see them by clicking the + button.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Machine-view-reference-1.png
    :align: center

You can hide it again by clicking the - button.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Machine-view-reference-2.png
    :align: center

**Resource Configuration**

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Resource-allocation-row.png
    :align: center

Resource configuration is represented as ``([Resource]: [Used Resources] / [Configured Resources])``.
For example, when a Ray cluster is configured with 4 cores, ``ray.init(num_cpus=4)``, you can see (CPU: 0 / 4).

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/resource-allocation-row-configured-1.png
    :align: center

When you spawn a new actor that uses 1 CPU, you can see this will be (CPU: 1/4).

Below is an example.

.. code-block:: python

  import ray

  ray.init(num_cpus=4)

  @ray.remote(num_cpus=1)
  class A:
      pass

  a = A.remote()

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/resource-allocation-row-configured-2.png
    :align: center

**Host**: If it is a node, it shows host information. If it is a worker, it shows a pid.

**Workers**: If it is a node, it shows a number of workers and virtual cores.
Note that number of workers can exceed number of cores.

**Uptime**: Uptime of each worker and process.

**CPU**: CPU usage of each node and worker.

**RAM**: RAM usage of each node and worker.

**Disk**: Disk usage of each node and worker.

**Sent**: Network bytes sent for each node and worker.

**Received**: Network bytes received for each node and worker.

**Logs**: Logs messages at each node and worker. You can see log messages by clicking it.

**Errors**: Error messages at each node and worker. You can see error messages by clicking it.


Logical View (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Actor Titles**: Name of an actor and its arguments.

**State**: State of an actor.

- 0: Alive
- 1: Restarting
- 2: Dead

**Pending**: A number of pending tasks for this actor.

**Excuted**: A number of executed tasks for this actor.

**NumObjectRefsInScope**: Number of object refs in scope for this actor. object refs
in scope will not be evicted unless object stores are full.

**NumLocalObjects**: Number of object refs that are in this actor's local memory.
Only big objects (>100KB) are residing in plasma object stores, and other small
objects are staying in local memory.

**UsedLocalObjectMemory**: Used memory used by local objects.

**kill actor**: A button to kill an actor in a cluster. It is corresponding to ``ray.kill``.

**profile for**: A button to run profiling. We currently support profiling for 10s,
30s and 60s. It requires passwordless ``sudo``.

**Infeasible Actor Creation**: Actor creation is infeasible when an actor
requires more resources than a Ray cluster can provide. This is depicted
as a red colored actor.

**Pending Actor Creation**: Actor creation is pending when there are no
available resources for this actor because they are already taken by other
tasks and actors. This is depicted as a yellow colored actor.

**Actor Hierarchy**: The logical view renders actor information in a tree format.

To illustrate this, in the code block below, the ``Parent`` actor creates
two ``Child`` actors and each ``Child`` actor creates one ``GrandChild`` actor.
This relationship is visible in the dashboard *Logical View* tab.

.. code-block:: python

  import ray
  ray.init()

  @ray.remote
  class Grandchild:
      def __init__(self):
          pass

  @ray.remote
  class Child:
      def __init__(self):
          self.grandchild_handle = Grandchild.remote()

  @ray.remote
  class Parent:
      def __init__(self):
          self.children_handles = [Child.remote() for _ in range(2)]

  parent_handle = Parent.remote()

You can see that the dashboard shows the parent/child relationship as expected.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/Logical-view-basic.png
    :align: center

Memory
~~~~~~
**Pause Collection**: A button to stop/continue updating Ray memory tables.

**IP Address**: Node IP Address where a Ray object is pinned.

**Pid**: ID of a process where a Ray object is being used.

**Type**: Type of a process. It is either a driver or worker.

**Object Ref**: Object ref of a Ray object.

**Object Size** Object Size of a Ray object in bytes.

**Reference Type**: Reference types of Ray objects. Checkout the `ray memory command <https://docs.ray.io/en/latest/memory-management.html#debugging-using-ray-memory>`_ to learn each reference type.

**Call Site**: Call site where this Ray object is referenced.

Ray Config
~~~~~~~~~~~~

If you are using the cluster launcher, this Configuration defined at ``cluster.yaml`` is shown.
See `Cluster.yaml reference <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`_ for more details.

Tune (Experimental)
~~~~~~~~~~~~~~~~~~~
**Trial ID**: Trial IDs for hyperparameter tuning.

**Job ID**: Job IDs for hyperparameter tuning.

**STATUS**: Status of each trial.

**Start Time**: Start time of each trial.

**Hyperparameters**: There are many hyperparameter users specify. All of values will
be visible at the dashboard.
