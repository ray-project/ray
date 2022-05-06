.. _ray-dashboard:

Ray Dashboard
=============
Ray's built-in dashboard provides metrics, charts, and other features that help
Ray users to understand Ray clusters and libraries.

The dashboard lets you:

- View cluster metrics.
- See errors and exceptions at a glance.
- View logs across many machines in a single pane.
- Understand Ray memory utilization and debug memory errors.
- See per-actor resource usage, executed tasks, logs, and more.
- Kill actors and profile your Ray jobs.
- See Tune jobs and trial information.
- Detect cluster anomalies and debug them.

Getting Started
---------------

To use the dashboard, first install Ray with the proper dependencies:

.. code-block:: bash

  pip install "ray[default]"

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
.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/machine-view-overview.png
   :align: center

The machine view lets you see resource utilization information on a per-node and per-worker basis. This also shows the assignment of GPU resources to specific actors or tasks.

In addition, the machine view lets you see **logs** and **error messages**. You can see messages for the whole cluster, or drill down into a specific node or worker.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/machine-view-logs.png
    :align: center

Finally, you can see the task that each worker is currently performing.


Logical View
~~~~~~~~~~~~
.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/logical-view-overview.png
    :align: center

The logical view lets you monitor the actors running on your Ray cluster. For each actor class defined in your program, the logical view allows you to see how many of that class are running, how many tasks they've executed, and more.

In addition, it will warn you if you have an actor that cannot be created because your cluster has insufficient resources to satisfy its requirements.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/logical-view-warning.png
    :align: center

You can expand the panel for a class to see more detailed information about individual actors. You can profile these, view their logs, and see information about their arguments.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/logical-view-expanded-actors.png
    :align: center

Memory View
~~~~~~~~~~~~
The memory view lets you see information about the data stored in the Ray Object store. It is very useful if you have encountered an ``ObjectStoreFullError``. A number of things are stored in the object store, including:

1. Actor References
2. Objects returned from a task or actor method
3. Objects that are passed into an actor as an argument

You can group the memory view by node, or by the stack trace that created the memory being used. The latter is particularly helpful for tracking down the **line of code where a memory leak occurs**. See more below in `Debugging ObjectStoreFullError and Memory Leaks`_.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/memory-view-stack-trace.png
    :align: center
.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/memory-view-expanded.png
    :align: center

.. note::
    This view does **not** show information about **heap memory usage**. That means if your actor or task is allocating too much memory locally (not in the Ray object store a.k.a Plasma), this view will not help you find it.

Ray Config
~~~~~~~~~~
The ray config tab shows you the current cluster launcher configuration if you're using the ray cluster launcher. The cluster launcher was formerly known as the autoscaler.

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

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/logical-view-overview.png
    :align: center

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/logical-view-warning.png
    :align: center


Debugging ObjectStoreFullError and Memory Leaks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can view information for Ray objects in the memory tab. It is useful to debug memory leaks, especially `ObjectStoreFullError`.

One common cause of these memory errors is that there are objects which never go out of scope. In order to find these, you can go to the Memory View, then select to "Group By Stack Trace." This groups memory entries by their stack traces up to three frames deep. If you see a group which is growing without bound, you might want to examine that line of code to see if you intend to keep that reference around.

Note that this is the same information as displayed in the `ray memory command <https://docs.ray.io/en/master/ray-core/objects/memory-management.html#debugging-using-ray-memory>`_. For details about the information contained in the table, please see the `ray memory` documentation.

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

Running Behind a Reverse Proxy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The dashboard should work out-of-the-box when accessed via a reverse proxy. API requests don't need to be proxied individually.

Always access the dashboard with a trailing ``/`` at the end of the URL.
For example, if your proxy is set up to handle requests to ``/ray/dashboard``, view the dashboard at ``www.my-website.com/ray/dashboard/``.

The dashboard now sends HTTP requests with relative URL paths. Browsers will handle these requests as expected when the ``window.location.href`` ends in a trailing ``/``.

This is a peculiarity of how many browsers handle requests with relative URLs, despite what `MDN <https://developer.mozilla.org/en-US/docs/Learn/Common_questions/What_is_a_URL#examples_of_relative_urls>`_
defines as the expected behavior.

Make your dashboard visible without a trailing ``/`` by including a rule in your reverse proxy that
redirects the user's browser to ``/``, i.e. ``/ray/dashboard`` --> ``/ray/dashboard/``.

Below is an example with a `traefik <https://doc.traefik.io/traefik/getting-started/quick-start/>`_ TOML file that accomplishes this:

.. code-block:: yaml

  [http]
    [http.routers]
      [http.routers.to-dashboard]
        rule = "PathPrefix(`/ray/dashboard`)"
        middlewares = ["test-redirectregex", "strip"]
        service = "dashboard"
    [http.middlewares]
      [http.middlewares.test-redirectregex.redirectRegex]
        regex = "^(.*)/ray/dashboard$"
        replacement = "${1}/ray/dashboard/"
      [http.middlewares.strip.stripPrefix]
        prefixes = ["/ray/dashboard"]
    [http.services]
      [http.services.dashboard.loadBalancer]
        [[http.services.dashboard.loadBalancer.servers]]
          url = "http://localhost:8265"

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


Logical View
~~~~~~~~~~~~
.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/logical-view-expanded-actors.png
    :align: center

**Title**: Name of an actor and its arguments.

**State**: State of an actor.
- Alive
- Restarting
- Dead
- Infeasible (cannot be created due to not enough available resources (e.g. CPUs, GPUs, memory) in the cluster, even at full capacity)
- Pending Creation
- Dependencies Unready (waiting for one or more of its arguments to be ready)

**Number of Excuted Tasks**: A number of completed method calls for this actor.

**Number of ObjectRefs In Scope**: The number of object refs in scope for this actor, which correspond to objects in the Ray object store. object refs
in scope will not be evicted unless object stores are full.

**Number of Local Objects**: Number of object refs that are in this actor's local memory.
Only big objects (>100KB) reside in plasma object stores, and other small
objects are staying in local memory.

**Used Local Object Memory**: Used memory used by local objects.

**kill actor**: A button to kill an actor in a cluster. It has the same effect as calling ``ray.kill`` on an actor handle.

**profile**: A button to run profiling. We currently support profiling for 10s,
30s and 60s. It requires passwordless ``sudo``. The result of profiling is a py-spy html output displaying how much CPU time the actor spent in various methods.


Memory
~~~~~~
**Pause Collection**: A button to stop/continue updating Ray memory tables.

**IP Address**: Node IP Address where a Ray object is pinned.

**PID**: ID of a process where a Ray object is being used.

**Type**: Type of a process. It is either a driver or worker.

**Object Ref**: Object ref of a Ray object.

**Object Size** Object Size of a Ray object in bytes.

**Reference Type**: Reference types of Ray objects. Checkout the `ray memory command <https://docs.ray.io/en/master/ray-core/objects/memory-management.html#debugging-using-ray-memory>`_ to learn each reference type.

**Call Site**: Call site where this Ray object is referenced, up to three stack frames deep.

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
