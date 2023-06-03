.. _observability-getting-started:

Getting Started
===============

Ray provides a web-based dashboard for monitoring and debugging Ray applications.
The visual representation of the system state, allows users to track the performance
of applications and troubleshoot issues.

Set up
------

Install `ray[default]` to access the dashboard:

.. code-block:: bash

  pip install -U "ray[default]"

Access the dashboard with the URL that Ray prints when it initializes, or via the context object that `ray.init` returns. The default URL is **http://localhost:8265**. 

.. testcode::
  :hide:

  import ray
  ray.shutdown()

.. testcode::

    import ray

    context = ray.init()
    print(context.dashboard_url)

.. testoutput::

   127.0.0.1:8265

.. code-block:: text

  INFO worker.py:1487 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265.

The Ray Cluster installation includes the Dashboard. See :ref:`Cluster Monitoring <monitor-cluster-via-dashboard>` for more details.

.. note::

  Set up Prometheus and Grafana for enhanced visualization features such as the :ref:`Metrics view <dash-metrics-view>`.
  See :ref:`Configuring and Managing the Dashboard <observability-configure-manage-dashboard>` for instructions.
  You can still use the Dashboard without the Prometheus and Grafana integrations.


Navigate the views
------------------

The Dashboard has multiple tabs called views. Depending on your task, you may use one or a combination of views:

- Analyze, monitor, or visualize status and resource utilization metrics for logical or physical components: :ref:`Metrics view <dash-metrics-view>`, :ref:`Cluster view <dash-node-view>`
- Monitor job and task progress and status: :ref:`Jobs view <dash-jobs-view>`
- Locate logs and error messages for failed tasks and actors: :ref:`Jobs view <dash-jobs-view>`, :ref:`Logs view <dash-logs-view>`
- Analyze CPU and memory usage of tasks and actors: :ref:`Metrics view <dash-metrics-view>`,  :ref:`Cluster view <dash-node-view>`
- Monitor a Serve application: :ref:`Serve view <dash-serve-view>`

.. _dash-jobs-view:

Jobs view
---------

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/CrpXSSs0uaw" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>

The Jobs view lists the active, finished, and failed Jobs on the Ray Cluster. Click on the job ID to view more detailed information.
A Job is a Ray workload that uses Ray APIs (e.g., ``ray.init``). You can submit a Job directly (by executing a Python script within a head node) or with the :ref:`Ray Job API <jobs-quickstart>`.
For more information about Ray Jobs, see the :ref:`Ray Job Overview <jobs-overview>` section.

Job profiling
~~~~~~~~~~~~~

Profile Ray Jobs by clicking on the “Stack Trace” or “CPU Flame Graph” links. See the :ref:`Dashboard Profiling <dashboard-profiling>` for more details.

.. _dash-workflow-job-progress:

Advanced Task and Actor breakdown
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Jobs view breaks down tasks and actors by their states.
Tasks and Actors are grouped and nested by default. You can see the nested entries by clicking the expand button.

Tasks and Actors are grouped and nested using the following criteria:

- All Tasks and Actors are grouped together. View individual entries by expanding the corresponding row.
- Tasks are grouped by their ``name`` attribute (e.g., ``task.options(name="<name_here>").remote()``).
- Child tasks (nested tasks) are nested under their parent task's row.
- Actors are grouped by their class name.
- Child actors (actors created within an actor) are nested under their parent actor's row.
- Actor tasks (remote methods within an actor) are nested under the actor for the corresponding actor method.

.. note::

  The Dashboard can display or retrieve up to 10K tasks at a time. If your job has more than 10K tasks,
  they are unaccounted. The number of unaccounted tasks is available from the task breakdown.

Task timeline
~~~~~~~~~~~~~

The :ref:`timeline API <ray-core-timeline>` is available from the Task Timeline pane.

Download the Chrome tracing file by clicking the download button.

Drag and drop the downloaded Chrome tracing file to `Perfetto UI <https://ui.perfetto.dev/>`_ to visualize Chrome tracing files. If Perfetto is not available, you can use ``chrome://tracing``.  

The timeline visualization of Ray Tasks and Actors has Node rows (hardware) and Worker rows (processes).
Each worker row displays a list of events (e.g., task scheduled, task running, input/output deserialization, etc.) for that worker over time.

Ray status
~~~~~~~~~~

The Jobs view displays the Autoscaler status of the Ray Cluster. This information is the output of the ``ray status`` CLI command.

The left pane shows the Autoscaling status, including pending, active, and failed nodes.
The right pane displays the Cluster's demands, which are resources that cannot be scheduled on the Cluster at the moment. This page is useful for debugging resource deadlocks or slow scheduling.

.. note::

  The output shows the aggregated information across the Cluster (not by Job). If you run more than one Job, some of the demands may come from other Jobs.

.. _dash-workflow-state-apis:

Task, Actor, and Placement Group tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Dashboard displays a table of the status of the Job's Tasks, Actors, and Placement Groups.
This information is the output of the :ref:`Ray State APIs <state-api-overview-ref>`.

You can expand the table to see a list of each Task, Actor, and Placement Group.

.. _dash-serve-view:

Serve view
----------

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/eqXfwM641a4" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>

Use the Serve view to monitor the status of your :ref:`Ray Serve <rayserve>` applications.

See your general Serve configurations, a list of the Serve applications, and, if you configured :ref:`Grafana and Prometheus <observability-configure-manage-dashboard>`, high-level
metrics of your Serve applications. Click the name of a Serve application to go to the Serve Application Detail page.

Serve Application Detail page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See the Serve application's configurations and metadata and the list of :ref:`Serve deployments and replicas <serve-key-concepts-deployment>`.
Click the expand button of a deployment to see the replicas.

Each deployment has two available actions. You can view the Deployment config and, if you configured :ref:`Grafana and Prometheus <observability-configure-manage-dashboard>`, you can open
a Grafana dashboard with detailed metrics about that deployment.

Each replica has two available actions. You can see the logs of that replica and, if you configured :ref:`Grafana and Prometheus <observability-configure-manage-dashboard>`, you can open
a Grafana dashboard with detailed metrics about that replica. Click on the replica name to go to the Serve Replica Detail page.


Serve Replica Detail page
~~~~~~~~~~~~~~~~~~~~~~~~~

See the metadata about the Serve replica, high-level metrics about the replica if you configured :ref:`Grafana and Prometheus <observability-configure-manage-dashboard>`, and
a history of completed :ref:`tasks <core-key-concepts>` for that replica.


Serve metrics
~~~~~~~~~~~~~

Ray Serve exports various time-series metrics to understand the status of your Serve application over time. See more details of these metrics :ref:`here <serve-production-monitoring-metrics>`.
To store and visualize these metrics, you must set up Prometheus and Grafana by following the instructions :ref:`here <observability-configure-manage-dashboard>`.

These metrics are available in the Ray Dashboard in the Serve page and the Serve Replica Detail page. They are also accessible as Grafana dashboards.
Within the Grafana dashboard, use the dropdown filters on the top to filter metrics by route, deployment, or replica. Exact descriptions
of each graph are available by hovering over the "info" icon on the top left of each graph.


.. _dash-node-view:

Cluster view
------------

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/K2jLoIhlsnY" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>

The Cluster view is a visualization of the hierarchical relationship of
machines (nodes) and workers (processes). Each host consists of many workers, that
you can see by clicking the + button. See also the assignment of GPU resources to specific Actors or Tasks.

Click the node ID to see the node detail page.

In addition, the machine view lets you see **logs** for a node or a worker.

.. _dash-actors-view:

Actors view
-----------

Use the Actors view to see the logs for an Actor and which Job created the actor.

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/MChn6O1ecEQ" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>
    
The information for up to 1000 dead actors is stored.
Override this value with the `RAY_DASHBOARD_MAX_ACTORS_TO_CACHE` environment variable
when starting Ray.

Actor profiling
~~~~~~~~~~~~~~~

Run the profiler on a running Actor. See :ref:`Dashboard Profiling <dashboard-profiling>` for more details.

Actor Detail page
~~~~~~~~~~~~~~~~~

Click the ID, to see the detail view of the Actor.

On the Actor Detail page, see the metadata, state, and all of the Actor's tasks that have run.

.. _dash-metrics-view:

Metrics view
------------

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/yn5Q65iHAR8" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>

Use the Metrics view to view visualizations of the time series metrics emitted by Ray.

Ray exports default metrics which are available from the :ref:`Metrics view <dash-metrics-view>`. Here are some available example metrics.

- Tasks, Actors, and Placement Groups broken down by states
- :ref:`Logical resource usage <logical-resources>` across nodes
- Hardware resource usage across nodes
- Autoscaler status

You can select the time range of the metrics in the top right corner. The graphs refresh automatically every 15 seconds.

See :ref:`System Metrics Page <system-metrics>` for available metrics.

.. note::

  The following functionality requires the Prometheus and Grafana setup. See :ref:`Configuring and Managing the Dashboard <observability-configure-manage-dashboard>` to learn how to set up Prometheus and Grafana.

Open the Grafana UI with the button in the Dashboard. The Grafana UI provides additional customizability of the charts.

.. _dash-workflow-cpu-memory-analysis:

Analyze the CPU and memory usage of Tasks and Actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`Metrics view <dash-metrics-view>` in the Dashboard provides a "per-component CPU/memory usage graph" that displays CPU and memory usage over time for each Task and Actor in the application (as well as system components). 
You can identify tasks and actors that may be consuming more resources than expected and optimize the performance of the application. 

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/node_cpu_by_comp.png
    :align: center


Per component CPU graph. 0.379 cores mean that it uses 40% of a single CPU core. Ray process names start with ``ray::``. ``raylet``, ``agent``, ``dashboard``, or ``gcs`` are system components.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/node_memory_by_comp.png
    :align: center

Per component memory graph. Ray process names start with ``ray::``. ``raylet``, ``agent``, ``dashboard``, or ``gcs`` are system components.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/cluster_page.png
    :align: center

Additionally, users can see a snapshot of hardware utilization from the :ref:`cluster page <dash-node-view>`, which provides an overview of resource usage across the entire Ray Cluster.

.. _dash-workflow-resource-utilization:

View the resource utilization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray requires users to specify the number of :ref:`resources <logical-resources>` their Tasks and Actors to use through arguments such as ``num_cpus``, ``num_gpus``, ``memory``, and ``resource``. 
These values are used for scheduling, but may not always match the actual resource utilization (physical resource utilization).

- See the logical and physical resource utilization over time from the :ref:`Metrics view <dash-metrics-view>`.
- The snapshot of physical resource utilization (CPU, GPU, memory, disk, network) is also available from the :ref:`Cluster view <dash-node-view>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/logical_resource.png
    :align: center

The :ref:`logical resources <logical-resources>` usage.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/physical_resource.png
    :align: center

The physical resources (hardware) usage. Ray provides CPU, GPU, Memory, GRAM, disk, and network usage for each machine in a cluster.

.. _dash-logs-view:

Logs view
---------

.. raw:: html

    <div style="position: relative; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe width="560" height="315" src="https://www.youtube.com/embed/8V187F2DsN0" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
    </div>
 
Use the Logs view to see the Ray logs in your Cluster. 

Logs view is organized by node and log file name. Many links to logs in the other views link to a filtered list of this view.

To understand the log file structure of Ray, see the :ref:`Logging directory structure page <logging-directory-structure>`.

The Logs view provides search functionality to help you find specific log messages.


**Driver logs**

If the Ray Job is submitted by the :ref:`Ray job API <jobs-quickstart>`, the Job logs are available from the Dashboard. The log file follows the following format; ``job-driver-<job_submission_id>.log``.

.. note::

  If the driver is executed directly on the head node of the Ray cluster (without the Job API) or run with the :ref:`Ray client <ray-client-ref>`, the driver logs are not accessible from the Dashboard. In this case, see the terminal output to view the driver logs.

**Task and Actor logs**

Task and Actor logs are accessible from the :ref:`task and actor table view <dash-workflow-state-apis>`. Click the log button.
See the worker logs (``worker-[worker_id]-[job_id]-[pid].[out|err]``) that execute the Task and Actor. ``.out`` (stdout) and ``.err`` (stderr) logs contain the logs emitted from Tasks and Actors.
The core worker logs (``python-core-worker-[worker_id]_[pid].log``) contain the system-level logs for the corresponding worker.

**Task and Actor errors**

Identify failed tasks or actors by looking at the Job progress bar, which links to the table.

The table displays the name of the failed Tasks or Actors and provides access to their corresponding log or error messages.

.. _dash-overview:

Overview view
-------------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/overview-page.png
    :align: center

The Overview view provides a high-level status of the Ray Cluster.

**Overview Metrics**

The Overview Metrics page provides the cluster-level hardware utilization and autoscaling status (number of pending, active, and failed nodes).

**Recent Jobs**

The Recent Jobs pane provides a list of recently submitted Ray Jobs.

.. _dash-event:

**Events view**

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/event-page.png
    :align: center

The Events view displays a list of events associated with a specific type (e.g., Autoscaler or Job) in chronological order. The same information is accessible with the ``ray list cluster-events`` :ref:`(Ray state APIs)<state-api-overview-ref>` CLI commands.

Two types of events are available:

- Job: Events related to :ref:`Ray job submission APIs <jobs-quickstart>`.
- Autoscaler: Events related to the :ref:`Ray autoscaler <cluster-autoscaler>`.

Resources
---------
- `Ray Summit observability talk <https://www.youtube.com/watch?v=v_JzurOkdVQ>`_
- `Ray metrics blog <https://www.anyscale.com/blog/monitoring-and-debugging-ray-workloads-ray-metrics>`_
- `Ray Dashboard roadmap <https://github.com/ray-project/ray/issues/30097#issuecomment-1445756658>`_
- `Observability Training Module <https://github.com/ray-project/ray-educational-materials/blob/main/Observability/Ray_observability_part_1.ipynb>`_