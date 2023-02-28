.. _ray-dashboard:

Ray Dashboard
=============
Ray provides a web-based dashboard for monitoring and debugging Ray applications.
The dashboard provides a visual representation of the system state, allowing users to track the performance 
of their applications and troubleshoot issues.

SANG-TODO Replace the image with GIF.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/Dashboard-overview.png
    :align: center

Here are common workflows when using the Ray dashboard.

- :ref:`See the metrics graphs <dash-workflow-critial-system-metrics>` (requires Prometheus and Grafana to be deployed).
- :ref:`View the hardware utilization (CPU, GPU, memory) <dash-workflow-resource-utilization>` so that you can compare it to Ray's logical resource usages.
- :ref:`See the progress of your job <dash-workflow-job-progress>`.
- :ref:`Find the logs or error messages of failed tasks or actors <dash-workflow-failed-tasks>`.
- :ref:`Profile, trace dump, and visualize the timeline of the Ray jobs, tasks, or actors <dashboard-profiling>`.
- :ref:`Analyze the CPU and memory usage of the cluster, tasks and actors <dash-workflow-cpu-memory-analysis>`.
- :ref:`See individual state of task, actor, placement group <dash-workflow-state-apis>`, and :ref:`nodes (machines) <dash-node-view>` which is equivalent to :ref:`Ray state APIs <state-api-overview-ref>`.

Getting Started
---------------

To use the dashboard, you should use the `ray[default]` installation:

.. code-block:: bash

  pip install -U "ray[default]"

You can access the dashboard through a URL printed when Ray is initialized (the default URL is **http://localhost:8265**) or via the context object returned from `ray.init`.

.. tabbed:: Python SDK

    .. code-block:: python

        context = ray.init()
        print(context.dashboard_url)

.. code-block:: text

  INFO worker.py:1487 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265.

The dashboard is also available :ref:`when using the cluster launcher <monitor-cluster>`.

.. note:: 

  When using the Ray dashboard, it is highly recommended to also set up Prometheus and Grafana. 
  Ray dashboard has a tight integration with them, and core features (e.g., :ref:`Metrics View <dash-metrics-view>`) wouldn't work without them.

See :ref:`Ray Metrics <ray-metrics>` to learn how to set up Prometheus and Grafana.

If you prefer to explicitly set the port on which the dashboard will run, you can pass
the ``--dashboard-port`` argument with ``ray start`` in the command line, or you can pass the
keyword argument ``dashboard_port`` in your call to ``ray.init()``.

How to Guide
------------

.. _dash-workflow-critial-system-metrics:

See the system metrics
~~~~~~~~~~~~~~~~~~~~~~

Ray exports metrics by default. Metrics are available from the :ref:`Metrics View <dash-metrics-view>`. Here are some example metrics that are available.

- The tasks, actors, and placement group broken down by states.
- The :ref:`logical resource usage <logical-resources>` across nodes.
- The hardware resource usage across nodes.
- The autoscaler status.

See :ref:`System Metrics Page <system-metrics>` for available metrics.

.. _dash-workflow-resource-utilization:

Comparing the hardware and logical resource utilization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray requires users to specify the amount of resources their tasks and actors will use through arguments such as ``num_cpus``, ``num_gpus``, ``memory``, and ``resource``. 
These values are used for scheduling, but may not always match the actual resource utilization. 
You can check the :ref:`Metrics View <dash-metrics-view>` to compare logical and hardware resource utilization. 

Let's see an example.

.. code-block:: python

    @ray.remote(num_cpus=1)
    def task():
        import time
        time.sleep(30)
    
    # Launch 30 tasks
    ray.get([task.remote() for _ in range(30)])

When you run the below code that executes many tasks that sleeps, 
you will see from the metrics page that there are many logical CPU allocations but little to no hardware CPU usage.
It is because the logical resource requirement is 1 CPU for each task, but each task uses nearly no CPU because it just sleeps.

TODO-SANG Add images

In this case, resource specification for those tasks is incorrect. 
To address this issue, let's adjust the num_cpus value to 0.1.

.. code-block:: python

    @ray.remote(num_cpus=0.1)
    def task():
        import time
        time.sleep(30)

.. _dash-workflow-failed-tasks:

Find the erorr messages or logs of the failed tasks/actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can easily identify failed tasks or actors by looking at the job progress bar, which links to the table. 
The table displays the name of the failed tasks or actors and provides access to their corresponding log or error messages.

SANG-TODO 2 images, one with a failed task, another for the error messages.

.. _dash-workflow-cpu-memory-analysis:

Analyze the CPU and memory usage of tasks and actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`Metrics View <dash-metrics-view>` in the Ray dashboard provides a component called the "CPU/memory usage graph" that displays CPU and memory usage over time for each task and actor in the application. 
This allows users to identify tasks and actors that may be consuming more resources than expected and optimize the performance of the application. 

SANG-TODO Add an image.

Additionally, users can see a snapshot of hardware utilization from the :ref:`cluster page <dash-node-view>`, which provides an overview of resource usage across the entire Ray cluster.

SANG-TODO Add an image.


.. _dash-overview:

Overview
--------

The overview page provides a high level status of the Ray cluster.

TODO-SANG Images

Overview Metrics
~~~~~~~~~~~~~~~~

Overview metrics page provides the cluster-level hardware utilization and the autoscaling status (number of pending, active, and failed nodes).

SANG-TODO Image

Recent Jobs
~~~~~~~~~~~

Recent jobs card provides a list of recently submitted Ray jobs.

SANG-TODO Image

Event View
~~~~~~~~~~

.. note:: 

  The event view feature is experimental.

The event view lets you see a list of events associated with a specific type (e.g., autoscaler or job) in a chronological order. The equivalent information is also accessible via CLI commands ``ray list cluster-events`` :ref:`(Ray state APIs)<state-api-overview-ref>`.

There are 2 types of events that are available.

- **Job**: Events related to :ref:`Ray job submission APIs <jobs-quickstart>`.
- **Autoscaler**: Events related to :ref:`Ray autoscaler <cluster-autoscaler>`

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/event.png
    :align: center

.. _dash-jobs-view:

Jobs View
---------

The Jobs view lets you monitor the different jobs that ran on your Ray cluster.

A job is a ray workload that uses Ray APIs (e.g., ``ray.init``). It can be submitted directly (e.g., by executing a Python script within a head node) or via :ref:`Ray job API <jobs-quickstart>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/jobs.png
    :align: center

The job page displays a list of active, finished, and failed jobs, and clicking on an ID allows users to view detailed information about that job. 
For more information on Ray jobs, see the Ray Job Overview section.

SANG-TODO Image of the job id with a red circle.

Job Profiling
~~~~~~~~~~~~~

You can profile Ray jobs by clicking on the “Stack Trace” or “CPU Flame Graph” actions. See the :ref:`Dashboard Profiling <dashboard-profiling>` for more details.

SANG-TODO Image of the profilng buttons with a red circle.

.. _dash-workflow-job-progress:

Advanced Task/Actor Breakdown
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The job page allows you to see tasks and actors broken down by their states. 
Tasks and actors are grouped and nested by default. You can see the nested entries by clicking the expand button.

Tasks/actors are grouped and nested by the following criteria.

- All tasks and actors are grouped together, and you can view individual entries by expanding the corresponding row.
- Tasks are grouped by their ``name`` attribute (e.g., ``task.options(name="<name_here>").remote()``).
- Child tasks (nested tasks) are nested under their parent task's row.
- Actors are grouped by their class name.
- Child actors (actors created within an actor) are nested under their parent actor's row.
- Actor tasks (remote methods within an actor) are nested under the actor for the corresponding actor method.

TODO-SANG Add images

.. note:: 

  Ray dashboard can only display/retrieves up to 10K tasks at a time. If there are more than 10K tasks from your job,
  they are unaccounted. The number of unaccounted tasks are available from the task breakdown.

Task Timeline
~~~~~~~~~~~~~

The :ref:`timeline API <ray-core-timeline>` is available from the dashboard. 
You can download the chrome tracing file by clicking the download button. You can either go to ``chrome://tracing`` or the `Perfetto UI <https://ui.perfetto.dev/>`_ and drop the downloaded chrome tracing file to see the timeline visualization of Ray tasks and actors.

TODO-SANG Add images

Ray Status
~~~~~~~~~~

The job page displays the output of the helpful CLI tool ray status, which shows the autoscaler status of the Ray cluster.

The left page shows the autoscaling status, including pending, active, and failed nodes. 
The right page displays the cluster's demands, which lists demanded resources that cannot be scheduled to the cluster at the moment. This page is useful for debugging resource deadlocks or slow scheduling.

TODO-SANG Add images

.. note:: 

  The output shows the aggregated information across the cluster. If you run more than on job, the demands could be wrong.

.. _dash-workflow-state-apis:  

Task Table, Actor Table, Placement Group Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dashboard shows a table with the status of the job's tasks, actors, and placement groups. 
This information is the same as what you would get from the :ref:`Ray state APIs <state-api-overview-ref>`.

.. _dash-node-view:

Cluster View
------------

The cluster view visualizes hierarchical relationship of
machines (nodes) and workers (processes). Each host consists of many workers, and
you can see them by clicking the + button. This also shows the assignment of GPU resources to specific actors or tasks.

You can hide it again by clicking the - button.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/nodes-view-expand.png
    :align: center

You can also click the node id to go into a node detail page where you can see more information.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/node-detail.png
    :align: center

In addition, the machine view lets you see **logs** for a node or a worker.

SANG-TODO image

.. _dash-actors-view:

Actors View
-----------

The Actors view lets you see information about the actors that have existed on the ray cluster.

You can view the logs for an actor and you can see which job created the actor.
The information of up to 1000 dead actors will be stored.
This value can be overridden by using the `RAY_DASHBOARD_MAX_ACTORS_TO_CACHE` environment variable
when starting Ray.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/actors.png
    :align: center

Actor Profiling
~~~~~~~~~~~~~~~

You can also run the profiler on a running actor. See :ref:`Dashboard Profiling <dashboard-profiling>` for more details.

TODO-SANG Add an image.

Actor Detail Page
~~~~~~~~~~~~~~~~~

By clicking the ID, you can also see the detail view of the actor. 
From the actor detail page, you can see the metadata, state, and the all tasks that have run from this actor. 

TODO-SANG Add an image of clicking an actor id.
TODO-SANG Add an image.

.. _dash-metrics-view:

Metrics View
------------

.. note:: 

  The metrics view required the Prometheus and Grafana setup. **See :ref:`Ray Metrics <ray-metrics>` to learn how to set up Prometheus and Grafana**.

The metrics view lets you view visualizations of the time series metrics emitted by Ray.

You can select the time range of the metrics in the top right corner. The graphs refresh automatically every 15 seconds.

There is also a convenient button to open the grafana UI from the dashboard. The Grafana UI provides additional customizability of the charts.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/metrics.png
    :align: center

.. _dash-logs-view:

Logs view
---------
The logs view lets you view all the ray logs that are in your cluster. It is organized by node and log file name. Many log links in the other pages will link to this view and filter the list so the relevant logs appear.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/logs.png
    :align: center

The log viewer provides various search functionality to help find the log messages you are looking for.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/logs-content.png
    :align: center

Advanced Usage
--------------

Viewing built-in dashboard API metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The dashboard is powered by a server that serves both the UI code and the data about the cluster via API endpoints.
There are basic prometheus metrics that are emitted for each of these API endpoints:

`ray_dashboard_api_requests_count_requests_total`: Collects the total count of requests. This is tagged by endpoint, method, and http_status.

`ray_dashboard_api_requests_duration_seconds_bucket`: Collects the duration of requests. This is tagged by endpoint and method.

For example, you can view the p95 duration of all requests with this query:

.. code-block:: text

  histogram_quantile(0.95, sum(rate(ray_dashboard_api_requests_duration_seconds_bucket[5m])) by (le))

These metrics can be queried via prometheus or grafana UI. Instructions on how to set these tools up can be found :ref:`here <ray-metrics>`.


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

How to disable the dashboard
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Dashboard is included in the `ray[default]` installation by default and automatically started.

To disable the dashboard, use the following arguments `--include-dashboard`.

.. tabbed:: CLI

    .. code-block:: bash

        ray start --include-dashboard=False

.. tabbed:: Python SDK

    .. code-block:: python

        ray.init(include_dashboard=False)

.. _dash-reference:

Page References
---------------

Cluster View
~~~~~~~~~~~~

.. list-table:: Cluster View Node Table Reference
  :widths: 25 75
  :header-rows: 1

  * - Term
    - Description
  * - **State**
    - Whether the node or worker is alive or dead.
  * - **ID**
    - The ID of the node or the workerId for the worker.
  * - **Host / Cmd line**
    - If it is a node, it shows host information. If it is a worker, it shows the name of the task that is being run.
  * - **IP / PID**
    - If it is a node, it shows the IP address of the node. If it's a worker, it shows the PID of the worker process.
  * - **CPU Usage**
    - CPU usage of each node and worker.
  * - **Memory**
    - RAM usage of each node and worker.
  * - **GPU**
    - GPU usage of the node.
  * - **GRAM**
    - GPU memory usage of the node.
  * - **Object Store Memory**
    - Amount of memory used by the object store for this node.
  * - **Disk**
    - Disk usage of the node.
  * - **Sent**
    - Network bytes sent for each node and worker.
  * - **Received**
    - Network bytes received for each node and worker.
  * - **Log**
    - Logs messages at each node and worker. You can see log files relevant to a node or worker by clicking this link.
  * - **Stack Trace**
    - Get the Python stack trace for the specified worker. Refer to :ref:`dashboard-profiling` for more information.
  * - **CPU Flame Graph**
    - Get a CPU flame graph for the specified worker. Refer to :ref:`dashboard-profiling` for more information.


Jobs View
~~~~~~~~~

.. list-table:: Jobs View Reference
  :widths: 25 75
  :header-rows: 1

  * - Term
    - Description
  * - **Job ID**
    - The ID of the job. This is the primary id that associates tasks and actors to this job.
  * - **Submission ID**
    - An alternate ID that can be provided by a user or generated for all ray job submissions.
      It's useful if you would like to associate your job with an ID that is provided by some external system.
  * - **Status**
    - Describes the state of a job. One of:
        * PENDING: The job has not started yet, likely waiting for the runtime_env to be set up.
        * RUNNING: The job is currently running.
        * STOPPED: The job was intentionally stopped by the user.
        * SUCCEEDED: The job finished successfully.
        * FAILED: The job failed.
  * - **Logs**
    - A link to the logs for this job.
  * - **StartTime**
    - The time the job was started.
  * - **EndTime**
    - The time the job finished.
  * - **DriverPid**
    - The PID for the driver process that is started the job.

Actors
~~~~~~

.. list-table:: Actor View Reference
  :widths: 25 75
  :header-rows: 1

  * - Term
    - Description
  * - **Actor ID**
    - The ID of the actor.
  * - **Restart Times**
    - Number of times this actor has been restarted.
  * - **Name**
    - The name of an actor. This can be user defined.
  * - **Class**
    - The class of the actor.
  * - **Function**
    - The current function the actor is running.
  * - **Job ID**
    - The job in which this actor was created.
  * - **Pid**
    - ID of the worker process on which the actor is running.
  * - **IP**
    - Node IP Address where the actor is located.
  * - **Port**
    - The Port for the actor.
  * - **State**
    - Either one of "ALIVE" or "DEAD".
  * - **Log**
    - A link to the logs that are relevant to this actor.
  * - **Stack Trace**
    - Get the Python stack trace for the specified actor. Refer to :ref:`dashboard-profiling` for more information.
  * - **CPU Flame Graph**
    - Get a CPU flame graph for the specified actor. Refer to :ref:`dashboard-profiling` for more information.

Logs
~~~~

Details of the different log files can be found here: :ref:`ray-logging`.

Resources
---------
- `Ray Summit observability talk <https://www.youtube.com/watch?v=v_JzurOkdVQ>`_
- `Ray metrics blog <https://www.anyscale.com/blog/monitoring-and-debugging-ray-workloads-ray-metrics>`_
- `Ray dashboard roadmap <https://github.com/ray-project/ray/issues/30097#issuecomment-1445756658>`_
