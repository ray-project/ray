.. _ray-dashboard:

Ray Dashboard
=============
Ray provides a web-based dashboard for monitoring and debugging Ray applications.
The dashboard provides a visual representation of the system state, allowing users to track the performance 
of their applications and troubleshoot issues.

.. raw:: html

    <div style="position: relative; padding-bottom: 56.25%; height: 0; overflow: hidden; max-width: 100%; height: auto;">
        <iframe src="https://www.youtube.com/embed/VPksEcjACOM" frameborder="0" allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
    </div>

Common Workflows
----------------

Here are common workflows when using the Ray dashboard.

- :ref:`View the metrics graphs <dash-metrics-view>`.
- :ref:`View the progress of your job <dash-workflow-job-progress>`.
- :ref:`Find the application logs or error messages of failed tasks or actors <dash-workflow-logs>`.
- :ref:`Profile, trace dump, and visualize the timeline of the Ray jobs, tasks, or actors <dashboard-profiling>`.
- :ref:`Analyze the CPU and memory usage of the cluster, tasks and actors <dash-workflow-cpu-memory-analysis>`.
- :ref:`View the individual state of task, actor, placement group <dash-workflow-state-apis>`, and :ref:`nodes (machines from a cluster) <dash-node-view>` which is equivalent to :ref:`Ray state APIs <state-api-overview-ref>`.
- :ref:`View the hardware utilization (CPU, GPU, memory) <dash-workflow-resource-utilization>`.

Getting Started
---------------

To use the dashboard, you should use the `ray[default]` installation:

.. code-block:: bash

  pip install -U "ray[default]"

You can access the dashboard through a URL printed when Ray is initialized (the default URL is **http://localhost:8265**) or via the context object returned from `ray.init`.

.. code-block:: python

    context = ray.init()
    print(context.dashboard_url)

.. code-block:: text

  INFO worker.py:1487 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265.

Ray cluster comes with the dashboard. See :ref:`Cluster Monitoring <monitor-cluster-via-dashboard>` for more details.

.. note:: 

  When using the Ray dashboard, it is highly recommended to also set up Prometheus and Grafana. 
  They are necessary for critical features such as :ref:`Metrics View <dash-metrics-view>`.
  See :ref:`Ray Metrics <ray-metrics>` to learn how to set up Prometheus and Grafana.

How to Guides
-------------

.. _dash-workflow-logs:

View the application logs and errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Driver Logs**

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/log_button_at_job.png
    :align: center

If the Ray job is submitted by :ref:`Ray job API <jobs-quickstart>`, the job logs are available from the dashboard. The log file follows the following format; ``job-driver-<job_submission_id>.log``.

.. note:: 

  If the driver is executed directly on the head node of the Ray cluster (without the job API) or run via :ref:`Ray client <ray-client-ref>`, the driver logs are not accessible from the dashboard. In this case, see the terminal output to view the driver logs.

**Task and Actor Logs**

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/actor_log.png
    :align: center

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/task_log.png
    :align: center

Task and actor logs are accessible from the :ref:`task and actor table view <dash-workflow-state-apis>`. Click the log button.
You can see the worker logs (``worker-[worker_id]-[job_id]-[pid].[out|err]``) that execute the task and actor. ``.out`` (stdout) and ``.err`` (stderr) logs contain the logs emitted from the tasks and actors. 
The core worker logs (``python-core-worker-[worker_id]_[pid].log``) contain the system-level logs for the corresponding worker.

**Task and Actor Errors**

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/failed_task_progress-bar.png
    :align: center

You can easily identify failed tasks or actors by looking at the job progress bar, which links to the table. 

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/task_error_button.png
    :align: center

The table displays the name of the failed tasks or actors and provides access to their corresponding log or error messages.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/task_error_box.png
    :align: center

.. _dash-workflow-cpu-memory-analysis:

Analyze the CPU and memory usage of tasks and actors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`Metrics View <dash-metrics-view>` in the Ray dashboard provides a "per-component CPU/memory usage graph" that displays CPU and memory usage over time for each task and actor in the application (as well as system components). 
This allows users to identify tasks and actors that may be consuming more resources than expected and optimize the performance of the application. 

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/node_cpu_by_comp.png
    :align: center


Per component CPU graph. 0.379 cores mean that it uses 40% of a single CPU core. Ray process names start with ``ray::``. ``raylet``, ``agent``, ``dashboard``, or ``gcs`` are system components.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/node_memory_by_comp.png
    :align: center

Per component memory graph. Ray process names start with ``ray::``. ``raylet``, ``agent``, ``dashboard``, or ``gcs`` are system components.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/cluster_page.png
    :align: center

Additionally, users can see a snapshot of hardware utilization from the :ref:`cluster page <dash-node-view>`, which provides an overview of resource usage across the entire Ray cluster.

.. _dash-workflow-resource-utilization:

View the Resource Utilization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray requires users to specify the number of :ref:`resources <logical-resources>` their tasks and actors will use through arguments such as ``num_cpus``, ``num_gpus``, ``memory``, and ``resource``. 
These values are used for scheduling, but may not always match the actual resource utilization (physical resource utilization).

- You can see the logical and physical resource utilization over time from the :ref:`Metrics View <dash-metrics-view>`.
- The snapshot of physical resource utilization (CPU, GPU, memory, disk, network) is also available from the :ref:`Cluster View <dash-node-view>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/logical_resource.png
    :align: center

The :ref:`logical resources <logical-resources>` usage.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/physical_resource.png
    :align: center

The physical resources (hardware) usage. Ray provides CPU, GPU, Memory, GRAM, disk, and network usage for each machine in a cluster.

.. _dash-overview:

Overview
--------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/overview-page.png
    :align: center

The overview page provides a high-level status of the Ray cluster.

**Overview Metrics**

The Overview Metrics page provides the cluster-level hardware utilization and autoscaling status (number of pending, active, and failed nodes).

**Recent Jobs**

The Recent Jobs card provides a list of recently submitted Ray jobs.

.. _dash-event:

**Event View**

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/event-page.png
    :align: center

The Event View displays a list of events associated with a specific type (e.g., autoscaler or job) in chronological order. The same information is accessible with the ``ray list cluster-events`` :ref:`(Ray state APIs)<state-api-overview-ref>` CLI commands .

Two types of events are available.

- Job: Events related to :ref:`Ray job submission APIs <jobs-quickstart>`.
- Autoscaler: Events related to the :ref:`Ray autoscaler <cluster-autoscaler>`.

.. _dash-jobs-view:

Jobs View
---------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/jobs.png
    :align: center

The Jobs View lets you monitor the different jobs that ran on your Ray cluster.

A job is a ray workload that uses Ray APIs (e.g., ``ray.init``). It can be submitted directly (e.g., by executing a Python script within a head node) or via :ref:`Ray job API <jobs-quickstart>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/job_list.png
    :align: center

The job page displays a list of active, finished, and failed jobs, and clicking on an ID allows users to view detailed information about that job. 
For more information on Ray jobs, see the Ray Job Overview section.

Job Profiling
~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/profile-job.png
    :align: center

You can profile Ray jobs by clicking on the “Stack Trace” or “CPU Flame Graph” actions. See the :ref:`Dashboard Profiling <dashboard-profiling>` for more details.

.. _dash-workflow-job-progress:

Advanced Task and Actor Breakdown
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/advanced-progress.png
    :align: left

The job page allows you to see tasks and actors broken down by their states. 
Tasks and actors are grouped and nested by default. You can see the nested entries by clicking the expand button.

Tasks and actors are grouped and nested by the following criteria.

- All tasks and actors are grouped together, and you can view individual entries by expanding the corresponding row.
- Tasks are grouped by their ``name`` attribute (e.g., ``task.options(name="<name_here>").remote()``).
- Child tasks (nested tasks) are nested under their parent task's row.
- Actors are grouped by their class name.
- Child actors (actors created within an actor) are nested under their parent actor's row.
- Actor tasks (remote methods within an actor) are nested under the actor for the corresponding actor method.

.. note:: 

  Ray dashboard can only display or retrieve up to 10K tasks at a time. If there are more than 10K tasks from your job,
  they are unaccounted. The number of unaccounted tasks is available from the task breakdown.

Task Timeline
~~~~~~~~~~~~~

The :ref:`timeline API <ray-core-timeline>` is available from the dashboard. 

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/profile-button.png
    :align: center

First, you can download the chrome tracing file by clicking the download button.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/profile_drag.png
    :align: center

Second, you can use tools like ``chrome://tracing`` or the `Perfetto UI <https://ui.perfetto.dev/>`_ and drop the downloaded chrome tracing file. We will use the Perfetto as it is the recommendation way to visualize chrome tracing files.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/timeline.png
    :align: center

Now, you can see the timeline visualization of Ray tasks and actors. There are Node rows (hardware) and Worker rows (processes). 
Each worker rows display a list of events (e.g., task scheduled, task running, input/output deserialization, etc.) happening from that worker over time.

Ray Status
~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/ray-status.png
    :align: center

The job page displays the output of the CLI tool ``ray status``, which shows the autoscaler status of the Ray cluster.

The left page shows the autoscaling status, including pending, active, and failed nodes. 
The right page displays the cluster's demands, which are resources that cannot be scheduled to the cluster at the moment. This page is useful for debugging resource deadlocks or slow scheduling.

.. note:: 

  The output shows the aggregated information across the cluster (not by job). If you run more than one job, some of the demands may come from other jobs.

.. _dash-workflow-state-apis:  

Task Table, Actor Table, Placement Group Table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/tables.png
    :align: center

The dashboard shows a table with the status of the job's tasks, actors, and placement groups. 
You get the same information from the :ref:`Ray state APIs <state-api-overview-ref>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/task-table.png
    :align: center

You can expand the table to see a list of each task, actor, and placement group.

.. _dash-serve-view:

Serve View
----------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/serve.png
    :align: center

The Serve view lets you monitor the status of your :ref:`Ray Serve <rayserve>` applications.

The initial page showcases your general Serve configurations, a list of the Serve applications, and, if you have :ref:`Grafana and Prometheus <ray-metrics>` configured, some high-level
metrics of all your Serve applications. Click the name of a Serve application to go to the Serve Application Detail Page.

Serve Application Detail Page
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/serve-application.png
    :align: center

This page shows the Serve application's configurations and metadata. It also lists the :ref:`Serve deployments and replicas <serve-key-concepts-deployment>`.
Click the expand button of a deployment to see all the replicas in that deployment.

For each deployment, there are two available actions. You can view the Deployment config and, if you configured :ref:`Grafana and Prometheus <ray-metrics>`, you can open
a Grafana dashboard with detailed metrics about that deployment.

For each replica, there are two available actions. You can see the logs of that replica and, if you configured :ref:`Grafana and Prometheus <ray-metrics>`, you can open
a Grafana dashboard with detailed metrics about that replica. Click on the replica name to go to the Serve Replica Detail Page.


Serve Replica Detail Page
~~~~~~~~~~~~~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/serve-replica.png
    :align: center

This page shows metadata about the Serve replica, high-level metrics about the replica if you configured :ref:`Grafana and Prometheus <ray-metrics>`, and
a history of completed :ref:`tasks <core-key-concepts>` of that replica.


Serve Metrics
~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/serve-metrics.png
    :align: center

Ray serve exports various time-series metrics to understand the status of your Serve application over time. More details of these metrics can be found :ref:`here <serve-production-monitoring-metrics>`.
In order to store and visualize these metrics, you must set up Prometheus and Grafana by following the instructions :ref:`here <ray-metrics>`.

These metrics are available in the Ray dashboard in the Serve page and the Serve Replica Detail page. They are also accessible as Grafana dashboards.
Within the Grafana dashboard, use the dropdown filters on the top to filter metrics by route, deployment, or replica. Exact descriptions
of each graph are available by hovering over the "info" icon on the top left of each graph.

.. _dash-node-view:

Cluster View
------------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/nodes-view-expand.png
    :align: center

The cluster view visualizes hierarchical relationship of
machines (nodes) and workers (processes). Each host consists of many workers, and
you can see them by clicking the + button. This also shows the assignment of GPU resources to specific actors or tasks.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/node-detail.png
    :align: center

You can also click the node id to go into a node detail page where you can see more information.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/machine-view-log.png
    :align: center


In addition, the machine view lets you see **logs** for a node or a worker.

.. _dash-actors-view:

Actors View
-----------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/actor-page.png
    :align: center

The Actors view lets you see information about the actors that have existed on the ray cluster.

You can view the logs for an actor and you can see which job created the actor.
The information of up to 1000 dead actors will be stored.
This value can be overridden by using the `RAY_DASHBOARD_MAX_ACTORS_TO_CACHE` environment variable
when starting Ray.

Actor Profiling
~~~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/actor-profiling.png
    :align: center

You can also run the profiler on a running actor. See :ref:`Dashboard Profiling <dashboard-profiling>` for more details.

Actor Detail Page
~~~~~~~~~~~~~~~~~

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/actor-list-id.png
    :align: center

By clicking the ID, you can also see the detail view of the actor. 

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard-v2/dashboard-pics/actor-detail.png
    :align: center

From the actor detail page, you can see the metadata, state, and the all tasks that have run from this actor. 

.. _dash-metrics-view:

Metrics View
------------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/metrics.png
    :align: center

Ray exports default metrics which are available from the :ref:`Metrics View <dash-metrics-view>`. Here are some available example metrics.

- The tasks, actors, and placement groups broken down by states.
- The :ref:`logical resource usage <logical-resources>` across nodes.
- The hardware resource usage across nodes.
- The autoscaler status.

See :ref:`System Metrics Page <system-metrics>` for available metrics.

.. note:: 

  The metrics view required the Prometheus and Grafana setup. See :ref:`Ray Metrics <ray-metrics>` to learn how to set up Prometheus and Grafana.

The metrics view lets you view visualizations of the time series metrics emitted by Ray.

You can select the time range of the metrics in the top right corner. The graphs refresh automatically every 15 seconds.

There is also a convenient button to open the grafana UI from the dashboard. The Grafana UI provides additional customizability of the charts.

.. _dash-logs-view:

Logs View
---------

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/logs.png
    :align: center

The logs view lets you view all the ray logs that are in your cluster. It is organized by node and log file name. Many log links in the other pages link to this view and filter the list so the relevant logs appear.

To understand the log file structure of Ray, see the :ref:`Logging directory structure page <logging-directory-structure>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/logs-content.png
    :align: center

The logs view provides search functionality to help you find specific log messages.

Advanced Usage
--------------

Changing Dashboard Ports
~~~~~~~~~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: Single-node local cluster

      **CLI**

      To customize the port on which the dashboard runs, you can pass
      the ``--dashboard-port`` argument with ``ray start`` in the command line.

      **ray.init**

      If you need to customize the port on which the dashboard will run, you can pass the
      keyword argument ``dashboard_port`` in your call to ``ray.init()``.

    .. tab-item:: VM Cluster Launcher

      To disable the dashboard while using the "VM cluster launcher", include the "ray start --head --include-dashboard=False" argument
      and specify the desired port number in the "head_start_ray_commands" section of the `cluster launcher's YAML file <https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172>`_.

    .. tab-item:: Kuberay

      See the `Specifying non-default ports <https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html#specifying-non-default-ports>`_ page.

Viewing Built-in Dashboard API Metrics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The dashboard is powered by a server that serves both the UI code and the data about the cluster via API endpoints.
There are basic Prometheus metrics that are emitted for each of these API endpoints:

`ray_dashboard_api_requests_count_requests_total`: Collects the total count of requests. This is tagged by endpoint, method, and http_status.

`ray_dashboard_api_requests_duration_seconds_bucket`: Collects the duration of requests. This is tagged by endpoint and method.

For example, you can view the p95 duration of all requests with this query:

.. code-block:: text

  histogram_quantile(0.95, sum(rate(ray_dashboard_api_requests_duration_seconds_bucket[5m])) by (le))

These metrics can be queried via Prometheus or Grafana UI. Instructions on how to set these tools up can be found :ref:`here <ray-metrics>`.


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

Disabling the Dashboard
~~~~~~~~~~~~~~~~~~~~~~~
Dashboard is included in the `ray[default]` installation by default and automatically started.

To disable the dashboard, use the following arguments `--include-dashboard`.

.. tab-set::

    .. tab-item:: Single-node local cluster

      **CLI**

      .. code-block:: bash

          ray start --include-dashboard=False

      **ray.init**

      .. code-block:: python

          ray.init(include_dashboard=False)

    .. tab-item:: VM Cluster Launcher

      To disable the dashboard while using the "VM cluster launcher", include the "ray start --head --include-dashboard=False" argument
      in the "head_start_ray_commands" section of the `cluster launcher's YAML file <https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172>`_.

    .. tab-item:: Kuberay

      TODO

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

Resources
---------
- `Ray Summit observability talk <https://www.youtube.com/watch?v=v_JzurOkdVQ>`_
- `Ray metrics blog <https://www.anyscale.com/blog/monitoring-and-debugging-ray-workloads-ray-metrics>`_
- `Ray dashboard roadmap <https://github.com/ray-project/ray/issues/30097#issuecomment-1445756658>`_
- `Observability Training Module <https://github.com/ray-project/ray-educational-materials/blob/main/Observability/Ray_observability_part_1.ipynb>`_